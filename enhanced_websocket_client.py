"""
Enhanced WebSocket Client for Real-time Order Book Data
Uses L4Book subscription format from the Rust WebSocket server.
Implements concurrent processing and keep-alive functionality.
"""
import asyncio
import json
import logging
import time
import websockets
import websockets.exceptions
from typing import Dict, Any, Optional, Callable, Set
from datetime import datetime, timezone
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class OrderBookSnapshot:
    """Order book snapshot data."""
    coin: str
    bids: list  # [(price, size), ...]
    asks: list  # [(price, size), ...]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    levels: int = 0

@dataclass
class ConnectionStats:
    """WebSocket connection statistics."""
    connected_at: Optional[datetime] = None
    messages_received: int = 0
    snapshots_received: int = 0
    updates_received: int = 0
    last_message_at: Optional[datetime] = None
    ping_count: int = 0
    reconnect_count: int = 0

class EnhancedWebSocketClient:
    """
    Enhanced WebSocket client for L4Book real-time data.
    Uses the proper subscription format for the Rust WebSocket server.
    """
    
    def __init__(self, ws_url: str, coin: str, message_handler: Optional[Callable] = None):
        # Ensure proper WebSocket URL format
        if not ws_url.endswith('/ws'):
            ws_url = ws_url.rstrip('/') + '/ws'
        
        self.ws_url = ws_url
        self.coin = coin.upper()
        self.message_handler = message_handler
        
        # Connection management
        self.connection = None
        self.running = False
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 30.0
        self._ping_interval = 20.0  # Ping every 20 seconds
        self._ping_timeout = 10.0
        
        # Data storage
        self.latest_snapshot: Optional[OrderBookSnapshot] = None
        self.stats = ConnectionStats()
        
        # Tasks
        self._ping_task = None
        self._listener_task = None
        
    async def connect(self) -> bool:
        """Establish WebSocket connection with proper L4Book subscription."""
        try:
            logger.info(f"🔌 Connecting to {self.ws_url} for {self.coin}")
            
            # Connect with compression and proper settings
            self.connection = await websockets.connect(
                self.ws_url,
                ping_interval=None,  # We'll handle pings manually
                ping_timeout=self._ping_timeout,
                close_timeout=10,
                max_size=50 * 1024 * 1024,  # 50MB max message size
                compression="deflate"  # Enable compression
            )
            
            # Send L4Book subscription using the exact format from the Rust server
            subscription_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "l4Book",
                    "coin": self.coin
                }
            }
            
            await self.connection.send(json.dumps(subscription_msg))
            logger.info(f"✅ Connected and subscribed to L4Book for {self.coin}")
            
            # Update stats
            self.stats.connected_at = datetime.now(timezone.utc)
            self.stats.reconnect_count += 1
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Connection failed for {self.coin}: {e}")
            return False
    
    async def start(self):
        """Start the WebSocket client with concurrent ping and message handling."""
        self.running = True
        
        while self.running:
            try:
                # Connect
                if not await self.connect():
                    await asyncio.sleep(self._reconnect_delay)
                    self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)
                    continue
                
                # Reset reconnect delay on successful connection
                self._reconnect_delay = 1.0
                
                # Start concurrent tasks
                self._ping_task = asyncio.create_task(self._ping_loop())
                self._listener_task = asyncio.create_task(self._message_loop())
                
                # Wait for either task to complete (usually due to error)
                done, pending = await asyncio.wait(
                    [self._ping_task, self._listener_task],
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Cancel remaining tasks
                for task in pending:
                    task.cancel()
                
                # Check for exceptions
                for task in done:
                    try:
                        await task
                    except Exception as e:
                        logger.error(f"❌ {self.coin} task error: {e}")
                
            except Exception as e:
                logger.error(f"❌ {self.coin} main loop error: {e}")
            
            finally:
                await self._cleanup_connection()
                
            if self.running:
                logger.info(f"🔄 {self.coin}: Reconnecting in {self._reconnect_delay}s...")
                await asyncio.sleep(self._reconnect_delay)
    
    async def _ping_loop(self):
        """Send periodic pings to keep connection alive."""
        while self.running and self.connection:
            try:
                await asyncio.sleep(self._ping_interval)
                
                if self.connection and not self.connection.closed:
                    # Send ping
                    await self.connection.ping()
                    self.stats.ping_count += 1
                    logger.debug(f"📡 {self.coin}: Sent ping #{self.stats.ping_count}")
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"🔌 {self.coin}: Connection closed during ping")
                break
            except Exception as e:
                logger.error(f"❌ {self.coin}: Ping error: {e}")
                break
    
    async def _message_loop(self):
        """Listen for messages and process them."""
        while self.running and self.connection:
            try:
                # Receive message with timeout
                message = await asyncio.wait_for(
                    self.connection.recv(),
                    timeout=60.0  # 60 second timeout
                )
                
                # Update stats
                self.stats.messages_received += 1
                self.stats.last_message_at = datetime.now(timezone.utc)
                
                # Process message
                await self._process_message(message)
                
            except asyncio.TimeoutError:
                logger.warning(f"⏰ {self.coin}: Message timeout")
                break
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"🔌 {self.coin}: Connection closed")
                break
            except Exception as e:
                logger.error(f"❌ {self.coin}: Message processing error: {e}")
                break
    
    async def _process_message(self, message: str):
        """Process incoming WebSocket message."""
        try:
            data = json.loads(message)
            
            # Check message type
            if "channel" in data and data["channel"] == "l4Book":
                if "data" in data:
                    book_data = data["data"]
                    
                    # Handle initial snapshot
                    if "coin" in book_data and "levels" in book_data:
                        await self._handle_snapshot(book_data)
                        self.stats.snapshots_received += 1
                    else:
                        # Handle book updates/diffs
                        await self._handle_book_update(book_data)
                        self.stats.updates_received += 1
            
            # Call custom message handler if provided
            if self.message_handler:
                await self.message_handler(self.coin, data)
                
        except json.JSONDecodeError as e:
            logger.error(f"❌ {self.coin}: JSON decode error: {e}")
        except Exception as e:
            logger.error(f"❌ {self.coin}: Message processing error: {e}")
    
    async def _handle_snapshot(self, book_data: Dict[str, Any]):
        """Handle order book snapshot."""
        try:
            coin = book_data.get("coin", self.coin)
            levels = book_data.get("levels", [])
            
            # Parse levels into bids and asks
            bids = []
            asks = []
            
            for level in levels:
                px = float(level.get("px", 0))
                sz = float(level.get("sz", 0))
                side = level.get("side")  # "A" for ask, "B" for bid
                
                if side == "B":  # Bid
                    bids.append((px, sz))
                elif side == "A":  # Ask
                    asks.append((px, sz))
            
            # Sort bids descending (highest first), asks ascending (lowest first)
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            
            # Create snapshot
            self.latest_snapshot = OrderBookSnapshot(
                coin=coin,
                bids=bids,
                asks=asks,
                levels=len(levels)
            )
            
            logger.info(f"📊 {self.coin}: Snapshot - {len(bids)} bids, {len(asks)} asks")
            
        except Exception as e:
            logger.error(f"❌ {self.coin}: Snapshot processing error: {e}")
    
    async def _handle_book_update(self, update_data: Dict[str, Any]):
        """Handle order book update/diff."""
        # For now, we'll just log the update
        # In a full implementation, you'd apply the diff to the existing snapshot
        logger.debug(f"📈 {self.coin}: Book update received")
    
    async def _cleanup_connection(self):
        """Clean up WebSocket connection and tasks."""
        if self._ping_task:
            self._ping_task.cancel()
        if self._listener_task:
            self._listener_task.cancel()
            
        if self.connection:
            try:
                await self.connection.close()
            except Exception:
                pass
            self.connection = None
    
    async def stop(self):
        """Stop the WebSocket client."""
        logger.info(f"🛑 Stopping {self.coin} WebSocket client")
        self.running = False
        await self._cleanup_connection()
    
    def get_latest_orderbook(self) -> Optional[OrderBookSnapshot]:
        """Get the latest order book snapshot."""
        return self.latest_snapshot
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        return {
            "coin": self.coin,
            "connected": self.connection is not None and not self.connection.closed,
            "connected_at": self.stats.connected_at.isoformat() if self.stats.connected_at else None,
            "messages_received": self.stats.messages_received,
            "snapshots_received": self.stats.snapshots_received,
            "updates_received": self.stats.updates_received,
            "last_message_at": self.stats.last_message_at.isoformat() if self.stats.last_message_at else None,
            "ping_count": self.stats.ping_count,
            "reconnect_count": self.stats.reconnect_count,
        }

class ConcurrentWebSocketManager:
    """
    Manages multiple WebSocket connections concurrently.
    One connection per market for optimal performance.
    """
    
    def __init__(self, ws_url: str, markets: list, message_handler: Optional[Callable] = None):
        self.ws_url = ws_url
        self.markets = markets
        self.message_handler = message_handler
        
        self.clients: Dict[str, EnhancedWebSocketClient] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
        self.running = False
    
    async def start(self):
        """Start concurrent WebSocket connections for all markets."""
        self.running = True
        logger.info(f"🚀 Starting concurrent WebSocket connections for {len(self.markets)} markets")
        
        # Create clients and tasks for each market
        for market in self.markets:
            client = EnhancedWebSocketClient(
                self.ws_url, 
                market, 
                self.message_handler
            )
            self.clients[market] = client
            
            # Start client task
            task = asyncio.create_task(client.start())
            self.tasks[market] = task
            
            logger.info(f"✅ Started WebSocket client for {market}")
            
            # Small delay to stagger connections
            await asyncio.sleep(0.1)
        
        logger.info(f"🎯 All {len(self.markets)} WebSocket connections started")
    
    async def add_market(self, market: str):
        """Add a new market dynamically."""
        if market in self.clients:
            logger.warning(f"⚠️ WebSocket client for {market} already exists")
            return
        
        logger.info(f"➕ Adding WebSocket client for {market}")
        
        client = EnhancedWebSocketClient(
            self.ws_url,
            market,
            self.message_handler
        )
        self.clients[market] = client
        
        task = asyncio.create_task(client.start())
        self.tasks[market] = task
        
        logger.info(f"✅ Added WebSocket client for {market}")
    
    async def remove_market(self, market: str):
        """Remove a market dynamically."""
        if market not in self.clients:
            return
        
        logger.info(f"➖ Removing WebSocket client for {market}")
        
        # Stop client
        await self.clients[market].stop()
        
        # Cancel task
        if market in self.tasks:
            self.tasks[market].cancel()
            del self.tasks[market]
        
        del self.clients[market]
        logger.info(f"🗑️ Removed WebSocket client for {market}")
    
    async def stop(self):
        """Stop all WebSocket connections."""
        logger.info("🛑 Stopping all WebSocket connections...")
        self.running = False
        
        # Stop all clients
        for client in self.clients.values():
            await client.stop()
        
        # Cancel all tasks
        for task in self.tasks.values():
            task.cancel()
        
        self.clients.clear()
        self.tasks.clear()
        logger.info("✅ All WebSocket connections stopped")
    
    def get_orderbook(self, market: str) -> Optional[OrderBookSnapshot]:
        """Get latest order book for a specific market."""
        if market in self.clients:
            return self.clients[market].get_latest_orderbook()
        return None
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all connections."""
        return {
            market: client.get_stats() 
            for market, client in self.clients.items()
        }
