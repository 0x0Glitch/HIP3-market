"""Optimized persistent WebSocket client for L4 orderbook data."""

import json
import logging
import asyncio
import websockets
import websockets.protocol
import websockets.exceptions
import time
from typing import Dict, Optional, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class PersistentWebSocketClient:
    """WebSocket client with persistent connection and automatic reconnection."""
    
    def __init__(self, ws_url: str, coin: str, n_levels: int = 100):
        # Fix the WebSocket URL - the Rust server expects /ws endpoint
        if not ws_url.endswith('/ws'):
            ws_url = ws_url.rstrip('/') + '/ws'
        self.ws_url = ws_url
        self.coin = coin
        self.n_levels = n_levels
        self.connection = None
        self.running = False
        self._lock = asyncio.Lock()
        self._last_snapshot = None
        self._last_snapshot_time = None
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._reconnect_delay = 1.0  # Start with 1 second, exponential backoff
        
    async def connect(self) -> bool:
        """Establish WebSocket connection."""
        try:
            async with self._lock:
                if self.connection and not self.connection.closed:
                    return True
                    
                logger.info(f"Connecting to WebSocket {self.ws_url} for {self.coin}")
                self.connection = await websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    max_size=10 * 1024 * 1024  # 10MB max message size
                )
                
                # Send subscription - based on Rust server ClientMessage format
                subscription = {
                    "method": "subscribe",
                    "subscription": {
                        "type": "l4Book",
                        "coin": self.coin
                    }
                }
                
                await self.connection.send(json.dumps(subscription))
                self.running = True
                self._reconnect_attempts = 0
                logger.info(f"Successfully connected and subscribed to {self.coin} L4 orderbook")
                
                # Start listening task
                asyncio.create_task(self._listen_loop())
                
                return True
                
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")
            return False
    
    async def _listen_loop(self):
        """Continuously listen for messages from WebSocket."""
        try:
            while self.running and self.connection:
                try:
                    # Wait for message with timeout
                    message = await asyncio.wait_for(self.connection.recv(), timeout=30.0)
                    
                    # Parse message
                    data = json.loads(message)
                    
                    # Debug: Log all messages to understand server behavior
                    logger.debug(f"Received message: {json.dumps(data, indent=2)[:200]}...")
                    
                    # Check for different message types from Rust server
                    if data.get("channel") == "subscriptionResponse":
                        logger.info(f"✅ Subscription confirmed for {self.coin}")
                    elif data.get("channel") == "l4Book" and data.get("data"):
                        book_data = data["data"]
                        
                        # L4Book from Rust server has two formats based on enum:
                        # 1. Snapshot { coin, time, height, levels }
                        # 2. Updates { time, height, orderStatuses, bookDiffs }
                        
                        if "Snapshot" in book_data:
                            # Initial snapshot format
                            snapshot = book_data["Snapshot"]
                            self._last_snapshot = snapshot
                            self._last_snapshot_time = time.time()
                            
                            if "levels" in snapshot:
                                levels = snapshot["levels"]
                                if isinstance(levels, list) and len(levels) >= 2:
                                    bid_count = len(levels[0]) if levels[0] else 0
                                    ask_count = len(levels[1]) if levels[1] else 0
                                    logger.info(f"📈 L4 Snapshot received: {bid_count} bids, {ask_count} asks")
                                else:
                                    logger.warning(f"⚠️  Invalid levels format in snapshot")
                                    
                        elif "Updates" in book_data:
                            # Updates format with bookDiffs
                            updates = book_data["Updates"]
                            if "bookDiffs" in updates or "orderStatuses" in updates:
                                diff_count = len(updates.get("bookDiffs", []))
                                status_count = len(updates.get("orderStatuses", []))
                                logger.debug(f"📝 L4 Updates: {diff_count} diffs, {status_count} statuses")
                                # Don't mark as stale on every update - let age-based checking handle it
                        else:
                            logger.warning(f"⚠️  Unknown L4Book format: {list(book_data.keys())}")
                    elif data.get("channel") == "error":
                        logger.error(f"❌ Server error: {data.get('data', 'Unknown error')}")
                    else:
                        logger.info(f"📨 Unknown message type: channel={data.get('channel')}, keys={list(data.keys())}")
                        
                except asyncio.TimeoutError:
                    # No message received in timeout period, connection might be stale
                    logger.warning(f"No message received in 30s for {self.coin}")
                    await self._handle_disconnection()
                    break
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse WebSocket message: {e}")
                    
                except websockets.exceptions.ConnectionClosed:
                    logger.warning(f"WebSocket connection closed for {self.coin}")
                    await self._handle_disconnection()
                    break
                except websockets.exceptions.ConnectionClosedError:
                    logger.warning(f"WebSocket connection closed with error for {self.coin}")
                    await self._handle_disconnection()
                    break
                    
        except Exception as e:
            logger.error(f"Error in listen loop for {self.coin}: {e}")
            await self._handle_disconnection()
    
    async def _handle_disconnection(self):
        """Handle WebSocket disconnection and attempt reconnection."""
        self.connection = None
        
        if not self.running:
            return
            
        if self._reconnect_attempts >= self._max_reconnect_attempts:
            logger.error(f"Max reconnection attempts reached for {self.coin}")
            return
            
        self._reconnect_attempts += 1
        delay = self._reconnect_delay * (2 ** (self._reconnect_attempts - 1))
        
        logger.info(f"Attempting reconnection {self._reconnect_attempts}/{self._max_reconnect_attempts} "
                   f"for {self.coin} in {delay}s")
        await asyncio.sleep(delay)
        
        if await self.connect():
            logger.info(f"Successfully reconnected for {self.coin}")
        else:
            await self._handle_disconnection()
    
    async def ensure_connected(self) -> bool:
        """Ensure WebSocket is connected, reconnect if necessary."""
        if self.connection:
            # Try to check if connection is alive by checking its state
            try:
                # WebSocket connections have a state property
                if hasattr(self.connection, 'state') and self.connection.state.value == 1:  # OPEN state
                    return True
            except:
                pass
            
        return await self.connect()
    
    def get_snapshot(self, max_age_seconds: Optional[float] = None) -> Optional[Dict]:
        """
        Get the latest L4 orderbook snapshot.
        
        Args:
            max_age_seconds: Maximum age of snapshot in seconds. If None, return any cached snapshot.
        
        Returns:
            L4 orderbook snapshot or None if not available or too old.
        """
        if not self._last_snapshot:
            logger.debug(f"No snapshot available for {self.coin}")
            return None
            
        # Check freshness if max_age specified
        if max_age_seconds and self._last_snapshot_time:
            age = time.time() - self._last_snapshot_time
            if age > max_age_seconds:
                logger.warning(f"Snapshot for {self.coin} is {age:.1f}s old (max: {max_age_seconds}s)")
                return None
                
        return self._last_snapshot
    
    async def get_fresh_snapshot(self, timeout: float = 5.0) -> Optional[Dict]:
        """
        Get a fresh L4 orderbook snapshot by resubscribing.
        
        The Rust server only sends ONE snapshot on subscription,
        then only sends Updates. To get a fresh snapshot, we must resubscribe.
        
        Args:
            timeout: Maximum time to wait for a fresh snapshot
            
        Returns:
            Fresh L4 orderbook snapshot or None if timeout
        """
        if not self.connection:
            if not await self.connect():
                return None
        
        # Resubscribe to get a fresh snapshot
        try:
            logger.info(f"Resubscribing to get fresh L4 snapshot for {self.coin}")
            
            # Send unsubscribe first
            unsubscribe = {
                "method": "unsubscribe",
                "subscription": {
                    "type": "l4Book",
                    "coin": self.coin
                }
            }
            await self.connection.send(json.dumps(unsubscribe))
            await asyncio.sleep(0.1)  # Brief pause
            
            # Send new subscription
            subscription = {
                "method": "subscribe",
                "subscription": {
                    "type": "l4Book",
                    "coin": self.coin
                }
            }
            await self.connection.send(json.dumps(subscription))
            
            # Wait for fresh snapshot
            old_time = self._last_snapshot_time
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                if self._last_snapshot_time and (not old_time or self._last_snapshot_time > old_time):
                    logger.info(f"✅ Received fresh snapshot (age: {time.time() - self._last_snapshot_time:.2f}s)")
                    return self._last_snapshot
                await asyncio.sleep(0.05)
            
            logger.warning(f"Timeout waiting for fresh snapshot after {timeout}s")
            
        except Exception as e:
            logger.error(f"Error getting fresh snapshot: {e}")
            
        return self._last_snapshot
    
    async def close(self):
        """Close the WebSocket connection."""
        self.running = False
        
        async with self._lock:
            if self.connection:
                await self.connection.close()
                self.connection = None
                logger.info(f"Closed WebSocket connection for {self.coin}")
    
    async def disconnect(self):
        """Alias for close() for compatibility."""
        await self.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        connected = False
        if self.connection:
            try:
                # Check if connection has state attribute and is open
                if hasattr(self.connection, 'state'):
                    connected = self.connection.state == websockets.protocol.State.OPEN
                else:
                    connected = True  # Assume connected if we have a connection object
            except:
                connected = False
                
        return {
            "coin": self.coin,
            "connected": connected,
            "last_snapshot_age": time.time() - self._last_snapshot_time if self._last_snapshot_time else None,
            "reconnect_attempts": self._reconnect_attempts,
            "has_snapshot": self._last_snapshot is not None
        }


class LiquidityDepthCalculator:
    """Calculate liquidity depth from L4 orderbook data."""
    
    @staticmethod
    def parse_l4_snapshot(snapshot: Dict, target_coin: str = 'LINK') -> tuple:
        """
        Parse L4 snapshot and build bid/ask levels.
        Rust server L4Book::Snapshot format: {coin, time, height, levels}
        Returns: (bids, asks, best_bid, best_ask, mid_price)
        """
        logger.debug(f"Parsing L4 snapshot for {target_coin}")
        bids = []
        asks = []
        
        # Direct snapshot format from WebSocket (already unwrapped)
        if "coin" in snapshot and "levels" in snapshot:
            if snapshot["coin"].upper() == target_coin.upper():
                levels = snapshot["levels"]
                if isinstance(levels, list) and len(levels) >= 2:
                    bid_orders, ask_orders = levels[0], levels[1]
                else:
                    logger.warning(f"Invalid levels format in L4 snapshot")
                    return [], [], None, None, None
            else:
                logger.warning(f"Coin mismatch: expected {target_coin}, got {snapshot.get('coin')}")
                return [], [], None, None, None
        else:
            logger.warning(f"Invalid snapshot format, missing required fields. Keys: {list(snapshot.keys())}")
            return [], [], None, None, None
        
        # Process orders - EXACT fields from Rust L4Order struct
        for order in bid_orders:
            try:
                # L4Order fields: limitPx (String), sz (String), isTrigger (bool)
                # Skip trigger orders as mentioned in README
                if not order.get("isTrigger", False):
                    price = float(order["limitPx"])
                    size = float(order["sz"])
                    bids.append({
                        "price": price,
                        "size": size
                    })
            except (KeyError, ValueError, TypeError) as e:
                logger.debug(f"Skipping invalid bid order: {e}")
                    
        for order in ask_orders:
            try:
                # L4Order fields: limitPx (String), sz (String), isTrigger (bool)
                # Skip trigger orders as mentioned in README
                if not order.get("isTrigger", False):
                    price = float(order["limitPx"])
                    size = float(order["sz"])
                    asks.append({
                        "price": price,
                        "size": size
                    })
            except (KeyError, ValueError, TypeError) as e:
                logger.debug(f"Skipping invalid ask order: {e}")
        
        # Sort orders
        bids = sorted(bids, key=lambda x: x["price"], reverse=True)
        asks = sorted(asks, key=lambda x: x["price"])
        
        # Calculate best prices
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else None
        
        logger.debug(f"Parsed: {len(bids)} bids, {len(asks)} asks, mid=${mid_price:.2f}" if mid_price else "Empty book")
        
        return bids, asks, best_bid, best_ask, mid_price
    
    @staticmethod
    def calculate_depth(bids: list, asks: list, mid_price: float,
                       percentages: list = None) -> Dict[str, Dict]:
        """Calculate liquidity depth at various percentage levels."""
        if not mid_price or not bids or not asks:
            return {}
            
        if percentages is None:
            percentages = [0.05, 0.1, 0.25]
            
        depth = {}
        
        for pct in percentages:
            bid_threshold = mid_price * (1 - pct)
            ask_threshold = mid_price * (1 + pct)
            
            bid_depth = sum(
                order["size"] * order["price"]
                for order in bids
                if order["price"] >= bid_threshold
            )
            
            ask_depth = sum(
                order["size"] * order["price"]
                for order in asks
                if order["price"] <= ask_threshold
            )
            
            depth[f"{int(pct*100)}%"] = {
                "bid_depth": bid_depth,
                "ask_depth": ask_depth,
                "total_depth": bid_depth + ask_depth
            }
            
        return depth
    
    def analyze_orderbook(self, snapshot: Dict, target_coin: str = 'LINK') -> Dict[str, Any]:
        """Analyze L4 orderbook snapshot and return metrics."""
        if not snapshot:
            return {}
            
        # Parse L4 snapshot to get bid/ask orders
        bids, asks, best_bid, best_ask, mid_price = self.parse_l4_snapshot(snapshot, target_coin)
        
        if not bids and not asks:
            return {}
            
        # Calculate spread
        spread = best_ask - best_bid if best_bid and best_ask else None
        spread_pct = (spread / mid_price * 100) if spread and mid_price else None
        
        # Calculate liquidity depth at different percentage levels
        depth_metrics = self.calculate_depth(bids, asks, mid_price)
        
        # Build metrics dict - only include values that actually exist (no default zeros)
        result = {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": mid_price,
            "spread": spread,
            "spread_pct": spread_pct,
            "total_bids": len(bids),
            "total_asks": len(asks),
            "orderbook_levels": len(bids) + len(asks)
        }
        
        # Only add depth metrics if they were successfully calculated
        if depth_metrics:
            for pct_key, pct_data in depth_metrics.items():
                if pct_key == "5%":
                    if "total_depth" in pct_data:
                        result["depth_5_pct"] = pct_data["total_depth"]
                    if "bid_depth" in pct_data:
                        result["bid_depth_5pct"] = pct_data["bid_depth"]
                    if "ask_depth" in pct_data:
                        result["ask_depth_5pct"] = pct_data["ask_depth"]
                elif pct_key == "10%":
                    if "total_depth" in pct_data:
                        result["depth_10_pct"] = pct_data["total_depth"]
                    if "bid_depth" in pct_data:
                        result["bid_depth_10pct"] = pct_data["bid_depth"]
                    if "ask_depth" in pct_data:
                        result["ask_depth_10pct"] = pct_data["ask_depth"]
                elif pct_key == "25%":
                    if "total_depth" in pct_data:
                        result["depth_25_pct"] = pct_data["total_depth"]
                    if "bid_depth" in pct_data:
                        result["bid_depth_25pct"] = pct_data["bid_depth"]
                    if "ask_depth" in pct_data:
                        result["ask_depth_25pct"] = pct_data["ask_depth"]
        
        return result

