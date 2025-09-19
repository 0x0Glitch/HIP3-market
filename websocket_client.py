"""High-performance async WebSocket client for orderbook data."""

import asyncio
import json
import logging
import time
from typing import Dict, Optional, List, Tuple, Any, Set
from datetime import datetime, timezone
import websockets
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed, WebSocketException

logger = logging.getLogger(__name__)


class OrderBookWebSocketClient:
    """Persistent WebSocket client for L4 orderbook data with automatic reconnection."""
    
    def __init__(self, ws_url: str, reconnect_delay: float = 1.0, max_reconnect_delay: float = 30.0):
        self.ws_url = ws_url
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.current_reconnect_delay = reconnect_delay
        
        self.websocket: Optional[WebSocketClientProtocol] = None
        self.subscriptions: Set[str] = set()
        self.latest_snapshots: Dict[str, Dict] = {}
        self.running = False
        self._connect_lock = asyncio.Lock()
        self._receive_task: Optional[asyncio.Task] = None
        self._started = False
        
        # Performance metrics
        self.last_message_time: Dict[str, float] = {}
        self.message_count: Dict[str, int] = {}
    
    async def start(self) -> bool:
        """Start the WebSocket client and establish connection."""
        if self._started:
            return True
        
        self._started = True
        self.running = True
        return await self.connect()
        
    async def connect(self) -> bool:
        """Establish WebSocket connection."""
        async with self._connect_lock:
            if self.websocket and not self.websocket.closed:
                return True
                
            try:
                logger.info(f"Connecting to WebSocket: {self.ws_url}")
                self.websocket = await websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    max_size=50 * 1024 * 1024,  # 50MB max message size
                    compression=None  # Disable compression for lower latency
                )
                self.running = True
                self.current_reconnect_delay = self.reconnect_delay
                
                # Start receive loop
                if self._receive_task:
                    self._receive_task.cancel()
                self._receive_task = asyncio.create_task(self._receive_loop())
                
                # Re-subscribe to all markets
                if self.subscriptions:
                    await self._resubscribe()
                
                logger.info("WebSocket connection established")
                return True
                
            except Exception as e:
                logger.error(f"Failed to connect to WebSocket: {e}")
                self.websocket = None
                return False
    
    async def _receive_loop(self):
        """Continuously receive messages from WebSocket."""
        while self.running and self.websocket:
            try:
                message = await self.websocket.recv()
                await self._handle_message(message)
                
            except ConnectionClosed:
                logger.warning("WebSocket connection closed")
                self.websocket = None
                if self.running:
                    await self._reconnect()
                break
                
            except Exception as e:
                logger.error(f"Error in receive loop: {e}")
                if self.running:
                    await asyncio.sleep(0.1)
    
    async def _handle_message(self, message: str):
        """Process received WebSocket message."""
        try:
            data = json.loads(message)
            
            # Check if this is L4 book data
            if data.get("channel") == "l4Book" and data.get("data"):
                # Extract coin from the snapshot
                coin = self._extract_coin_from_snapshot(data["data"])
                if coin:
                    # Update latest snapshot
                    self.latest_snapshots[coin] = data["data"]
                    self.last_message_time[coin] = time.time()
                    self.message_count[coin] = self.message_count.get(coin, 0) + 1
                    
                    logger.debug(f"Received L4 snapshot for {coin} (message #{self.message_count[coin]})")
                    
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse WebSocket message: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    def _extract_coin_from_snapshot(self, snapshot: Dict) -> Optional[str]:
        """Extract coin symbol from snapshot data."""
        try:
            if isinstance(snapshot, dict) and "Snapshot" in snapshot:
                snapshot_data = snapshot["Snapshot"]
                
                # Single-coin format
                if isinstance(snapshot_data, dict) and "coin" in snapshot_data:
                    return snapshot_data["coin"]
                
                # Multi-coin format - return first coin for now
                elif isinstance(snapshot_data, list) and len(snapshot_data) > 0:
                    if isinstance(snapshot_data[0], list) and len(snapshot_data[0]) > 0:
                        return snapshot_data[0][0]
                        
            return None
            
        except Exception as e:
            logger.error(f"Error extracting coin from snapshot: {e}")
            return None
    
    async def _reconnect(self):
        """Attempt to reconnect with exponential backoff."""
        while self.running:
            logger.info(f"Attempting to reconnect in {self.current_reconnect_delay}s...")
            await asyncio.sleep(self.current_reconnect_delay)
            
            if await self.connect():
                logger.info("Reconnection successful")
                break
                
            # Exponential backoff
            self.current_reconnect_delay = min(
                self.current_reconnect_delay * 2,
                self.max_reconnect_delay
            )
    
    async def _resubscribe(self):
        """Re-subscribe to all markets after reconnection."""
        for coin in self.subscriptions:
            await self._send_subscription(coin)
    
    async def _send_subscription(self, coin: str, n_levels: int = 100) -> bool:
        """Send subscription message for a coin."""
        if not self.websocket or self.websocket.closed:
            return False
            
        subscription = {
            "method": "subscribe",
            "subscription": {
                "type": "l4Book",
                "coin": coin,
                "n_levels": n_levels
            }
        }
        
        try:
            await self.websocket.send(json.dumps(subscription))
            logger.info(f"Subscribed to L4 orderbook for {coin}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to subscribe to {coin}: {e}")
            return False
    
    async def subscribe(self, coin: str, n_levels: int = 100) -> bool:
        """Subscribe to L4 orderbook for a coin."""
        # Ensure connected
        if not self.websocket or self.websocket.closed:
            if not await self.connect():
                return False
        
        # Send subscription
        if await self._send_subscription(coin, n_levels):
            self.subscriptions.add(coin)
            return True
            
        return False
    
    async def unsubscribe(self, coin: str) -> bool:
        """Unsubscribe from a coin's orderbook."""
        if not self.websocket or self.websocket.closed:
            self.subscriptions.discard(coin)
            return True
            
        unsubscribe = {
            "method": "unsubscribe",
            "subscription": {
                "type": "l4Book",
                "coin": coin
            }
        }
        
        try:
            await self.websocket.send(json.dumps(unsubscribe))
            self.subscriptions.discard(coin)
            self.latest_snapshots.pop(coin, None)
            logger.info(f"Unsubscribed from {coin}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unsubscribe from {coin}: {e}")
            return False
    
    def get_latest_snapshot(self, coin: str) -> Optional[Dict]:
        """Get the latest snapshot for a coin (non-blocking)."""
        return self.latest_snapshots.get(coin)
    
    async def wait_for_snapshot(self, coin: str, timeout: float = 5.0) -> Optional[Dict]:
        """Wait for a fresh snapshot for a coin."""
        start_time = time.time()
        initial_count = self.message_count.get(coin, 0)
        
        # Subscribe if not already
        if coin not in self.subscriptions:
            if not await self.subscribe(coin):
                return None
        
        # Wait for a new message
        while time.time() - start_time < timeout:
            if self.message_count.get(coin, 0) > initial_count:
                return self.latest_snapshots.get(coin)
                
            await asyncio.sleep(0.01)  # Small sleep to prevent busy waiting
        
        # Return latest even if not fresh
        return self.latest_snapshots.get(coin)
    
    async def fetch_l4_snapshot(self, coin: str, timeout: float = 5.0) -> Optional[Dict]:
        """Fetch L4 snapshot for a coin with subscription management."""
        try:
            # Subscribe if not already subscribed
            if coin not in self.subscriptions:
                success = await self.subscribe(coin)
                if not success:
                    logger.error(f"Failed to subscribe to {coin}")
                    return None
            
            # Wait for fresh snapshot
            snapshot = await self.wait_for_snapshot(coin, timeout)
            if not snapshot:
                logger.warning(f"No snapshot received for {coin} within {timeout}s")
                return None
            
            return snapshot
            
        except Exception as e:
            logger.error(f"Error fetching L4 snapshot for {coin}: {e}")
            return None
    
    async def close(self):
        """Close WebSocket connection and cleanup."""
        self.running = False
        
        if self._receive_task and not self._receive_task.done():
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        
        if self.websocket and not self.websocket.closed:
            await self.websocket.close()
            
        self.subscriptions.clear()
        self.latest_snapshots.clear()
        logger.info("WebSocket client closed")


class OptimizedLiquidityCalculator:
    """Optimized liquidity depth calculator with caching."""
    
    def __init__(self):
        self._cache: Dict[str, Tuple[float, Dict]] = {}  # coin -> (timestamp, result)
        self._cache_ttl = 0.5  # Cache for 500ms
    
    def analyze_orderbook(self, snapshot: Dict, coin: str) -> Dict[str, Any]:
        """Analyze orderbook snapshot with caching."""
        # Check cache
        if coin in self._cache:
            cache_time, cached_result = self._cache[coin]
            if time.time() - cache_time < self._cache_ttl:
                return cached_result
        
        # Parse and analyze
        result = self._analyze_uncached(snapshot, coin)
        
        # Update cache
        self._cache[coin] = (time.time(), result)
        
        return result
    
    def _analyze_uncached(self, snapshot: Dict, coin: str) -> Dict[str, Any]:
        """Perform uncached orderbook analysis."""
        if not snapshot:
            return {}
        
        # Parse the snapshot
        bids, asks, best_bid, best_ask, mid_price = self._parse_l4_snapshot(snapshot, coin)
        
        if not bids and not asks:
            logger.warning(f"No valid orders found for {coin}")
            return {}
        
        # Calculate spread
        spread = best_ask - best_bid if best_bid and best_ask else None
        spread_pct = (spread / mid_price * 100) if spread and mid_price else None
        
        # Calculate depth efficiently
        depth_metrics = self._calculate_depth_optimized(bids, asks, mid_price)
        
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": mid_price,
            "spread": spread,
            "spread_pct": spread_pct,
            "bid_count": len(bids),
            "ask_count": len(asks),
            "total_bids": len(bids),
            "total_asks": len(asks),
            "orderbook_levels": len(bids) + len(asks),
            # Total depth
            "depth_5_pct": depth_metrics.get("5%", {}).get("total_depth", 0),
            "depth_10_pct": depth_metrics.get("10%", {}).get("total_depth", 0),
            "depth_25_pct": depth_metrics.get("25%", {}).get("total_depth", 0),
            # Individual bid/ask depth
            "bid_depth_5pct": depth_metrics.get("5%", {}).get("bid_depth", 0),
            "ask_depth_5pct": depth_metrics.get("5%", {}).get("ask_depth", 0),
            "bid_depth_10pct": depth_metrics.get("10%", {}).get("bid_depth", 0),
            "ask_depth_10pct": depth_metrics.get("10%", {}).get("ask_depth", 0),
            "bid_depth_25pct": depth_metrics.get("25%", {}).get("bid_depth", 0),
            "ask_depth_25pct": depth_metrics.get("25%", {}).get("ask_depth", 0)
        }
    
    def _parse_l4_snapshot(self, snapshot: Dict, target_coin: str) -> Tuple[List, List, float, float, float]:
        """Parse L4 snapshot efficiently."""
        bids = []
        asks = []
        
        # Extract order data based on format
        bid_orders, ask_orders = self._extract_orders(snapshot, target_coin)
        
        if not bid_orders and not ask_orders:
            return [], [], None, None, None
        
        # Process orders in bulk (vectorized approach would be even faster with numpy)
        for order in bid_orders:
            if not order.get("isTrigger", False):
                try:
                    bids.append({
                        "price": float(order["limitPx"]),
                        "size": float(order["sz"])
                    })
                except (KeyError, ValueError):
                    pass
        
        for order in ask_orders:
            if not order.get("isTrigger", False):
                try:
                    asks.append({
                        "price": float(order["limitPx"]),
                        "size": float(order["sz"])
                    })
                except (KeyError, ValueError):
                    pass
        
        # Sort once
        bids.sort(key=lambda x: x["price"], reverse=True)
        asks.sort(key=lambda x: x["price"])
        
        # Calculate best prices
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else None
        
        return bids, asks, best_bid, best_ask, mid_price
    
    def _extract_orders(self, snapshot: Dict, target_coin: str) -> Tuple[List, List]:
        """Extract bid and ask orders from snapshot."""
        if isinstance(snapshot, dict) and "Snapshot" in snapshot:
            snapshot_data = snapshot["Snapshot"]
            
            # Single-coin format
            if isinstance(snapshot_data, dict) and "coin" in snapshot_data:
                if snapshot_data["coin"].upper() == target_coin.upper():
                    levels = snapshot_data.get("levels", [])
                    if len(levels) >= 2:
                        return levels[0], levels[1]
            
            # Multi-coin format
            elif isinstance(snapshot_data, list):
                for entry in snapshot_data:
                    if isinstance(entry, list) and len(entry) >= 2:
                        if str(entry[0]).upper() == target_coin.upper():
                            coin_data = entry[1]
                            if isinstance(coin_data, list) and len(coin_data) >= 2:
                                return coin_data[0], coin_data[1]
        
        return [], []
    
    def _calculate_depth_optimized(self, bids: List[Dict], asks: List[Dict], mid_price: float) -> Dict:
        """Calculate depth with optimized algorithm."""
        if not mid_price or not bids or not asks:
            return {f"{int(p*100)}%": {"bid_depth": 0, "ask_depth": 0, "total_depth": 0} 
                   for p in [0.05, 0.1, 0.25]}
        
        percentages = [0.05, 0.1, 0.25]
        depth = {}
        
        for pct in percentages:
            bid_threshold = mid_price * (1 - pct)
            ask_threshold = mid_price * (1 + pct)
            
            # Optimized depth calculation using early termination
            bid_depth = 0
            for order in bids:
                if order["price"] < bid_threshold:
                    break
                bid_depth += order["size"] * order["price"]
            
            ask_depth = 0
            for order in asks:
                if order["price"] > ask_threshold:
                    break
                ask_depth += order["size"] * order["price"]
            
            depth[f"{int(pct*100)}%"] = {
                "bid_depth": bid_depth,
                "ask_depth": ask_depth,
                "total_depth": bid_depth + ask_depth
            }
        
        return depth
    
    def calculate_liquidity_depth(self, snapshot: Dict, market_config) -> Dict[str, Any]:
        """Calculate liquidity depth for a market - main entry point."""
        return self.analyze_orderbook(snapshot, market_config.symbol)
