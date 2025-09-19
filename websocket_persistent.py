"""Optimized persistent WebSocket client for L4 orderbook data."""

import json
import logging
import asyncio
import websockets
import time
from typing import Dict, Optional, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class PersistentWebSocketClient:
    """WebSocket client with persistent connection and automatic reconnection."""
    
    def __init__(self, ws_url: str, coin: str, n_levels: int = 100):
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
                
                # Send subscription
                subscription = {
                    "method": "subscribe",
                    "subscription": {
                        "type": "l4Book",
                        "coin": self.coin,
                        "n_levels": self.n_levels
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
            while self.running and self.connection and not self.connection.closed:
                try:
                    # Wait for message with timeout
                    message = await asyncio.wait_for(self.connection.recv(), timeout=30.0)
                    
                    # Parse message
                    data = json.loads(message)
                    
                    # Check if this is L4 book data
                    if data.get("channel") == "l4Book" and data.get("data"):
                        self._last_snapshot = data["data"]
                        self._last_snapshot_time = time.time()
                        logger.debug(f"Updated L4 snapshot for {self.coin}")
                        
                except asyncio.TimeoutError:
                    # No message received in timeout period, connection might be stale
                    logger.warning(f"No message received in 30s for {self.coin}, checking connection")
                    if self.connection.closed:
                        await self._handle_disconnection()
                        break
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse WebSocket message: {e}")
                    
                except websockets.exceptions.ConnectionClosed:
                    logger.warning(f"WebSocket connection closed for {self.coin}")
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
        if self.connection and not self.connection.closed:
            return True
            
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
        Get a fresh L4 orderbook snapshot by waiting for the next update.
        
        Args:
            timeout: Maximum time to wait for a fresh snapshot
            
        Returns:
            Fresh L4 orderbook snapshot or None if timeout
        """
        if not await self.ensure_connected():
            return None
            
        # Mark current snapshot as old
        old_time = self._last_snapshot_time
        
        # Wait for new snapshot
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self._last_snapshot_time and self._last_snapshot_time > (old_time or 0):
                return self._last_snapshot
            await asyncio.sleep(0.1)
            
        # Return cached snapshot if available
        return self._last_snapshot
    
    async def close(self):
        """Close the WebSocket connection."""
        self.running = False
        
        async with self._lock:
            if self.connection:
                await self.connection.close()
                self.connection = None
                logger.info(f"Closed WebSocket connection for {self.coin}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        return {
            "coin": self.coin,
            "connected": self.connection is not None and not self.connection.closed,
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
        Returns: (bids, asks, best_bid, best_ask, mid_price)
        """
        logger.debug(f"Parsing L4 snapshot for {target_coin}")
        bids = []
        asks = []
        
        # Handle different snapshot formats
        snapshot_data = snapshot.get("Snapshot", snapshot)
        
        if isinstance(snapshot_data, dict) and "coin" in snapshot_data and "levels" in snapshot_data:
            # Single-coin format
            if snapshot_data["coin"].upper() == target_coin.upper():
                levels = snapshot_data["levels"]
                if isinstance(levels, list) and len(levels) >= 2:
                    bid_orders, ask_orders = levels[0], levels[1]
                else:
                    return [], [], None, None, None
            else:
                return [], [], None, None, None
                
        elif isinstance(snapshot_data, list):
            # Multi-coin format
            coin_data = None
            for entry in snapshot_data:
                if isinstance(entry, list) and len(entry) >= 2:
                    if str(entry[0]).upper() == target_coin.upper():
                        coin_data = entry[1]
                        break
                        
            if coin_data and isinstance(coin_data, list) and len(coin_data) >= 2:
                bid_orders, ask_orders = coin_data[0], coin_data[1]
            else:
                return [], [], None, None, None
        else:
            return [], [], None, None, None
        
        # Process orders
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
        """Analyze orderbook snapshot and return metrics."""
        if not snapshot:
            return {}
            
        bids, asks, best_bid, best_ask, mid_price = self.parse_l4_snapshot(snapshot, target_coin)
        
        if not bids and not asks:
            return {}
            
        spread = best_ask - best_bid if best_bid and best_ask else None
        spread_pct = (spread / mid_price * 100) if spread and mid_price else None
        
        depth_metrics = self.calculate_depth(bids, asks, mid_price)
        
        result = {
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
            "depth_5_pct": depth_metrics.get("5%", {}).get("total_depth", 0),
            "depth_10_pct": depth_metrics.get("10%", {}).get("total_depth", 0),
            "depth_25_pct": depth_metrics.get("25%", {}).get("total_depth", 0),
            "bid_depth_5pct": depth_metrics.get("5%", {}).get("bid_depth", 0),
            "ask_depth_5pct": depth_metrics.get("5%", {}).get("ask_depth", 0),
            "bid_depth_10pct": depth_metrics.get("10%", {}).get("bid_depth", 0),
            "ask_depth_10pct": depth_metrics.get("10%", {}).get("ask_depth", 0),
            "bid_depth_25pct": depth_metrics.get("25%", {}).get("bid_depth", 0),
            "ask_depth_25pct": depth_metrics.get("25%", {}).get("ask_depth", 0),
            "snapshot_timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return result
