"""Optimized WebSocket client with subscribe/resubscribe architecture for L4 orderbook data."""

import json
import logging
import asyncio
import websockets
import websockets.exceptions
import time
import random
import hashlib
from typing import Dict, Optional, Any
from collections import deque

logger = logging.getLogger(__name__)


class WebSocketClient:
    """WebSocket client with optimized subscribe/resubscribe pattern and connection stability."""

    def __init__(self, ws_url: str, coin: str, n_levels: int = 100):
        # Fix the WebSocket URL - the Rust server expects /ws endpoint
        if not ws_url.endswith('/ws'):
            ws_url = ws_url.rstrip('/') + '/ws'
        self.ws_url = ws_url
        self.coin = coin
        self.n_levels = n_levels
        self.ws = None
        self.running = False
        self._lock = asyncio.Lock()

        # Snapshot state (rebuilt on each reconnection)
        self._current_snapshot = None
        self._snapshot_time = None
        self._snapshot_count = 0
        self._last_metrics_hash = None  # Prevent duplicate data insertion

        # Reconnection management
        self._reconnect_count = 0
        self._max_reconnect_delay = 60.0
        self._base_reconnect_delay = 1.0

        # Health monitoring
        self._last_message_time = None
        self._heartbeat_task = None
        self._connection_healthy = False

        # Update buffering during brief disconnections
        self._update_buffer = deque(maxlen=1000)
        self._buffering = False

    def _is_ws_open(self) -> bool:
        """Check if WebSocket connection is open."""
        if not self.ws:
            return False
        try:
            # WebSocket state: 0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED
            return self.ws.state.value == 1  # OPEN state
        except:
            return False

    async def connect(self) -> bool:
        """Establish WebSocket connection with TCP keepalive and health monitoring."""
        try:
            async with self._lock:
                if self.ws and self._is_ws_open():
                    return True

                logger.info(f"Connecting to {self.ws_url} for {self.coin}")

                # Connect with optimized settings for stability
                self.ws = await websockets.connect(
                    self.ws_url,
                    ping_interval=20,           # Ping every 20s to keep connection alive
                    ping_timeout=10,            # Timeout if no pong in 10s
                    close_timeout=5,            # Quick close on disconnect
                    max_size=10 * 1024 * 1024   # 10MB max message size
                )

                # Subscribe to L4 orderbook
                subscription = {
                    "method": "subscribe",
                    "subscription": {
                        "type": "l4Book",
                        "coin": self.coin
                    }
                }

                await self.ws.send(json.dumps(subscription))
                self.running = True
                self._reconnect_count = 0
                self._connection_healthy = True
                self._last_message_time = time.time()

                logger.info(f"✅ Connected and subscribed to {self.coin} L4 orderbook")

                # Start background tasks
                asyncio.create_task(self._listen_loop())
                if not self._heartbeat_task or self._heartbeat_task.done():
                    self._heartbeat_task = asyncio.create_task(self._heartbeat())

                return True

        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    async def _heartbeat(self):
        """Monitor connection health and trigger reconnection if stale."""
        while self.running:
            try:
                await asyncio.sleep(30)

                if not self._last_message_time:
                    continue

                age = time.time() - self._last_message_time

                # If no message in 60s, connection is likely stale
                if age > 60:
                    logger.warning(f"No messages for {age:.0f}s, connection may be stale")
                    self._connection_healthy = False
                    await self._handle_disconnection()
                    break

            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                break

    async def _listen_loop(self):
        """Listen for WebSocket messages and rebuild orderbook on each snapshot."""
        try:
            while self.running and self.ws:
                try:
                    # Wait for message with timeout
                    message = await asyncio.wait_for(self.ws.recv(), timeout=35.0)
                    self._last_message_time = time.time()

                    # Parse message
                    data = json.loads(message)

                    # Handle different message types
                    if data.get("channel") == "subscriptionResponse":
                        logger.info(f"✅ Subscription confirmed for {self.coin}")

                    elif data.get("channel") == "l4Book" and data.get("data"):
                        book_data = data["data"]

                        # Handle snapshot - REBUILD orderbook from scratch
                        if "Snapshot" in book_data:
                            snapshot = book_data["Snapshot"]
                            self._current_snapshot = snapshot
                            self._snapshot_time = time.time()
                            self._snapshot_count += 1

                            # Stop buffering - we have fresh data
                            if self._buffering:
                                self._buffering = False
                                self._update_buffer.clear()

                            if "levels" in snapshot:
                                levels = snapshot["levels"]
                                if isinstance(levels, list) and len(levels) >= 2:
                                    bid_count = len(levels[0]) if levels[0] else 0
                                    ask_count = len(levels[1]) if levels[1] else 0
                                    logger.info(f"📸 Snapshot #{self._snapshot_count}: {bid_count} bids, {ask_count} asks")

                        # Updates are ignored - we only use snapshots
                        # This eliminates all state drift and race conditions

                    elif data.get("channel") == "error":
                        logger.error(f"❌ Server error: {data.get('data')}")

                except asyncio.TimeoutError:
                    logger.warning(f"No message in 35s for {self.coin}")
                    await self._handle_disconnection()
                    break

                except websockets.exceptions.ConnectionClosed:
                    logger.warning(f"Connection closed for {self.coin}")
                    await self._handle_disconnection()
                    break

                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON: {e}")

        except Exception as e:
            logger.error(f"Listen loop error: {e}")
            await self._handle_disconnection()

    async def _handle_disconnection(self):
        """Handle disconnection with exponential backoff and jitter."""
        self.ws = None
        self._connection_healthy = False

        if not self.running:
            return

        # Start buffering updates (will be discarded on reconnect with fresh snapshot)
        self._buffering = True

        # Calculate backoff with jitter to prevent thundering herd
        self._reconnect_count += 1
        delay = min(
            self._base_reconnect_delay * (2 ** (self._reconnect_count - 1)),
            self._max_reconnect_delay
        )
        # Add jitter: +/- 20% randomness
        jitter = delay * 0.2 * (random.random() * 2 - 1)
        delay = delay + jitter

        logger.warning(f"🔌 Reconnecting in {delay:.1f}s (attempt #{self._reconnect_count})")
        await asyncio.sleep(delay)

        if await self.connect():
            logger.info(f"✅ Reconnected after {self._reconnect_count} attempts")
            self._reconnect_count = 0
        else:
            # Retry with next backoff
            await self._handle_disconnection()

    async def ensure_connected(self) -> bool:
        """Ensure connection is active."""
        if self.ws and self._is_ws_open() and self._connection_healthy:
            return True
        return await self.connect()

    def is_connected(self) -> bool:
        """Check if connected."""
        return self.ws is not None and self._is_ws_open() and self._connection_healthy

    def is_healthy(self) -> bool:
        """Check if connection is healthy with fresh data."""
        if not self.is_connected():
            return False

        if not self._current_snapshot:
            return False

        # Data should be fresh (within 60s)
        if self._snapshot_time:
            age = time.time() - self._snapshot_time
            if age > 60:
                logger.debug(f"Snapshot is {age:.0f}s old")
                return False

        return True

    def get_snapshot(self, max_age_seconds: Optional[float] = None) -> Optional[Dict]:
        """
        Get current orderbook snapshot.

        Args:
            max_age_seconds: Maximum snapshot age. If None, return any available snapshot.

        Returns:
            Orderbook snapshot or None if unavailable/too old.
        """
        if not self._current_snapshot:
            return None

        # Check freshness
        if max_age_seconds and self._snapshot_time:
            age = time.time() - self._snapshot_time
            if age > max_age_seconds:
                logger.warning(f"{self.coin}: Orderbook snapshot too old: {age:.1f}s (max: {max_age_seconds}s)")
                return None

        return self._current_snapshot

    async def get_fresh_metrics(self) -> Dict[str, Any]:
        """FORCE fresh orderbook metrics by resubscribing - NO CACHE, ALWAYS FRESH."""
        # Ensure connected
        if not self.is_connected():
            logger.warning(f"{self.coin}: Not connected for fresh orderbook data")
            return {"orderbook": None}

        # ALWAYS resubscribe to get guaranteed fresh snapshot (no cache tolerance)
        logger.debug(f"{self.coin}: Forcing resubscription for fresh orderbook data...")
        fresh_snapshot = await self.get_fresh_snapshot(timeout=4.0)  # Slightly longer timeout
        
        if not fresh_snapshot:
            logger.warning(f"{self.coin}: Failed to get fresh snapshot within 4s - orderbook server may be slow")
            return {"orderbook": None}

        # Analyze orderbook
        depth_calc = LiquidityDepthCalculator()
        metrics = depth_calc.analyze_orderbook(fresh_snapshot, self.coin)

        if not metrics or not any(key in metrics for key in ['best_bid', 'best_ask', 'mid_price']):
            logger.error(f"{self.coin}: Failed to extract metrics from fresh snapshot")
            return {"orderbook": None}

        metrics["orderbook"] = fresh_snapshot
        logger.info(f"✅ {self.coin}: Fresh orderbook metrics obtained via resubscription")

        return metrics

    async def get_fresh_snapshot(self, timeout: float = 5.0) -> Optional[Dict]:
        """
        Get fresh snapshot by unsubscribing and resubscribing.
        This guarantees a new snapshot instead of cached data.
        """
        if not self.ws or not self._is_ws_open():
            return None

        try:
            # Step 1: Clear any existing cached snapshot to force fresh data
            old_snapshot_time = self._snapshot_time
            self._current_snapshot = None
            self._snapshot_time = None
            
            # Step 2: Unsubscribe from current subscription
            unsubscribe_msg = {
                "method": "unsubscribe",
                "subscription": {
                    "type": "l4Book",
                    "coin": self.coin
                }
            }
            await self.ws.send(json.dumps(unsubscribe_msg))
            await asyncio.sleep(0.1)  # Brief pause
            
            # Step 3: Subscribe again to trigger fresh snapshot
            subscribe_msg = {
                "method": "subscribe", 
                "subscription": {
                    "type": "l4Book",
                    "coin": self.coin
                }
            }
            await self.ws.send(json.dumps(subscribe_msg))
            
            # Step 4: Wait for fresh snapshot (guaranteed new since we cleared cache)
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                # Check if we got a completely new snapshot
                if self._snapshot_time and self._current_snapshot:
                    logger.debug(f"✅ Got fresh snapshot for {self.coin} (age: {time.time() - self._snapshot_time:.2f}s)")
                    return self._current_snapshot
                    
                await asyncio.sleep(0.05)  # Check every 50ms
            
            logger.warning(f"⚠️  Timeout waiting for fresh snapshot for {self.coin}")
            return self._current_snapshot  # Return existing if timeout
            
        except Exception as e:
            logger.error(f"Error getting fresh snapshot for {self.coin}: {e}")
            return self._current_snapshot

    def get_latest_metrics(self) -> Dict[str, Any]:
        """Get latest cached metrics with 4-second freshness limit."""
        if not self.is_connected():
            return {"orderbook": None}

        # STRICT: Only allow 4-second old snapshots for real-time trading
        snapshot = self.get_snapshot(max_age_seconds=4.0)
        if not snapshot:
            logger.debug(f"{self.coin}: Orderbook snapshot too old (>4s), need fresh data")
            return {"orderbook": None}

        depth_calc = LiquidityDepthCalculator()
        metrics = depth_calc.analyze_orderbook(snapshot, self.coin)

        if not metrics:
            return {"orderbook": None}

        metrics["orderbook"] = snapshot
        return metrics

    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        return {
            "coin": self.coin,
            "connected": self.is_connected(),
            "healthy": self.is_healthy(),
            "running": self.running,
            "snapshot_age": time.time() - self._snapshot_time if self._snapshot_time else None,
            "reconnect_count": self._reconnect_count,
            "snapshot_count": self._snapshot_count,
            "has_snapshot": self._current_snapshot is not None,
            "buffering": self._buffering,
            "buffer_size": len(self._update_buffer)
        }

    async def close(self):
        """Close connection."""
        self.running = False
        self._connection_healthy = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        async with self._lock:
            if self.ws:
                await self.ws.close()
                self.ws = None
                logger.info(f"Closed connection for {self.coin}")

    async def disconnect(self):
        """Alias for close()."""
        await self.close()


class LiquidityDepthCalculator:
    """Calculate liquidity depth from L4 orderbook data."""

    @staticmethod
    def parse_l4_snapshot(snapshot: Dict, target_coin: str = 'LINK') -> tuple:
        """
        Parse L4 snapshot and build bid/ask levels.
        Returns: (bids, asks, best_bid, best_ask, mid_price)
        """
        bids = []
        asks = []

        # Validate snapshot format
        if "coin" in snapshot and "levels" in snapshot:
            if snapshot["coin"].upper() == target_coin.upper():
                levels = snapshot["levels"]
                if isinstance(levels, list) and len(levels) >= 2:
                    bid_orders, ask_orders = levels[0], levels[1]
                else:
                    return [], [], None, None, None
            else:
                return [], [], None, None, None
        else:
            return [], [], None, None, None

        # Process bid orders
        for order in bid_orders:
            try:
                if not order.get("isTrigger", False):
                    price = float(order["limitPx"])
                    size = float(order["sz"])
                    bids.append({"price": price, "size": size})
            except (KeyError, ValueError, TypeError):
                continue

        # Process ask orders
        for order in ask_orders:
            try:
                if not order.get("isTrigger", False):
                    price = float(order["limitPx"])
                    size = float(order["sz"])
                    asks.append({"price": price, "size": size})
            except (KeyError, ValueError, TypeError):
                continue

        # Sort orders
        bids = sorted(bids, key=lambda x: x["price"], reverse=True)
        asks = sorted(asks, key=lambda x: x["price"])

        # Calculate best prices
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else None

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

        # Parse snapshot
        bids, asks, best_bid, best_ask, mid_price = self.parse_l4_snapshot(snapshot, target_coin)

        if not bids and not asks:
            return {}

        # Calculate spread
        spread = best_ask - best_bid if best_bid and best_ask else None
        spread_pct = (spread / mid_price * 100) if spread and mid_price else None

        # Calculate depth metrics
        depth_metrics = self.calculate_depth(bids, asks, mid_price)

        # Build result
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

        # Add depth metrics
        if depth_metrics:
            for pct_key, pct_data in depth_metrics.items():
                if pct_key == "5%":
                    result["total_depth_5pct"] = pct_data["total_depth"]
                    result["bid_depth_5pct"] = pct_data["bid_depth"]
                    result["ask_depth_5pct"] = pct_data["ask_depth"]
                elif pct_key == "10%":
                    result["total_depth_10pct"] = pct_data["total_depth"]
                    result["bid_depth_10pct"] = pct_data["bid_depth"]
                    result["ask_depth_10pct"] = pct_data["ask_depth"]
                elif pct_key == "25%":
                    result["total_depth_25pct"] = pct_data["total_depth"]
                    result["bid_depth_25pct"] = pct_data["bid_depth"]
                    result["ask_depth_25pct"] = pct_data["ask_depth"]

        return result
