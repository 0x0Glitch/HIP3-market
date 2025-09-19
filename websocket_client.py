"""Enhanced WebSocket client for Hyperliquid L4 orderbook data with optimizations."""

import asyncio
import json
import logging
import time
from typing import Dict, Optional, List, Tuple, Any, Set
from datetime import datetime, timezone
import websockets
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed, WebSocketException
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class OptimizedWebSocketClient:
    """Optimized long-lived WebSocket client for continuous L4 orderbook streaming."""
    
    def __init__(self, ws_url: str, reconnect_delay: float = 1.0, max_reconnect_delay: float = 30.0):
        self.ws_url = ws_url
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.current_reconnect_delay = reconnect_delay
        
        # Connection state
        self.websocket: Optional[WebSocketClientProtocol] = None
        self.writer_stream = None
        self.reader_stream = None
        self.running = False
        self._connect_lock = asyncio.Lock()
        self._receive_task: Optional[asyncio.Task] = None
        self._started = False
        
        # Subscription management
        self.subscriptions: Set[str] = set()
        self.pending_subscriptions: Set[str] = set()
        self.subscription_lock = asyncio.Lock()
        
        # Data management
        self.latest_snapshots: Dict[str, Dict] = {}
        self.snapshot_timestamps: Dict[str, float] = {}
        self.message_count: Dict[str, int] = {}
        
        # Performance metrics
        self.connection_health = {
            "last_successful_message": 0,
            "consecutive_failures": 0,
            "total_reconnects": 0,
            "messages_received": 0,
            "connection_start_time": 0
        }
        
        # Optimization: Use a small thread pool for JSON parsing
        self._json_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="json_parser")
    
    def _is_websocket_closed(self) -> bool:
        """Safely check if websocket connection is closed."""
        if not self.websocket:
            return True
        
        try:
            if hasattr(self.websocket, 'closed'):
                return self.websocket.closed
            elif hasattr(self.websocket, 'close_code'):
                return self.websocket.close_code is not None
            else:
                return True
        except AttributeError:
            return True
    
    async def start(self) -> bool:
        """Start the optimized WebSocket client with long-lived connection."""
        if self._started:
            return True
        
        self._started = True
        self.running = True
        
        success = await self.connect()
        if success:
            logger.info("✓ Long-lived WebSocket connection established")
        return success
        
    async def connect(self) -> bool:
        """Establish optimized long-lived WebSocket connection with stream splitting."""
        async with self._connect_lock:
            if self.websocket and not self._is_websocket_closed():
                return True
                
            try:
                logger.info(f"Establishing long-lived connection to: {self.ws_url}")
                
                self.websocket = await websockets.connect(
                    self.ws_url,
                    ping_interval=15,
                    ping_timeout=8,
                    close_timeout=5,
                    max_size=100 * 1024 * 1024,
                    compression="deflate",
                    max_queue=100,
                )
                
                self.reader_stream, self.writer_stream = self.websocket, self.websocket
                self.running = True
                self.current_reconnect_delay = self.reconnect_delay
                self.connection_health["connection_start_time"] = time.time()
                
                if self._receive_task:
                    self._receive_task.cancel()
                    try:
                        await self._receive_task
                    except asyncio.CancelledError:
                        pass
                
                self._receive_task = asyncio.create_task(self._optimized_receive_loop())
                
                if self.subscriptions:
                    await self._batch_resubscribe()
                
                logger.info("✓ Long-lived WebSocket connection established with stream splitting")
                return True
                
            except Exception as e:
                logger.error(f"Failed to establish long-lived connection: {e}")
                self.websocket = None
                self.reader_stream = None
                self.writer_stream = None
                return False
    
    async def _optimized_receive_loop(self):
        """Optimized receive loop with better error handling and performance."""
        logger.info("Starting optimized receive loop for long-lived connection")
        consecutive_errors = 0
        
        while self.running and self.reader_stream:
            try:
                message = await self.reader_stream.recv()
                
                if isinstance(message, str):
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        self._json_executor,
                        self._process_text_message,
                        message
                    )
                elif isinstance(message, bytes):
                    logger.debug(f"Received binary message: {len(message)} bytes")
                
                consecutive_errors = 0
                
            except ConnectionClosed:
                logger.warning("Long-lived WebSocket connection closed")
                self.websocket = None
                self.reader_stream = None
                self.writer_stream = None
                if self.running:
                    await self._reconnect()
                break
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error in optimized receive loop (#{consecutive_errors}): {e}")
                
                if consecutive_errors > 5:
                    logger.error("Too many consecutive errors, forcing reconnection")
                    self.websocket = None
                    if self.running:
                        await self._reconnect()
                    break
                
                await asyncio.sleep(min(0.1 * consecutive_errors, 1.0))
        
        logger.info("Optimized receive loop ended")
    
    def _process_text_message(self, message: str):
        """Process text message in thread pool for better performance."""
        try:
            data = json.loads(message)
            
            current_time = time.time()
            self.connection_health["messages_received"] += 1
            self.connection_health["last_successful_message"] = current_time
            self.connection_health["consecutive_failures"] = 0
            
            if data.get("channel") == "l4Book" and data.get("data"):
                coin = self._extract_coin_from_snapshot(data["data"])
                if coin:
                    self.latest_snapshots[coin] = data["data"]
                    self.snapshot_timestamps[coin] = current_time
                    self.message_count[coin] = self.message_count.get(coin, 0) + 1
                    
                    logger.debug(f"Processed L4 snapshot for {coin} (#{self.message_count[coin]})")
            
            elif data.get("channel") == "subscriptionResponse":
                sub_data = data.get("data", {})
                if "subscription" in sub_data:
                    coin = sub_data["subscription"].get("coin")
                    if coin:
                        logger.debug(f"Subscription confirmed for {coin}")
                        self.pending_subscriptions.discard(coin)
                        
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            self.connection_health["consecutive_failures"] += 1
        except Exception as e:
            logger.error(f"Message processing error: {e}")
            self.connection_health["consecutive_failures"] += 1
    
    def _extract_coin_from_snapshot(self, snapshot: Dict) -> Optional[str]:
        """Extract coin symbol from snapshot data."""
        try:
            if isinstance(snapshot, dict) and "Snapshot" in snapshot:
                snapshot_data = snapshot["Snapshot"]
                
                if isinstance(snapshot_data, dict) and "coin" in snapshot_data:
                    return snapshot_data["coin"]
                
                elif isinstance(snapshot_data, list) and len(snapshot_data) > 0:
                    if isinstance(snapshot_data[0], list) and len(snapshot_data[0]) > 0:
                        return snapshot_data[0][0]
                        
            return None
            
        except Exception as e:
            logger.error(f"Error extracting coin from snapshot: {e}")
            return None
    
    async def _reconnect(self):
        """Attempt to reconnect with exponential backoff."""
        self.connection_health["total_reconnects"] += 1
        
        while self.running:
            logger.info(f"Attempting to reconnect in {self.current_reconnect_delay}s...")
            await asyncio.sleep(self.current_reconnect_delay)
            
            if await self.connect():
                logger.info("Long-lived connection re-established")
                break
                
            self.current_reconnect_delay = min(
                self.current_reconnect_delay * 2,
                self.max_reconnect_delay
            )

    async def _batch_resubscribe(self):
        """Efficiently re-subscribe to all markets using batch operations."""
        if not self.subscriptions:
            return
        
        logger.info(f"Batch re-subscribing to {len(self.subscriptions)} markets")
        
        async with self.subscription_lock:
            tasks = []
            for coin in self.subscriptions:
                tasks.append(self._send_subscription(coin))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful = sum(1 for r in results if r is True)
            logger.info(f"Batch re-subscription completed: {successful}/{len(tasks)} successful")
    
    async def _send_subscription(self, coin: str, n_levels: int = 100) -> bool:
        """Send optimized subscription message for a coin."""
        if not self.writer_stream or self._is_websocket_closed():
            return False
        
        if coin in self.pending_subscriptions:
            logger.debug(f"Subscription already pending for {coin}")
            return True
            
        subscription = {
            "method": "subscribe",
            "subscription": {
                "type": "l4Book",
                "coin": coin,
                "n_levels": n_levels
            }
        }
        
        try:
            self.pending_subscriptions.add(coin)
            await self.writer_stream.send(json.dumps(subscription))
            logger.debug(f"Sent L4 subscription for {coin}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send subscription for {coin}: {e}")
            self.pending_subscriptions.discard(coin)
            return False
    
    async def subscribe(self, coin: str, n_levels: int = 100) -> bool:
        """Subscribe to L4 orderbook with optimized connection handling."""
        async with self.subscription_lock:
            if coin in self.subscriptions:
                logger.debug(f"Already subscribed to {coin}")
                return True
            
            if not self.writer_stream or self._is_websocket_closed():
                if not await self.connect():
                    return False
            
            if await self._send_subscription(coin, n_levels):
                self.subscriptions.add(coin)
                logger.info(f"✓ Subscribed to L4 orderbook for {coin}")
                return True
                
            return False
    
    async def _wait_for_fresh_snapshot(self, coin: str, timeout: float = 1.0) -> Optional[Dict]:
        """Wait for fresh snapshot from continuous stream."""
        start_time = time.time()
        initial_count = self.message_count.get(coin, 0)
        
        while time.time() - start_time < timeout:
            current_count = self.message_count.get(coin, 0)
            if current_count > initial_count:
                return self.latest_snapshots.get(coin)
            
            if self._is_websocket_closed():
                logger.debug(f"Connection lost while waiting for {coin}")
                break
                
            await asyncio.sleep(0.01)
        
        return None
    
    async def fetch_l4_snapshot(self, coin: str, timeout: float = 1.0, force_fresh: bool = True) -> Optional[Dict]:
        """Fetch L4 snapshot with option to force fresh data."""
        try:
            if coin not in self.subscriptions:
                success = await self.subscribe(coin)
                if not success:
                    logger.debug(f"Subscription failed for {coin}")
                    return None
            
            current_time = time.time()
            last_snapshot_time = self.snapshot_timestamps.get(coin, 0)
            snapshot_age = current_time - last_snapshot_time
            
            if force_fresh:
                logger.debug(f"Forcing fresh data for {coin} (cached age: {snapshot_age:.1f}s)")
                snapshot = await self._wait_for_fresh_snapshot(coin, timeout)
                
                if snapshot:
                    logger.debug(f"✓ Got fresh live data for {coin}")
                    return snapshot
                else:
                    if coin in self.latest_snapshots:
                        logger.warning(f"Fresh data timeout, using cached data for {coin} (age: {snapshot_age:.1f}s)")
                        return self.latest_snapshots[coin]
                    else:
                        logger.error(f"No fresh data and no cache available for {coin}")
                        return None
            else:
                if snapshot_age < 5.0 and coin in self.latest_snapshots:
                    logger.debug(f"Using cached snapshot for {coin} (age: {snapshot_age:.1f}s)")
                    return self.latest_snapshots[coin]
                
                snapshot = await self._wait_for_fresh_snapshot(coin, timeout)
                return snapshot or self.latest_snapshots.get(coin)
            
        except Exception as e:
            logger.debug(f"Error fetching snapshot for {coin}: {e}")
            return self.latest_snapshots.get(coin)
    
    def get_connection_health(self) -> Dict[str, Any]:
        """Get comprehensive WebSocket connection health metrics."""
        current_time = time.time()
        last_msg_age = current_time - self.connection_health["last_successful_message"] if self.connection_health["last_successful_message"] else float('inf')
        connection_uptime = current_time - self.connection_health["connection_start_time"] if self.connection_health["connection_start_time"] else 0
        
        return {
            "is_connected": not self._is_websocket_closed(),
            "is_healthy": not self._is_websocket_closed() and last_msg_age < 20,
            "connection_uptime_seconds": connection_uptime,
            "total_reconnects": self.connection_health["total_reconnects"],
            "consecutive_failures": self.connection_health["consecutive_failures"],
            "messages_received": self.connection_health["messages_received"],
            "last_message_age_seconds": last_msg_age,
            "subscription_count": len(self.subscriptions),
            "pending_subscriptions": len(self.pending_subscriptions),
            "cached_snapshots": len(self.latest_snapshots),
            "message_rate_per_minute": (self.connection_health["messages_received"] / max(connection_uptime / 60, 1)) if connection_uptime > 0 else 0
        }

    async def close(self):
        """Close WebSocket connection and cleanup."""
        self.running = False
        
        if self._receive_task and not self._receive_task.done():
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        
        if self.websocket and not self._is_websocket_closed():
            await self.websocket.close()
            
        self.subscriptions.clear()
        self.latest_snapshots.clear()
        
        if hasattr(self, '_json_executor'):
            self._json_executor.shutdown(wait=False)
            
        logger.info("WebSocket client closed")


class OptimizedLiquidityCalculator:
    """Optimized liquidity depth calculator with comprehensive metrics."""
    
    def __init__(self):
        self._cache: Dict[str, Tuple[float, Dict]] = {}
        self._cache_ttl = 0.5

    def calculate_liquidity_depth(self, snapshot: Dict, market) -> Dict[str, Any]:
        """Calculate comprehensive liquidity depth metrics from L4 snapshot."""
        try:
            if not snapshot or "Snapshot" not in snapshot:
                logger.warning("Invalid snapshot format")
                return self._get_default_metrics(market.symbol)
            
            snapshot_data = snapshot["Snapshot"]
            
            # Extract L4 orderbook data
            levels = snapshot_data.get("levels", [])
            if not levels or len(levels) < 2:
                logger.warning(f"No orderbook levels found for {market.symbol}")
                return self._get_default_metrics(market.symbol)
            
            bids_raw = levels[0] if len(levels) > 0 else []  # Buy orders
            asks_raw = levels[1] if len(levels) > 1 else []  # Sell orders
            
            if not bids_raw or not asks_raw:
                logger.warning(f"Missing bids or asks for {market.symbol}")
                return self._get_default_metrics(market.symbol)
            
            # Process and aggregate L4 to L2
            bids = self._process_l4_to_l2(bids_raw, "bid")
            asks = self._process_l4_to_l2(asks_raw, "ask") 
            
            if not bids or not asks:
                logger.warning(f"No valid bids/asks after processing for {market.symbol}")
                return self._get_default_metrics(market.symbol)
            
            # Sort orderbook
            bids = sorted(bids, key=lambda x: float(x["price"]), reverse=True)  # Highest first
            asks = sorted(asks, key=lambda x: float(x["price"]))  # Lowest first
            
            # Calculate core metrics
            best_bid = float(bids[0]["price"])
            best_ask = float(asks[0]["price"])
            mid_price = (best_bid + best_ask) / 2.0
            spread = best_ask - best_bid
            spread_pct = (spread / mid_price) * 100.0 if mid_price > 0 else 0.0
            
            # Calculate depth at different percentages
            depth_5pct = self._calculate_depth_at_percentage(bids, asks, mid_price, 0.05)
            depth_10pct = self._calculate_depth_at_percentage(bids, asks, mid_price, 0.10)
            depth_25pct = self._calculate_depth_at_percentage(bids, asks, mid_price, 0.25)
            
            return {
                # Core orderbook metrics
                "mid_price": mid_price,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "spread_pct": spread_pct,
                
                # 5% depth
                "bid_depth_5pct": depth_5pct["bid_depth_usd"],
                "ask_depth_5pct": depth_5pct["ask_depth_usd"],
                "total_depth_5pct": depth_5pct["total_depth_usd"],
                
                # 10% depth  
                "bid_depth_10pct": depth_10pct["bid_depth_usd"],
                "ask_depth_10pct": depth_10pct["ask_depth_usd"],
                "total_depth_10pct": depth_10pct["total_depth_usd"],
                
                # 25% depth
                "bid_depth_25pct": depth_25pct["bid_depth_usd"],
                "ask_depth_25pct": depth_25pct["ask_depth_usd"],
                "total_depth_25pct": depth_25pct["total_depth_usd"],
                
                # Impact prices
                "impact_px_bid": depth_5pct.get("impact_bid_price", best_bid),
                "impact_px_ask": depth_5pct.get("impact_ask_price", best_ask),
                
                # Orderbook metadata
                "orderbook_levels": len(bids) + len(asks),
                "total_bids": len(bids),
                "total_asks": len(asks)
            }
            
        except Exception as e:
            logger.error(f"Error calculating liquidity depth for {market.symbol}: {e}")
            return self._get_default_metrics(market.symbol)
    
    def _process_l4_to_l2(self, orders: List, side: str) -> List[Dict]:
        """Convert L4 orders to L2 aggregated levels."""
        try:
            if not orders:
                return []
            
            l2_levels = {}
            
            for order in orders:
                if not isinstance(order, dict):
                    continue
                
                # Skip trigger orders for liquidity calculation
                if order.get("isTrigger", False):
                    continue
                
                price = order.get("px")
                size = order.get("sz")
                
                if price is None or size is None:
                    continue
                
                try:
                    price_float = float(price)
                    size_float = float(size)
                    
                    if price_float <= 0 or size_float <= 0:
                        continue
                    
                    # Aggregate by price level
                    if price_float in l2_levels:
                        l2_levels[price_float] += size_float
                    else:
                        l2_levels[price_float] = size_float
                        
                except (ValueError, TypeError):
                    continue
            
            # Convert to list format
            return [
                {"price": price, "size": size}
                for price, size in l2_levels.items()
                if size > 0
            ]
            
        except Exception as e:
            logger.error(f"Error processing L4 to L2 for {side}: {e}")
            return []
    
    def _calculate_depth_at_percentage(self, bids: List, asks: List, mid_price: float, percentage: float) -> Dict:
        """Calculate liquidity depth at a specific percentage from mid price."""
        try:
            bid_threshold = mid_price * (1 - percentage)
            ask_threshold = mid_price * (1 + percentage)
            
            # Calculate bid depth (buy-side liquidity)
            bid_depth_usd = 0.0
            impact_bid_price = mid_price
            
            for bid in bids:
                price = float(bid["price"])
                size = float(bid["size"])
                
                if price >= bid_threshold:
                    bid_depth_usd += price * size
                    impact_bid_price = price  # Track lowest price we hit
                else:
                    break
            
            # Calculate ask depth (sell-side liquidity)
            ask_depth_usd = 0.0
            impact_ask_price = mid_price
            
            for ask in asks:
                price = float(ask["price"])
                size = float(ask["size"])
                
                if price <= ask_threshold:
                    ask_depth_usd += price * size
                    impact_ask_price = price  # Track highest price we hit
                else:
                    break
            
            total_depth_usd = bid_depth_usd + ask_depth_usd
            
            return {
                "bid_depth_usd": bid_depth_usd,
                "ask_depth_usd": ask_depth_usd,
                "total_depth_usd": total_depth_usd,
                "impact_bid_price": impact_bid_price,
                "impact_ask_price": impact_ask_price
            }
            
        except Exception as e:
            logger.error(f"Error calculating depth at {percentage*100}%: {e}")
            return {
                "bid_depth_usd": 0.0,
                "ask_depth_usd": 0.0,
                "total_depth_usd": 0.0,
                "impact_bid_price": mid_price,
                "impact_ask_price": mid_price
            }
    
    def _get_default_metrics(self, coin: str) -> Dict[str, Any]:
        """Return default metrics when calculation fails."""
        return {
            # Core metrics - using 0 instead of None to avoid NULL in DB
            "mid_price": 0.0,
            "best_bid": 0.0,
            "best_ask": 0.0,
            "spread": 0.0,
            "spread_pct": 0.0,
            
            # Depth metrics - using 0 instead of None
            "bid_depth_5pct": 0.0,
            "ask_depth_5pct": 0.0,
            "total_depth_5pct": 0.0,
            "bid_depth_10pct": 0.0,
            "ask_depth_10pct": 0.0,
            "total_depth_10pct": 0.0,
            "bid_depth_25pct": 0.0,
            "ask_depth_25pct": 0.0,
            "total_depth_25pct": 0.0,
            
            # Impact prices
            "impact_px_bid": 0.0,
            "impact_px_ask": 0.0,
            
            # Counts
            "orderbook_levels": 0,
            "total_bids": 0,
            "total_asks": 0
        }