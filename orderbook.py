"""OrderBook WebSocket client and liquidity depth calculator."""

import json
import asyncio
import websockets
import logging
from typing import Dict, List, Tuple, Optional, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

class OrderBookClient:
    def __init__(self, ws_url: str, coin: str):
        self.ws_url = ws_url
        self.coin = coin
        self.websocket = None
        self.current_snapshot = None
        
    async def connect(self):
        """Connect to WebSocket and subscribe to L4 book."""
        try:
            self.websocket = await websockets.connect(self.ws_url)
            
            # Subscribe to L4 book
            subscription = {
                "method": "subscribe",
                "subscription": {
                    "type": "l4Book",
                    "coin": self.coin
                }
            }
            
            await self.websocket.send(json.dumps(subscription))
            logger.info(f"Subscribed to L4 book for {self.coin}")
            
        except Exception as e:
            logger.error(f"Failed to connect to orderbook WebSocket: {e}")
            raise
            
    async def disconnect(self):
        """Disconnect from WebSocket."""
        if self.websocket:
            try:
                if not self.websocket.closed:
                    await self.websocket.close()
                logger.info("Disconnected from orderbook WebSocket")
            except Exception as e:
                logger.error(f"Error disconnecting WebSocket: {e}")
        self.websocket = None
            
    async def get_snapshot(self) -> Optional[Dict]:
        """Get the latest L4 orderbook snapshot."""
        if not self.websocket or self.websocket.closed:
            try:
                await self.connect()
            except Exception as e:
                logger.error(f"Failed to connect for snapshot: {e}")
                return None
            
        try:
            # Read messages until we get a snapshot
            max_attempts = 10
            for attempt in range(max_attempts):
                try:
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=5.0)
                    data = json.loads(message)
                    
                    # Check if it's an L4Book snapshot
                    if data.get("channel") == "l4Book" and "Snapshot" in data.get("data", {}):
                        self.current_snapshot = data["data"]["Snapshot"]
                        return self.current_snapshot
                except asyncio.TimeoutError:
                    if attempt == max_attempts - 1:
                        logger.error("Timeout waiting for orderbook snapshot after all attempts")
                        return None
                    continue
                    
            logger.warning("No snapshot received within max attempts")
            return None
            
        except websockets.exceptions.ConnectionClosed:
            logger.error("WebSocket connection closed while getting snapshot")
            self.websocket = None
            return None
        except Exception as e:
            logger.error(f"Error getting orderbook snapshot: {e}")
            return None

class LiquidityDepthCalculator:
    """Calculate liquidity depth from L4 orderbook data."""
    
    @staticmethod
    def parse_l4_snapshot(snapshot: Dict) -> Tuple[List, List, float, float, float]:
        """
        Parse L4 snapshot and build bid/ask levels.
        Returns: (bids, asks, best_bid, best_ask, mid_price)
        """
        levels = snapshot.get("levels", [])
        
        bids = []
        asks = []
        
        for order_list in levels:
            for order in order_list:
                # Skip trigger orders
                if order.get("isTrigger", False):
                    continue
                    
                px = order.get("limitPx")
                sz = order.get("sz")
                side = order.get("side")
                
                # Skip orders with invalid data
                if px is None or sz is None or side is None:
                    continue
                    
                try:
                    px = float(px)
                    sz = float(sz)
                except (TypeError, ValueError):
                    logger.debug(f"Invalid price/size values: px={px}, sz={sz}")
                    continue
                    
                # Skip invalid orders (zero/negative size or price)
                if sz <= 0 or px <= 0:
                    continue
                    
                # Additional validation - prices should be reasonable
                if px > 1e9 or sz > 1e9:  # Sanity check for unreasonable values
                    logger.warning(f"Unreasonable order values ignored: px={px}, sz={sz}")
                    continue
                
                if side == "B":
                    bids.append({"px": px, "sz": sz})
                elif side == "A":
                    asks.append({"px": px, "sz": sz})
                    
        # Sort bids descending, asks ascending
        bids.sort(key=lambda x: x["px"], reverse=True)
        asks.sort(key=lambda x: x["px"])
        
        best_bid = bids[0]["px"] if bids else None
        best_ask = asks[0]["px"] if asks else None
        
        if best_bid and best_ask:
            mid_price = (best_bid + best_ask) / 2
        else:
            mid_price = None
            
        return bids, asks, best_bid, best_ask, mid_price
        
    @staticmethod
    def aggregate_by_price(orders: List[Dict]) -> Dict[float, float]:
        """Aggregate orders by price level."""
        price_levels = defaultdict(float)
        for order in orders:
            price_levels[order["px"]] += order["sz"]
        return dict(price_levels)
        
    @staticmethod
    def calculate_depth_at_percentage(
        bids: List[Dict],
        asks: List[Dict],
        mid_price: float,
        percentage: float
    ) -> Dict[str, float]:
        """
        Calculate liquidity depth at percentage from mid price.
        Returns dict with bid_depth, ask_depth, and total_depth.
        """
        if not mid_price or mid_price <= 0:
            return {"bid_depth": 0, "ask_depth": 0, "total_depth": 0}
            
        # Calculate price thresholds
        bid_threshold = mid_price * (1 - percentage / 100)
        ask_threshold = mid_price * (1 + percentage / 100)
        
        # Sum bid liquidity within threshold
        bid_depth = sum(
            order["sz"] for order in bids
            if order["px"] >= bid_threshold
        )
        
        # Sum ask liquidity within threshold
        ask_depth = sum(
            order["sz"] for order in asks
            if order["px"] <= ask_threshold
        )
        
        return {
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "total_depth": bid_depth + ask_depth
        }
        
    def analyze_orderbook(self, snapshot: Dict) -> Dict[str, Any]:
        """
        Perform complete orderbook analysis.
        Returns metrics including spread, depth at various percentages, etc.
        """
        # Parse snapshot
        bids, asks, best_bid, best_ask, mid_price = self.parse_l4_snapshot(snapshot)
        
        if not bids or not asks:
            logger.warning("Empty orderbook")
            return {}
            
        # Calculate spread
        spread = best_ask - best_bid if (best_ask and best_bid) else None
        spread_pct = (spread / mid_price * 100) if (spread and mid_price and mid_price > 0) else None
        
        # Calculate depth at different percentages
        depth_5 = self.calculate_depth_at_percentage(bids, asks, mid_price, 5)
        depth_10 = self.calculate_depth_at_percentage(bids, asks, mid_price, 10)
        depth_25 = self.calculate_depth_at_percentage(bids, asks, mid_price, 25)
        
        # Aggregate by price level for L2 view
        bid_levels = self.aggregate_by_price(bids)
        ask_levels = self.aggregate_by_price(asks)
        
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": mid_price,
            "spread": spread,
            "spread_pct": spread_pct,
            
            "bid_depth_5pct": depth_5["bid_depth"],
            "ask_depth_5pct": depth_5["ask_depth"],
            "total_depth_5pct": depth_5["total_depth"],
            
            "bid_depth_10pct": depth_10["bid_depth"],
            "ask_depth_10pct": depth_10["ask_depth"],
            "total_depth_10pct": depth_10["total_depth"],
            
            "bid_depth_25pct": depth_25["bid_depth"],
            "ask_depth_25pct": depth_25["ask_depth"],
            "total_depth_25pct": depth_25["total_depth"],
            
            "orderbook_levels": len(bid_levels) + len(ask_levels),
            "total_bids": len(bids),
            "total_asks": len(asks)
        }
