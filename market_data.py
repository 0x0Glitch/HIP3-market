"""Market data fetching from node /info endpoint."""

import aiohttp
import asyncio
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class MarketDataFetcher:
    def __init__(self, node_url: str, public_url: str, timeout_ms: int = 250):
        self.node_url = node_url
        self.public_url = public_url
        self.timeout_s = timeout_ms / 1000.0
        self.last_successful_node_time = time.time()
        
    async def post_json(self, url: str, body: Dict[str, Any], timeout: float) -> Optional[Dict]:
        """Make POST request with JSON body."""
        try:
            timeout_obj = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                async with session.post(url, json=body) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        logger.warning(f"HTTP {resp.status} from {url}")
                        return None
        except asyncio.TimeoutError:
            logger.debug(f"Timeout requesting {url}")
            return None
        except Exception as e:
            logger.error(f"Error posting to {url}: {e}")
            return None
            
    async def is_node_fresh(self) -> bool:
        """Check if local node state is recent."""
        try:
            body = {"type": "exchangeStatus"}
            response = await self.post_json(self.node_url, body, self.timeout_s)
            
            if not response:
                return False
                
            # Get L1 time from response - check both possible fields
            l1_time_ms = response.get("responseTime", response.get("time", response.get("l1Time", 0)))
            if not l1_time_ms:
                logger.warning("No timestamp field found in exchangeStatus response")
                return False
                
            # Convert to milliseconds if needed (check if timestamp is in seconds)
            if l1_time_ms < 1000000000000:  # Likely in seconds if less than this
                l1_time_ms = l1_time_ms * 1000
                
            # Check if node is within 3 seconds of current time
            current_time_ms = time.time() * 1000
            drift_ms = abs(current_time_ms - l1_time_ms)
            
            is_fresh = drift_ms < 3000  # 3 second threshold
            
            if is_fresh:
                self.last_successful_node_time = time.time()
            else:
                logger.debug(f"Node time drift: {drift_ms}ms (threshold: 3000ms)")
                
            return is_fresh
            
        except Exception as e:
            logger.error(f"Error checking node freshness: {e}")
            return False
            
    async def get_meta_and_asset_ctxs(self, coin: str) -> Optional[Dict[str, Any]]:
        """Get market metadata and asset contexts."""
        body = {"type": "metaAndAssetCtxs"}
        
        # Try local node first if fresh
        if await self.is_node_fresh():
            start_time = time.time()
            response = await self.post_json(self.node_url, body, self.timeout_s)
            latency_ms = int((time.time() - start_time) * 1000)
            
            if response:
                logger.debug(f"Got market data from local node (latency: {latency_ms}ms)")
                return self.normalize_response(response, coin, latency_ms)
                
        # Fallback to public API
        logger.info("Using public API for market data")
        response = await self.post_json(self.public_url, body, self.timeout_s * 3)
        
        if response:
            return self.normalize_response(response, coin, None)
            
        return None
        
    def normalize_response(self, response: Dict, target_coin: str, node_latency_ms: Optional[int]) -> Optional[Dict[str, Any]]:
        """Normalize response format for specified coin."""
        try:
            universe = response.get("universe", [])
            asset_ctxs = response.get("assetCtxs", [])
            
            # Find coin index in universe
            coin_to_index = {meta["name"]: i for i, meta in enumerate(universe)}
            index = coin_to_index.get(target_coin)
            
            if index is None:
                logger.error(f"Coin '{target_coin}' not found in universe")
                return None
                
            meta = universe[index]
            ctx = asset_ctxs[index]
            
            return {
                "coin": meta["name"],
                "max_leverage": meta.get("maxLeverage"),
                "only_isolated": meta.get("onlyIsolated", False),
                "mark_price": float(ctx.get("markPx") or 0),
                "oracle_price": float(ctx.get("oraclePx") or 0),
                "mid_price": float(ctx.get("midPx") or 0),
                "funding_rate": float(ctx.get("funding") or 0),
                "open_interest": float(ctx.get("openInterest") or 0),
                "volume_24h": float(ctx.get("dayNtlVlm") or 0),
                "impact_px_bid": float(ctx.get("impactPxs", [0, 0])[0]) if ctx.get("impactPxs") and len(ctx.get("impactPxs", [])) >= 2 else None,
                "impact_px_ask": float(ctx.get("impactPxs", [0, 0])[1]) if ctx.get("impactPxs") and len(ctx.get("impactPxs", [])) >= 2 else None,
                "premium": float(ctx.get("premium") or 0),
                "node_latency_ms": node_latency_ms,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error normalizing response: {e}")
            return None
