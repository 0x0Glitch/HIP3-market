import aiohttp
import asyncio
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class MarketDataFetcher:
    def __init__(self, node_url: str, public_url: str, timeout_ms: int = 2500):
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
            # Try different node status endpoints
            response = None
            for endpoint_type in ["exchangeStatus", "clearinghouseState"]:
                body = {"type": endpoint_type}
                response = await self.post_json(self.node_url, body, self.timeout_s)
                logger.debug(f"Node {endpoint_type} response: {response is not None}")
                if response:
                    break
                    
            if not response:
                logger.warning("No response from local node endpoints")
                return False
                
            # Debug response structure
            logger.debug(f"Node response keys: {list(response.keys())}")
                
            # Get L1 time from response - check both possible fields
            l1_time_ms = response.get("responseTime", response.get("time", response.get("l1Time", 0)))
            if not l1_time_ms:
                logger.warning(f"No timestamp field found in node response. Keys: {list(response.keys())}")
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
        
        # Strategy: Local node for meta (universe), Public API for assetCtxs
        try:
            # 1. Get universe from local node (try both fresh check and direct attempt)
            universe = None
            node_latency_ms = None
            
            # First try with freshness check
            try_local_node = await self.is_node_fresh()
            
            # If freshness check fails, try direct meta request anyway
            if not try_local_node:
                logger.info("Node freshness check failed, trying direct meta request")
                try_local_node = True
                
            if try_local_node:
                start_time = time.time()
                meta_response = await self.post_json(self.node_url, {"type": "meta"}, self.timeout_s)
                node_latency_ms = int((time.time() - start_time) * 1000)
                
                if meta_response and "universe" in meta_response:
                    universe = meta_response["universe"]
                    logger.info(f"Got universe from local node (latency: {node_latency_ms}ms)")
                else:
                    logger.warning("Local node meta request failed or returned invalid data")
                    
            # 2. Get assetCtxs from public API
            logger.info("Fetching assetCtxs from public API")
            public_response = await self.post_json(self.public_url, {"type": "metaAndAssetCtxs"}, self.timeout_s * 3)
        
            if not public_response:
                logger.error("Failed to get response from public API")
                return None
            
            # Debug the response structure
            logger.debug(f"Public API response type: {type(public_response)}")
        
            if isinstance(public_response, str):
                logger.error(f"Public API returned string instead of JSON: {public_response[:200]}")
                return None
            
            if not isinstance(public_response, list) or len(public_response) != 2:
                logger.error(f"Unexpected metaAndAssetCtxs shape: {type(public_response)}, length: {len(public_response) if isinstance(public_response, list) else 'N/A'}")
                if isinstance(public_response, dict):
                    logger.error(f"Response keys: {list(public_response.keys())}")
                return None
            
            public_universe, asset_ctxs = public_response[0], public_response[1]
            
            # Use local universe if available, otherwise public
            final_universe = universe if universe else public_universe
            
            return self.normalize_mixed_response(final_universe, asset_ctxs, coin, node_latency_ms)
            
        except Exception as e:
            logger.error(f"Error in get_meta_and_asset_ctxs: {e}")
            return None
    
    def normalize_mixed_response(self, universe: list, asset_ctxs: list, target_coin: str, node_latency_ms: Optional[int]) -> Optional[Dict[str, Any]]:
        """Normalize mixed local/public response."""
        try:
            # Validate inputs
            if not universe or not asset_ctxs:
                logger.error(f"Empty universe ({len(universe) if universe else 0}) or asset_ctxs ({len(asset_ctxs) if asset_ctxs else 0})")
                return None
            
            # Find coin index in universe
            name_to_index = {meta.get("name"): i for i, meta in enumerate(universe) if meta.get("name")}
            index = name_to_index.get(target_coin)
            
            if index is None:
                available_coins = [meta.get("name", "Unknown") for meta in universe[:10]]
                logger.error(f"Coin '{target_coin}' not found in universe. Available coins: {available_coins}")
                return None
            
            # Bounds check for asset_ctxs
            if index >= len(asset_ctxs):
                logger.error(f"Asset index {index} out of range for assetCtxs (length: {len(asset_ctxs)})")
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
                "funding_rate_pct": float(ctx.get("funding") or 0) * 100,
                "open_interest": float(ctx.get("openInterest") or 0) * float(ctx.get("markPx") or 0),  # Convert to USD
                "volume_24h": float(ctx.get("dayNtlVlm") or 0),
                "impact_px_bid": float(ctx.get("impactPxs", [0, 0])[0]) if ctx.get("impactPxs") and len(ctx.get("impactPxs", [])) >= 2 else None,
                "impact_px_ask": float(ctx.get("impactPxs", [0, 0])[1]) if ctx.get("impactPxs") and len(ctx.get("impactPxs", [])) >= 2 else None,
                "premium": float(ctx.get("premium") or 0),
                "node_latency_ms": node_latency_ms,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error normalizing mixed response: {e}")
            return None
        
    def normalize_response(self, response: Dict, target_coin: str, node_latency_ms: Optional[int]) -> Optional[Dict[str, Any]]:
        """Normalize response format for specified coin."""
        try:
            # Debug logging to understand response structure
            logger.debug(f"Response type: {type(response)}")
            if isinstance(response, (list, dict)):
                logger.debug(f"Response structure: {str(response)[:200]}...")
            
            # Handle both dict and list response formats
            if isinstance(response, list):
                logger.debug(f"Got list response with {len(response)} elements")
                if len(response) < 2:
                    logger.error(f"Expected response list with at least 2 elements, got {len(response)}")
                    return None
                universe, asset_ctxs = response[0], response[1]
            elif isinstance(response, dict):
                universe = response.get("universe", [])
                asset_ctxs = response.get("assetCtxs", [])
                if not universe or not asset_ctxs:
                    logger.error(f"Missing universe or assetCtxs in dict response. Keys: {list(response.keys())}")
                    return None
            elif isinstance(response, str):
                logger.error(f"Response is a string, expected dict/list: {response[:200]}...")
                return None
            else:
                logger.error(f"Unexpected response type: {type(response)}")
                return None
                
            # Validate we have data
            if not universe or not asset_ctxs:
                logger.error(f"Empty universe ({len(universe) if universe else 0}) or asset_ctxs ({len(asset_ctxs) if asset_ctxs else 0})")
                return None
            
            # Find coin index in universe
            coin_to_index = {meta["name"]: i for i, meta in enumerate(universe)}
            index = coin_to_index.get(target_coin)
            
            if index is None:
                available_coins = [meta.get("name", "Unknown") for meta in universe[:10]]  # Show first 10
                logger.error(f"Coin '{target_coin}' not found in universe. Available coins: {available_coins}")
                return None
                
            # Bounds check for asset_ctxs
            if index >= len(asset_ctxs):
                logger.error(f"Index {index} out of range for asset_ctxs (length: {len(asset_ctxs)})")
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
                "funding_rate_pct": float(ctx.get("funding") or 0) * 100,
                "open_interest": float(ctx.get("openInterest") or 0) * float(ctx.get("markPx") or 0),  # Convert to USD
                "volume_24h": float(ctx.get("dayNtlVlm") or 0),
                "impact_px_bid": float(ctx.get("impactPxs", [0, 0])[0]) if ctx.get("impactPxs") and len(ctx.get("impactPxs", [])) >= 2 else None,
                "impact_px_ask": float(ctx.get("impactPxs", [0, 0])[1]) if ctx.get("impactPxs") and len(ctx.get("impactPxs", [])) >= 2 else None,
                "premium": float(ctx.get("premium") or 0),
                "node_latency_ms": node_latency_ms,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error normalizing response: {e}")
            return None
    
    async def fetch_market_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch market data for a specific symbol - main entry point."""
        return await self.get_meta_and_asset_ctxs(symbol)
