import asyncio
import aiohttp
import json
import logging
import time
from typing import Dict, Optional, Any, List
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class PublicRestClient:
    """Simple REST client for Hyperliquid public API - direct calls only."""

    def __init__(self, timeout_s: float = 5.0):
        self.api_url = "https://api.hyperliquid.xyz/info"
        self.timeout_s = timeout_s
        self.session: Optional[aiohttp.ClientSession] = None

    async def connect(self) -> bool:
        """Initialize HTTP session."""
        try:
            if self.session is None:
                self.session = aiohttp.ClientSession()
            logger.info("✅ REST API client initialized")
            return True
        except Exception as e:
            logger.error(f"REST API connection failed: {e}")
            return False

    async def get_market_data(self, coin: str) -> Optional[Dict[str, Any]]:
        """Get fresh market data for a specific coin via direct REST call."""
        try:
            payload = {"type": "metaAndAssetCtxs"}
            
            async with self.session.post(
                self.api_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=self.timeout_s)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._extract_coin_data(data, coin)
                else:
                    logger.error(f"REST API error: {response.status}")
                    return None

        except Exception as e:
            logger.error(f"Error fetching market data via REST: {e}")
            return None

    async def disconnect(self):
        """Clean up HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None

    def _extract_coin_data(self, payload: Dict, coin: str) -> Optional[Dict[str, Any]]:
        """Extract data for a specific coin from API response."""
        try:
            # Parse response structure: [universe_obj, asset_ctxs]
            if not isinstance(payload, list) or len(payload) != 2:
                return None

            universe_obj, asset_ctxs = payload

            # Extract universe array from wrapper object if needed
            if isinstance(universe_obj, dict) and "universe" in universe_obj:
                universe = universe_obj["universe"]
            else:
                universe = universe_obj

            if not universe or not asset_ctxs:
                return None

            # Find coin index
            name_to_index = {
                meta.get("name"): i
                for i, meta in enumerate(universe)
                if meta.get("name")
            }
            index = name_to_index.get(coin)

            if index is None:
                available = [m.get("name") for m in universe[:5]]
                logger.warning(f"Coin '{coin}' not found. Available: {available}")
                return None

            # Bounds check
            if index >= len(asset_ctxs):
                logger.error(f"Index {index} out of range for asset_ctxs")
                return None

            meta = universe[index]
            ctx = asset_ctxs[index]

            # Normalize to expected format
            return {
                "coin": meta["name"],
                "max_leverage": meta.get("maxLeverage"),
                "only_isolated": meta.get("onlyIsolated", False),
                "mark_price": float(ctx.get("markPx") or 0),
                "oracle_price": float(ctx.get("oraclePx") or 0),
                "mid_price": float(ctx.get("midPx") or 0),
                "funding_rate_pct": float(ctx.get("funding") or 0) * 100,
                "open_interest": float(ctx.get("openInterest") or 0) * float(ctx.get("markPx") or 0),
                "volume_24h": float(ctx.get("dayNtlVlm") or 0),
                "impact_px_bid": float(ctx.get("impactPxs", [0, 0])[0]) if ctx.get("impactPxs") and len(ctx.get("impactPxs", [])) >= 2 else None,
                "impact_px_ask": float(ctx.get("impactPxs", [0, 0])[1]) if ctx.get("impactPxs") and len(ctx.get("impactPxs", [])) >= 2 else None,
                "premium": float(ctx.get("premium") or 0),
                "node_latency_ms": None,  # Not applicable for WS
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

        except Exception as e:
            logger.error(f"Error extracting market data for {coin}: {e}")
            return None
