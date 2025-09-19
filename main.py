"""Multi-market monitoring system."""

import asyncio
import logging
import time
from typing import Dict, Any, Optional

from config import Config
from database import Database
from market_data import MarketDataFetcher
from websocket_client import OrderBookWebSocketClient, OptimizedLiquidityCalculator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MarketMonitor:
    """Simple market monitoring system."""
    
    def __init__(self, config: Config):
        self.config = config
        self.db = Database(config.database_url)
        
        self.market_fetcher = MarketDataFetcher(
            config.node_info_url,
            config.public_info_url,
            config.request_timeout_ms
        )
        
        self.ws_client = OrderBookWebSocketClient(config.orderbook_ws_url)
        self.depth_calculator = OptimizedLiquidityCalculator()
        self.running = True
        
    async def setup(self):
        """Initialize connections."""
        logger.info("Setting up monitor...")
        
        self.config.validate()
        
        if not self.db.connect():
            raise RuntimeError("Database connection failed")
        
        if not await self.ws_client.connect():
            logger.warning("WebSocket connection failed, will retry")
        
        for market in self.config.markets:
            await self.ws_client.subscribe(market, 100)
            logger.info(f"Subscribed to {market}")
        
        logger.info("Setup complete")
    
    async def teardown(self):
        """Clean up connections."""
        await self.ws_client.close()
        self.db.close()
        logger.info("Shutdown complete")
    
    async def process_market(self, market: str) -> Optional[Dict[str, Any]]:
        """Process a single market."""
        try:
            # Get market data from node API
            market_data = await self.market_fetcher.get_meta_and_asset_ctxs(market)
            if not market_data:
                return None
            
            # Get orderbook data from WebSocket
            snapshot = self.ws_client.get_latest_snapshot(market)
            if not snapshot:
                snapshot = await self.ws_client.wait_for_snapshot(market, timeout=2.0)
            
            if not snapshot:
                return None
            
            orderbook_data = self.depth_calculator.analyze_orderbook(snapshot, market)
            if not orderbook_data:
                return None
            
            # Merge data
            metrics = {**market_data, **orderbook_data}
            return metrics
            
        except Exception as e:
            logger.error(f"Error processing {market}: {e}")
            return None
    
    async def run(self):
        """Main monitoring loop."""
        logger.info("Starting monitoring...")
        await self.setup()
        
        try:
            while self.running:
                for market in self.config.markets:
                    metrics = await self.process_market(market)
                    if metrics:
                        success = self.db.insert_metric(metrics)
                        if success:
                            logger.info(f"{market}: ${metrics.get('mark_price', 'N/A')}")
                
                await asyncio.sleep(self.config.monitoring_interval)
                
        except KeyboardInterrupt:
            logger.info("Stopped by user")
        finally:
            await self.teardown()


async def main():
    """Main entry point."""
    config = Config()
    monitor = MarketMonitor(config)
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())
