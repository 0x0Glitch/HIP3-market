"""
Multi-Market Monitor for Hyperliquid with Dynamic Hot-Reloading
Concurrent monitoring of multiple markets with automatic table creation.
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Set
import concurrent.futures

from config import Config
from database import Database
from market_data import MarketDataFetcher
from websocket_persistent import PersistentWebSocketClient, LiquidityDepthCalculator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MarketMonitorManager:
    
    def __init__(self):
        self.config = Config()
        self.db = Database(self.config.database_url, min_connections=5, max_connections=20)
        self.running = True
        self.market_tasks: Dict[str, asyncio.Task] = {}
        self.market_monitors: Dict[str, 'SingleMarketMonitor'] = {}
        
    async def setup(self):
        """Initialize the multi-market monitor."""
        logger.info("🚀 Setting up Multi-Market Monitor...")
        logger.info(f"Target Markets: {', '.join(self.config.target_markets)}")
        
        # Connect to database
        try:
            db_success = self.db.connect()
            if not db_success:
                logger.error("Failed to connect to database")
                return False
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False
            
        # Ensure tables exist for all target markets
        success = self.db.ensure_market_tables(self.config.target_markets)
        if not success:
            logger.error("Failed to create required market tables")
            return False
            
        logger.info("✅ Multi-Market Monitor setup completed")
        return True
        
    async def start(self):
        """Start monitoring all configured markets."""
        logger.info("=" * 80)
        logger.info("🎯 STARTING MULTI-MARKET MONITORING")
        logger.info(f"Markets: {', '.join(self.config.target_markets)}")
        logger.info(f"Monitoring Interval: {self.config.MONITORING_INTERVAL}s")
        logger.info("=" * 80)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            # Start monitoring tasks for all markets
            await self._start_market_monitors()
            
            # Start hot-reload task
            hot_reload_task = asyncio.create_task(self._hot_reload_task())
            
            # Wait for all tasks
            all_tasks = list(self.market_tasks.values()) + [hot_reload_task]
            await asyncio.gather(*all_tasks, return_exceptions=True)
            
        except KeyboardInterrupt:
            logger.info("🛑 Keyboard interrupt received")
        except Exception as e:
            logger.critical(f"💥 Fatal error in monitor manager: {e}", exc_info=True)
        finally:
            await self.stop()
            
    async def _start_market_monitors(self):
        """Start monitoring tasks for all configured markets."""
        for market in self.config.target_markets:
            await self._add_market_monitor(market)
            
    async def _add_market_monitor(self, market: str):
        """Add a new market monitor."""
        if market in self.market_monitors:
            logger.warning(f"Monitor for {market} already exists")
            return
            
        logger.info(f"🔄 Adding monitor for {market}")
        
        # Ensure table exists
        self.db.ensure_market_table(market)
        
        # Create monitor
        monitor = SingleMarketMonitor(market, self.config, self.db)
        await monitor.setup()
        
        # Start monitoring task
        task = asyncio.create_task(monitor.run())
        
        self.market_monitors[market] = monitor
        self.market_tasks[market] = task
        
        logger.info(f"✅ Started monitoring {market}")
        
    async def _remove_market_monitor(self, market: str):
        """Remove a market monitor."""
        if market not in self.market_monitors:
            return
            
        logger.info(f"🗑️ Removing monitor for {market}")
        
        # Cancel task
        if market in self.market_tasks:
            self.market_tasks[market].cancel()
            del self.market_tasks[market]
            
        # Stop monitor
        if market in self.market_monitors:
            await self.market_monitors[market].stop()
            del self.market_monitors[market]
            
        logger.info(f"🛑 Stopped monitoring {market}")
        
    async def _hot_reload_task(self):
        """Hot-reload task to detect new markets."""
        logger.info("🔥 Hot-reload task started")
        
        while self.running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                # Check for new markets
                added_markets = self.config.reload_markets()
                current_markets = set(self.config.target_markets)
                existing_markets = set(self.market_monitors.keys())
                
                # Add new markets
                for market in added_markets:
                    await self._add_market_monitor(market)
                    
                # Remove markets that are no longer configured
                removed_markets = existing_markets - current_markets
                for market in removed_markets:
                    await self._remove_market_monitor(market)
                    
            except Exception as e:
                logger.error(f"Hot-reload error: {e}")
                await asyncio.sleep(5)
                
    async def stop(self):
        """Stop all monitoring."""
        logger.info("🛑 Stopping Multi-Market Monitor...")
        self.running = False
        
        # Cancel all market tasks
        for market, task in self.market_tasks.items():
            logger.info(f"Cancelling {market} task...")
            task.cancel()
            
        # Stop all monitors
        for market, monitor in self.market_monitors.items():
            logger.info(f"Stopping {market} monitor...")
            try:
                await monitor.stop()
            except Exception as e:
                logger.error(f"Error stopping {market} monitor: {e}")
                
        # Disconnect database
        self.db.disconnect()
        logger.info("✅ Multi-Market Monitor stopped")
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}")
        self.running = False


class SingleMarketMonitor:
    """Monitor for a single market."""
    
    def __init__(self, market: str, config: Config, db: Database):
        self.market = market
        self.config = config
        self.db = db
        self.running = True
        
        # Create market-specific components
        self.market_fetcher = MarketDataFetcher(
            config.NODE_INFO_URL,
            config.PUBLIC_INFO_URL
        )
        self.orderbook_client = PersistentWebSocketClient(
            config.ORDERBOOK_WS_URL,
            market,
            n_levels=100
        )
        self.depth_calculator = LiquidityDepthCalculator()
        
        self.max_data_age = config.MONITORING_INTERVAL / 2.0
        
    async def setup(self):
        """Setup the single market monitor."""
        logger.info(f"🔧 Setting up monitor for {self.market}")
        
        # Connect persistent WebSocket
        try:
            await self.orderbook_client.connect()
            logger.info(f"✅ WebSocket connected for {self.market}")
        except Exception as e:
            logger.warning(f"WebSocket connection failed for {self.market}: {e}")
            
    async def run(self):
        """Run the monitoring loop for this market."""
        logger.info(f"🎯 Starting monitoring loop for {self.market}")
        
        while self.running:
            try:
                await self._monitor_cycle()
                await asyncio.sleep(self.config.MONITORING_INTERVAL)
            except asyncio.CancelledError:
                logger.info(f"🛑 {self.market} monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in {self.market} monitoring cycle: {e}")
                await asyncio.sleep(5)  # Brief pause before retry
                
    async def _monitor_cycle(self):
        """Single monitoring cycle for this market."""
        try:
            # Fetch market data
            market_data = await self.market_fetcher.fetch_market_data(self.market)
            if not market_data:
                logger.warning(f"No market data for {self.market}")
                return
                
            # Get orderbook data
            orderbook_data = self.orderbook_client.get_latest_metrics()
            
            # Combine data
            combined_metrics = {
                'coin': self.market,
                **market_data,
                **orderbook_data,
                'timestamp': datetime.now(timezone.utc)
            }
            
            # Calculate liquidity depths
            if orderbook_data.get('orderbook'):
                depth_metrics = self.depth_calculator.calculate_all_depths(
                    orderbook_data['orderbook'],
                    market_data.get('mark_price', 0)
                )
                combined_metrics.update(depth_metrics)
                
            # Insert into database
            success = self.db.insert_market_metrics(combined_metrics)
            if success:
                logger.info(f"📊 {self.market}: Inserted metrics (price: ${market_data.get('mark_price', 0):.4f})")
            else:
                logger.error(f"❌ {self.market}: Failed to insert metrics")
                
        except Exception as e:
            logger.error(f"❌ {self.market} monitor cycle error: {e}")
            
    async def stop(self):
        """Stop monitoring this market."""
        logger.info(f"🛑 Stopping {self.market} monitor")
        self.running = False
        
        try:
            await self.orderbook_client.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting {self.market} WebSocket: {e}")


async def main():
    """Main entry point."""
    monitor_manager = MarketMonitorManager()
    
    # Setup
    setup_success = await monitor_manager.setup()
    if not setup_success:
        logger.error("Failed to setup monitor manager")
        sys.exit(1)
        
    # Start monitoring
    await monitor_manager.start()


if __name__ == "__main__":
    asyncio.run(main())
