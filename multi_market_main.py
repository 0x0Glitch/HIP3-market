"""
Multi-Market Monitoring System with Robust Stopping Logic
Supports concurrent monitoring of multiple markets with hot-reloading.
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Set
import traceback

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


class SingleMarketMonitor:
    """Monitor for a single market with proper cleanup."""
    
    def __init__(self, market: str, config: Config, db: Database):
        self.market = market
        self.config = config
        self.db = db
        self.running = True
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
        
        # Ensure table exists
        try:
            success = self.db.ensure_market_table(self.market)
            if not success:
                logger.error(f"Failed to create table for {self.market}")
                return False
        except Exception as e:
            logger.error(f"Error creating table for {self.market}: {e}")
            return False
        
        # Connect WebSocket
        try:
            await self.orderbook_client.connect()
            logger.info(f"✅ WebSocket connected for {self.market}")
        except Exception as e:
            logger.warning(f"WebSocket connection failed for {self.market}: {e}")
            
        return True
        
    async def run(self):
        """Run the monitoring loop for this market."""
        logger.info(f"🎯 Starting monitoring loop for {self.market}")
        
        consecutive_failures = 0
        max_failures = 5
        
        while self.running:
            try:
                # Monitor cycle
                success = await self._monitor_cycle()
                if success:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    
                # If too many failures, add delay
                if consecutive_failures >= max_failures:
                    logger.warning(f"{self.market}: {consecutive_failures} consecutive failures, adding delay")
                    await asyncio.sleep(5)
                    consecutive_failures = 0
                    
                # Sleep with cancellation support
                await self._interruptible_sleep(self.config.MONITORING_INTERVAL)
                
            except asyncio.CancelledError:
                logger.info(f"🛑 {self.market} monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in {self.market} monitoring: {e}")
                await asyncio.sleep(1)  # Brief pause on error
                
    async def _monitor_cycle(self) -> bool:
        """Single monitoring cycle for this market."""
        try:
            # Fetch market data
            market_data = await self.market_fetcher.fetch_market_data(self.market)
            if not market_data:
                logger.warning(f"{self.market}: No market data received")
                return False
                
            # Get orderbook data
            orderbook_data = self.orderbook_client.get_latest_metrics()
            
            # Combine data
            combined_metrics = {
                'coin': self.market,
                **market_data,
                **orderbook_data,
                'timestamp': datetime.now(timezone.utc)
            }
            
            # Calculate liquidity depths if orderbook available
            if orderbook_data.get('orderbook'):
                try:
                    depth_metrics = self.depth_calculator.calculate_all_depths(
                        orderbook_data['orderbook'],
                        market_data.get('mark_price', 0)
                    )
                    combined_metrics.update(depth_metrics)
                except Exception as e:
                    logger.warning(f"{self.market}: Failed to calculate depths: {e}")
                    
            # Insert into database
            success = self.db.insert_market_metrics(combined_metrics)
            if success:
                price = market_data.get('mark_price', 0)
                logger.info(f"📊 {self.market}: ${price:.4f} - Data inserted")
            else:
                logger.error(f"❌ {self.market}: Failed to insert metrics")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"❌ {self.market} cycle error: {e}")
            return False
            
    async def _interruptible_sleep(self, duration: float):
        """Sleep that can be interrupted by cancellation."""
        try:
            await asyncio.sleep(duration)
        except asyncio.CancelledError:
            raise
            
    async def stop(self):
        """Stop monitoring this market."""
        logger.info(f"🛑 Stopping {self.market} monitor")
        self.running = False
        
        try:
            await self.orderbook_client.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting {self.market} WebSocket: {e}")


class MultiMarketMonitor:
    """Manages multiple market monitors with hot-reload support."""
    
    def __init__(self):
        self.config = Config()
        self.db = Database(
            self.config.database_url, 
            min_connections=5, 
            max_connections=20
        )
        self.running = True
        self.shutdown_event = asyncio.Event()
        self.market_tasks: Dict[str, asyncio.Task] = {}
        self.market_monitors: Dict[str, SingleMarketMonitor] = {}
        
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
        try:
            success = self.db.ensure_market_tables(self.config.target_markets)
            if not success:
                logger.error("Failed to create required market tables")
                return False
        except Exception as e:
            logger.error(f"Error creating market tables: {e}")
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
        
        try:
            # Start monitoring tasks for all markets
            await self._start_market_monitors()
            
            # Start hot-reload task
            hot_reload_task = asyncio.create_task(
                self._hot_reload_task(), 
                name="hot_reload"
            )
            
            # Wait for shutdown
            all_tasks = list(self.market_tasks.values()) + [hot_reload_task]
            
            # Wait for either shutdown event or task completion
            done, pending = await asyncio.wait(
                all_tasks + [asyncio.create_task(self.shutdown_event.wait())],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
        except Exception as e:
            logger.error(f"💥 Error in monitoring: {e}")
        finally:
            await self.stop()
            
    async def _start_market_monitors(self):
        """Start monitoring tasks for all configured markets."""
        for market in self.config.target_markets:
            try:
                await self._add_market_monitor(market)
            except Exception as e:
                logger.error(f"Failed to start monitor for {market}: {e}")
                
    async def _add_market_monitor(self, market: str):
        """Add a new market monitor."""
        if market in self.market_monitors:
            logger.warning(f"Monitor for {market} already exists")
            return
            
        logger.info(f"🔄 Adding monitor for {market}")
        
        try:
            # Create monitor
            monitor = SingleMarketMonitor(market, self.config, self.db)
            setup_success = await monitor.setup()
            
            if not setup_success:
                logger.error(f"Failed to setup monitor for {market}")
                return
            
            # Start monitoring task
            task = asyncio.create_task(monitor.run(), name=f"monitor_{market}")
            
            self.market_monitors[market] = monitor
            self.market_tasks[market] = task
            
            logger.info(f"✅ Started monitoring {market}")
            
        except Exception as e:
            logger.error(f"Error adding monitor for {market}: {e}")
            
    async def _remove_market_monitor(self, market: str):
        """Remove a market monitor."""
        if market not in self.market_monitors:
            return
            
        logger.info(f"🗑️ Removing monitor for {market}")
        
        try:
            # Cancel task
            if market in self.market_tasks:
                self.market_tasks[market].cancel()
                try:
                    await self.market_tasks[market]
                except asyncio.CancelledError:
                    pass
                del self.market_tasks[market]
                
            # Stop monitor
            if market in self.market_monitors:
                await self.market_monitors[market].stop()
                del self.market_monitors[market]
                
            logger.info(f"🛑 Stopped monitoring {market}")
            
        except Exception as e:
            logger.error(f"Error removing {market} monitor: {e}")
            
    async def _hot_reload_task(self):
        """Hot-reload task to detect market changes."""
        logger.info("🔥 Hot-reload task started")
        
        while self.running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                if not self.running:
                    break
                
                # Check for market changes
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
                    
            except asyncio.CancelledError:
                logger.info("Hot-reload task cancelled")
                break
            except Exception as e:
                logger.error(f"Hot-reload error: {e}")
                await asyncio.sleep(5)
                
    async def stop(self):
        """Stop all monitoring gracefully."""
        if not self.running:
            return
            
        logger.info("🛑 Stopping Multi-Market Monitor...")
        self.running = False
        self.shutdown_event.set()
        
        # Stop all market monitors
        stop_tasks = []
        for market, monitor in self.market_monitors.items():
            logger.info(f"Stopping {market} monitor...")
            stop_tasks.append(monitor.stop())
            
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
            
        # Cancel all market tasks
        cancel_tasks = []
        for market, task in self.market_tasks.items():
            logger.info(f"Cancelling {market} task...")
            task.cancel()
            cancel_tasks.append(task)
            
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
            
        # Disconnect database
        try:
            self.db.disconnect()
            logger.info("Database disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting database: {e}")
            
        logger.info("✅ Multi-Market Monitor stopped gracefully")
        
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}")
        asyncio.create_task(self.stop())


async def main():
    """Main entry point with robust error handling."""
    monitor = None
    
    try:
        monitor = MultiMarketMonitor()
        
        # Setup signal handlers
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}")
            if monitor:
                asyncio.create_task(monitor.stop())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Setup and start monitoring
        setup_success = await monitor.setup()
        if not setup_success:
            logger.error("Failed to setup monitor")
            return 1
            
        await monitor.start()
        return 0
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"Critical error: {e}")
        logger.error(traceback.format_exc())
        return 1
    finally:
        if monitor:
            try:
                await monitor.stop()
            except Exception as e:
                logger.error(f"Error in final cleanup: {e}")


if __name__ == "__main__":
    try:
        logger.info("=== Multi-Market Monitoring System ===")
        config = Config()
        logger.info(f"Version: 2.0.0")
        logger.info(f"Markets: {', '.join(config.target_markets)}")
        logger.info(f"Monitoring interval: {config.MONITORING_INTERVAL:.1f} seconds")
        logger.info(f"Database: {config.database_url}")
        logger.info("")
        logger.info("Press Ctrl+C to stop gracefully")
        logger.info("System supports hot-reloading - edit .env to add markets")
        logger.info("=" * 60)

        exit_code = asyncio.run(main())
        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.info("\n🛑 Monitoring system stopped by user (Ctrl+C)")
        sys.exit(0)
    except Exception as e:
        logger.error(f"\n💥 Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        logger.info("Multi-Market monitoring system shutdown complete.")
