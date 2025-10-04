"""
Multi-Market Monitoring System with WebSocket Client
"""
import asyncio
import logging
import signal
import sys
import time
import aiohttp
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from config import Config
from database import Database
from market_data import PublicRestClient
from websocket_client import WebSocketClient, LiquidityDepthCalculator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SingleMarketMonitor:
    """Monitor for a single market."""

    def __init__(self, market: str, config: Config, db: Database, parent_monitor):
        self.market = market
        self.config = config
        self.db = db
        self.parent_monitor = parent_monitor  # Reference to MultiMarketMonitor for shared data
        self.running = True
        self.orderbook_client = WebSocketClient(
            config.ORDERBOOK_WS_URL,
            market,
            n_levels=100
        )
        self.depth_calculator = LiquidityDepthCalculator()
        self.max_data_age = config.MONITORING_INTERVAL / 2.0

    async def setup(self):
        logger.info(f"🔧 Setting up monitor for {self.market}")

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
        try:
            # Step 1: Get market data from shared data (no individual REST call)
            market_data = self.parent_monitor.get_market_data_from_shared(self.market)
            
            if not market_data:
                logger.warning(f"{self.market}: No market data from shared cache")
                return False

            # Step 2: Force fresh orderbook data by resubscribing (proven approach)
            orderbook_data = await self.orderbook_client.get_fresh_metrics()
            
            if not orderbook_data or orderbook_data.get("orderbook") is None:
                logger.warning(f"{self.market}: Failed to get fresh orderbook snapshot")
                return False

            # Step 3: Combine data
            combined_metrics = {
                'coin': self.market,
                **market_data,
                **orderbook_data,
                'timestamp': datetime.now(timezone.utc)
            }

            # Step 4: Store in database
            success = self.db.insert_market_metrics(combined_metrics)
            if success:
                price = market_data.get('mark_price', 0)
                logger.info(f"📊 {self.market}: ${price:.4f} - SHARED_DATA+FRESH_L4 ✅")
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
            logger.info(f"Sleep for {self.market} interrupted")
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

        # Shared REST API client for all markets
        self.public_rest_client = PublicRestClient(timeout_s=5.0)
        
        # Shared market data - updated once per cycle for all markets
        self.shared_market_data = None
        self.shared_data_lock = asyncio.Lock()

    async def setup(self):
        """Initialize the multi-market monitor."""
        logger.info(f"Target Markets: {', '.join(self.config.target_markets)}")

        # Connect to public REST API (shared for all markets)
        try:
            api_success = await self.public_rest_client.connect()
            if not api_success:
                logger.error("Failed to connect to Hyperliquid REST API")
                return False
            logger.info("✅ Hyperliquid REST API connected")
        except Exception as e:
            logger.error(f"REST API connection error: {e}")
            return False

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

    async def fetch_shared_market_data(self) -> bool:
        """Fetch metaAndAssetCtxs once for all markets to avoid 429 rate limiting."""
        try:
            # Make one REST call for all markets
            payload = {"type": "metaAndAssetCtxs"}
            
            async with self.public_rest_client.session.post(
                self.public_rest_client.api_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=self.public_rest_client.timeout_s)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    async with self.shared_data_lock:
                        self.shared_market_data = data
                    return True
                else:
                    logger.error(f"REST API error: {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error fetching shared market data: {e}")
            return False

    def get_market_data_from_shared(self, coin: str) -> Optional[Dict[str, Any]]:
        """Extract market data for specific coin from shared data."""
        if not self.shared_market_data:
            return None
        return self.public_rest_client._extract_coin_data(self.shared_market_data, coin)

    async def start(self):
        """Start monitoring all configured markets."""
        logger.info(f"Markets: {', '.join(self.config.target_markets)}")

        try:
            # Start monitoring tasks for all markets
            await self._start_market_monitors()
            
            # Start shared data fetching loop
            asyncio.create_task(self._shared_data_loop())

            # Wait for shutdown
            await self.shutdown_event.wait()

        except Exception as e:
            logger.error(f"Error in monitoring: {e}")
        finally:
            await self.stop()

    async def _shared_data_loop(self):
        """Fetch shared market data every monitoring interval."""
        while self.running:
            try:
                success = await self.fetch_shared_market_data()
                if success:
                    logger.debug("✅ Shared market data updated")
                else:
                    logger.warning("⚠️ Failed to fetch shared market data")
                
                # Wait for monitoring interval before next fetch
                await asyncio.sleep(self.config.MONITORING_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in shared data loop: {e}")
                await asyncio.sleep(1)

    async def _start_market_monitors(self):
        for market in self.config.target_markets:
            try:
                await self._add_market_monitor(market)
            except Exception as e:
                logger.error(f"Failed to start monitor for {market}: {e}")

    async def _add_market_monitor(self, market: str):
        if market in self.market_monitors:
            logger.warning(f"Monitor for {market} already exists")
            return

        try:
            monitor = SingleMarketMonitor(market, self.config, self.db, self)
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

        # Disconnect public REST API
        try:
            await self.public_rest_client.disconnect()
            logger.info("Hyperliquid REST API disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting REST API: {e}")

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
        return 1
    finally:
        if monitor:
            try:
                await monitor.stop()
            except Exception as e:
                logger.error(f"Error in cleanup: {e}")


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.info("Stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        logger.info("Multi-Market monitoring system shutdown complete.")