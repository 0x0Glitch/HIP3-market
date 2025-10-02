import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional

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

class LinkMarketMonitor:
    def __init__(self):
        self.config = Config()
        self.db = Database(self.config.database_url, min_connections=2, max_connections=10)
        self.market_fetcher = MarketDataFetcher(
            self.config.NODE_INFO_URL,
            self.config.PUBLIC_INFO_URL
        )
        self.orderbook_client = PersistentWebSocketClient(
            self.config.ORDERBOOK_WS_URL,
            self.config.COIN_SYMBOL,
            n_levels=100
        )
        self.depth_calculator = LiquidityDepthCalculator()
        self.running = True
        # Calculate max age for data freshness (half of monitoring interval)
        self.max_data_age = self.config.MONITORING_INTERVAL / 2.0

    async def setup(self):
        """Initialize connections."""
        logger.info("Setting up LINK market monitor...")

        # Connect to database
        try:
            db_success = self.db.connect()
            if not db_success:
                logger.error("Failed to connect to database, will retry during operation")
        except Exception as e:
            logger.error(f"Database connection error: {e}, will retry during operation")

        # Connect persistent WebSocket
        try:
            connected = await self.orderbook_client.connect()
            if connected:
                logger.info("OrderBook WebSocket connected successfully")
            else:
                logger.warning("OrderBook WebSocket connection failed, will retry during operation")
        except Exception as e:
            logger.error(f"Error connecting to OrderBook WebSocket: {e}")

        logger.info("Setup complete")

    async def teardown(self):
        """Clean up connections."""
        logger.info("Shutting down LINK market monitor...")

        # Close persistent WebSocket connection
        if self.orderbook_client:
            try:
                await self.orderbook_client.close()
            except Exception as e:
                logger.error(f"Error closing orderbook client: {e}")

        try:
            self.db.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting database: {e}")

        logger.info("Shutdown complete")

    async def collect_market_data(self) -> Optional[Dict[str, Any]]:
        """Collect market data from node /info endpoint."""
        try:
            market_data = await self.market_fetcher.get_meta_and_asset_ctxs(
                self.config.COIN_SYMBOL
            )

            if not market_data:
                logger.error("Failed to fetch market data")
                return None

            logger.debug(f"Market data: {market_data}")
            return market_data

        except Exception as e:
            logger.error(f"Error collecting market data: {e}")
            return None

    async def collect_orderbook_data(self) -> Optional[Dict[str, Any]]:
        """Collect orderbook data and calculate liquidity depth."""
        try:
            # Ensure connection is active
            if not await self.orderbook_client.ensure_connected():
                logger.warning("Failed to ensure WebSocket connection")
                return None
            
            # Get fresh snapshot with data freshness requirement
            # Use max_age to ensure data is fresh (half of monitoring interval)
            snapshot = self.orderbook_client.get_snapshot(max_age_seconds=self.max_data_age)
            
            # If snapshot is too old, try to get a fresh one
            if not snapshot:
                logger.info("Requesting fresh orderbook snapshot...")
                snapshot = await self.orderbook_client.get_fresh_snapshot(timeout=min(5.0, self.max_data_age))

            if not snapshot:
                logger.warning("No fresh orderbook snapshot available")
                return None

            # Analyze orderbook
            orderbook_metrics = self.depth_calculator.analyze_orderbook(snapshot, self.config.COIN_SYMBOL)

            # Add connection stats and freshness validation
            stats = self.orderbook_client.get_stats()
            snapshot_age_seconds = stats.get('last_snapshot_age')
            
            if snapshot_age_seconds is not None:
                orderbook_metrics['snapshot_age_ms'] = int(snapshot_age_seconds * 1000)
                
                # Log data freshness for monitoring
                if snapshot_age_seconds <= self.max_data_age:
                    logger.info(f"✅ Using FRESH orderbook data (age: {snapshot_age_seconds:.2f}s, max: {self.max_data_age:.1f}s)")
                else:
                    logger.warning(f"⚠️  Using STALE orderbook data (age: {snapshot_age_seconds:.2f}s, max: {self.max_data_age:.1f}s)")
            else:
                logger.warning("⚠️  No snapshot age information available")
            
            logger.debug(f"Orderbook metrics: {orderbook_metrics}")
            return orderbook_metrics

        except Exception as e:
            logger.error(f"Error collecting orderbook data: {e}, will attempt reconnection")
            # Try to reconnect for next iteration
            asyncio.create_task(self.orderbook_client.connect())
            return None

    async def monitor_iteration(self):
        """Single monitoring iteration."""
        try:
            logger.info(f"Starting monitoring iteration at {datetime.now(timezone.utc).isoformat()}")

            # Collect market data
            market_data = await self.collect_market_data()
            if not market_data:
                logger.warning("Skipping iteration due to missing market data")
                return

            # Collect orderbook data
            orderbook_data = await self.collect_orderbook_data()

            # Merge data
            metrics = {**market_data}
            if orderbook_data:
                # Only override mid_price from orderbook if it exists and market data doesn't have it
                if orderbook_data.get('mid_price') and not metrics.get('mid_price'):
                    metrics['mid_price'] = orderbook_data['mid_price']

                # Always use orderbook data for spread and depth metrics
                for key in ['best_bid', 'best_ask', 'spread', 'spread_pct', 'mid_price',
                           'bid_count', 'ask_count', 'total_bids', 'total_asks', 'orderbook_levels']:
                    if key in orderbook_data:
                        metrics[key] = orderbook_data[key]

                # Map depth metrics with correct database column names
                if 'depth_5_pct' in orderbook_data:
                    metrics['total_depth_5pct'] = orderbook_data['depth_5_pct']
                    # Only store individual bid/ask depth if they exist (no default zeros)
                    if 'bid_depth_5pct' in orderbook_data:
                        metrics['bid_depth_5pct'] = orderbook_data['bid_depth_5pct']
                    if 'ask_depth_5pct' in orderbook_data:
                        metrics['ask_depth_5pct'] = orderbook_data['ask_depth_5pct']

                if 'depth_10_pct' in orderbook_data:
                    metrics['total_depth_10pct'] = orderbook_data['depth_10_pct']
                    if 'bid_depth_10pct' in orderbook_data:
                        metrics['bid_depth_10pct'] = orderbook_data['bid_depth_10pct']
                    if 'ask_depth_10pct' in orderbook_data:
                        metrics['ask_depth_10pct'] = orderbook_data['ask_depth_10pct']

                if 'depth_25_pct' in orderbook_data:
                    metrics['total_depth_25pct'] = orderbook_data['depth_25_pct']
                    if 'bid_depth_25pct' in orderbook_data:
                        metrics['bid_depth_25pct'] = orderbook_data['bid_depth_25pct']
                    if 'ask_depth_25pct' in orderbook_data:
                        metrics['ask_depth_25pct'] = orderbook_data['ask_depth_25pct']
            else:
                logger.warning("No orderbook data available for this iteration")

            # Log summary with proper formatting
            def format_value(val, prefix='', suffix='', decimals=2):
                if val is None or val == 'N/A':
                    return 'N/A'
                try:
                    if isinstance(val, (int, float)):
                        if decimals is not None:
                            return f"{prefix}{val:,.{decimals}f}{suffix}"
                        else:
                            return f"{prefix}{val:,}{suffix}"
                    return f"{prefix}{val}{suffix}"
                except:
                    return 'N/A'

            logger.info(
                f"LINK Market Summary:\n"
                f"  Mark Price: {format_value(metrics.get('mark_price'), '$')}\n"
                f"  Oracle Price: {format_value(metrics.get('oracle_price'), '$')}\n"
                f"  Mid Price: {format_value(metrics.get('mid_price'), '$')}\n"
                f"  Spread: {format_value(metrics.get('spread'), '$')} ({format_value(metrics.get('spread_pct'), suffix='%')})\n"
                f"  Funding Rate: {format_value(metrics.get('funding_rate_pct'), decimals=4, suffix='%')}\n"
                f"  Open Interest: {format_value(metrics.get('open_interest'), decimals=0)}\n"
                f"  24h Volume: {format_value(metrics.get('volume_24h'), '$', decimals=0)}\n"
                f"  5% Depth: {format_value(metrics.get('total_depth_5pct'), '$', decimals=0)}\n"
                f"  10% Depth: {format_value(metrics.get('total_depth_10pct'), '$', decimals=0)}\n"
                f"  25% Depth: {format_value(metrics.get('total_depth_25pct'), '$', decimals=0)}"
            )


            if not self._validate_metrics(metrics):
                logger.warning("Skipping database insertion - metrics validation failed, waiting for complete data")
                return

            # Store in database
            try:
                success = self.db.insert_market_metrics(metrics)
                if not success:
                    logger.warning("Failed to insert metrics to database, will retry next iteration")
            except Exception as e:
                logger.error(f"Database insert error: {e}, will retry next iteration")

        except Exception as e:
            logger.error(f"Error in monitoring iteration: {e}")
            logger.info("Continuing to next iteration despite error...")

    def _validate_metrics(self, metrics: Dict[str, Any]) -> bool:
        """
        Validate that essential metrics are not None/null before database insertion.
        Returns True if metrics are valid for database insertion, False otherwise.
        """
        # Required fields that must not be None
        required_fields = [
            'coin',           # Always required
            'mid_price',      # Essential for calculations
            'best_bid',       # Core orderbook data
            'best_ask',       # Core orderbook data
        ]
        
        # Check required fields
        for field in required_fields:
            value = metrics.get(field)
            if value is None or (isinstance(value, str) and value.strip() == ''):
                logger.warning(f"Required field '{field}' is None/empty, skipping database insertion")
                return False
        
        # Important depth fields - if any orderbook data is present, these should be valid
        critical_depth_fields = [
            'total_depth_5pct'  # At minimum, we need 5% depth
        ]
        
        # Check if we have orderbook data but critical depth fields are missing
        has_orderbook_data = any([
            metrics.get('best_bid') is not None,
            metrics.get('best_ask') is not None,
            metrics.get('orderbook_levels', 0) > 0
        ])
        
        if has_orderbook_data:
            # Check critical depth fields
            missing_critical_fields = [field for field in critical_depth_fields if metrics.get(field) is None]
            if missing_critical_fields:
                logger.warning(f"Orderbook data present but critical depth fields missing: {missing_critical_fields}")
                return False
            
            # Also check that depth values are not zero (which indicates calculation failure)
            for field in critical_depth_fields:
                value = metrics.get(field)
                if value is not None and value == 0:
                    logger.warning(f"Depth field '{field}' is zero - indicates calculation failure")
                    return False
        
        # Validate numeric fields are actually numeric
        numeric_fields = [
            'mark_price', 'oracle_price', 'mid_price', 'best_bid', 'best_ask',
            'spread', 'spread_pct', 'funding_rate_pct', 'open_interest', 'volume_24h',
            'total_depth_5pct', 'total_depth_10pct', 'total_depth_25pct'
        ]
        
        for field in numeric_fields:
            value = metrics.get(field)
            if value is not None:
                try:
                    float(value)
                except (ValueError, TypeError):
                    logger.warning(f"Field '{field}' has invalid numeric value: {value}")
                    return False
        
        logger.debug("Metrics validation passed - all essential fields present and valid")
        return True

    async def run(self):
        """Main monitoring loop."""
        logger.info("Initializing monitoring system...")

        try:
            await self.setup()
        except Exception as e:
            logger.error(f"Setup error: {e}")
            logger.info("Continuing with partial setup, will retry connections during operation...")

        iteration_count = 0
        consecutive_failures = 0
        max_consecutive_failures = 10

        try:
            while self.running:
                iteration_count += 1
                logger.info(f"\n=== Monitoring Iteration #{iteration_count} ===")

                try:
                    await self.monitor_iteration()
                    consecutive_failures = 0  # Reset failure count on success

                except Exception as e:
                    consecutive_failures += 1
                    logger.error(f"Iteration #{iteration_count} failed: {e}")

                    if consecutive_failures >= max_consecutive_failures:
                        logger.warning(
                            f"Had {consecutive_failures} consecutive failures. "
                            f"Sleeping for extra 30 seconds before continuing..."
                        )
                        await asyncio.sleep(30)
                        consecutive_failures = 0  # Reset to prevent long delays

                if self.running:  # Check if still running before sleeping
                    logger.info(f"Waiting {self.config.MONITORING_INTERVAL:.1f} seconds until next iteration...")

                    # Sleep in smaller chunks to allow for graceful shutdown
                    sleep_duration = self.config.MONITORING_INTERVAL
                    sleep_increment = 0.1  # Check every 100ms for shutdown
                    elapsed = 0.0
                    
                    while elapsed < sleep_duration and self.running:
                        await asyncio.sleep(min(sleep_increment, sleep_duration - elapsed))
                        elapsed += sleep_increment

        except KeyboardInterrupt:
            logger.info("Received interrupt signal (Ctrl+C)")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            logger.info("This should not terminate the program automatically")
        finally:
            logger.info("Cleaning up and shutting down...")
            try:
                await self.teardown()
            except Exception as e:
                logger.error(f"Error during teardown: {e}")

    def stop(self):
        """Stop the monitoring loop."""
        self.running = False


async def main():
    """Main entry point."""
    monitor = None

    try:
        monitor = LinkMarketMonitor()

        # Setup signal handlers
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}")
            if monitor:
                monitor.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Run monitor with error recovery
        await monitor.run()

    except Exception as e:
        logger.error(f"Critical error in main(): {e}")
        if monitor:
            try:
                await monitor.teardown()
            except:
                pass
        raise


if __name__ == "__main__":
    try:
        logger.info("=== LINK Perpetual Market Monitoring System ===")
        logger.info(f"Version: 1.1.0")
        logger.info(f"Monitoring interval: {Config().MONITORING_INTERVAL:.1f} seconds")
        logger.info(f"Target coin: {Config().COIN_SYMBOL}")
        logger.info(f"Node endpoint: {Config().NODE_INFO_URL}")
        logger.info(f"OrderBook WebSocket: {Config().ORDERBOOK_WS_URL}")
        logger.info(f"Database: {Config().database_url}")
        logger.info("")
        logger.info("Press Ctrl+C to stop the monitoring system")
        logger.info("System will handle errors gracefully and continue running")
        logger.info("========================================================")

        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("\nMonitoring system stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"\nFatal error: {e}")
        logger.error("The system should not exit automatically. This may be a critical bug.")
        sys.exit(1)
    finally:
        logger.info("LINK market monitoring system has shut down.")
