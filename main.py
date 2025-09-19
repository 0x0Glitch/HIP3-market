"""Multi-market monitoring system with performance optimizations."""

import asyncio
import logging
import signal
import sys
import time
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from config import Config, MarketConfig
from database import Database
from market_data import MarketDataFetcher
from websocket_client import OptimizedWebSocketClient, OptimizedLiquidityCalculator

# Configure logging
def setup_logging(config: Config):
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Reduce verbosity of some loggers
    logging.getLogger('websockets').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class MarketMonitor:
    """High-performance multi-market monitoring system."""
    
    def __init__(self, config: Config):
        self.config = config
        self.db = Database(
            config.database_url,
            config.database_pool_size,
            config.database_max_overflow
        )
        
        self.market_fetcher = MarketDataFetcher(
            config.node_info_url,
            config.public_info_url,
            config.request_timeout_ms
        )
        
        self.ws_client = OptimizedWebSocketClient(
            config.orderbook_ws_url,
            config.ws_reconnect_delay,
            config.ws_max_reconnect_delay
        )
        
        self.depth_calculator = OptimizedLiquidityCalculator()
        
        self.running = True
        self.metrics_buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self.iteration_count = 0
        
        # Single market processing for fresh data every 2s
        self.current_market_index = 0
        self.use_single_market_mode = len(self.config.markets) == 1  # Enable single market mode
        
        # Performance tracking
        self.performance_stats = {
            "total_iterations": 0,
            "successful_iterations": 0,
            "failed_iterations": 0,
            "total_latency_ms": 0,
            "market_stats": {}
        }
        
        # Threading executor for CPU-intensive tasks
        self.executor = ThreadPoolExecutor(
            max_workers=config.max_concurrent_markets,
            thread_name_prefix="market_processor"
        )
        
        # Semaphore to limit concurrent requests
        self.request_semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        
    async def setup(self):
        """Initialize connections and validate configuration."""
        logger.info("Setting up high-performance market monitor...")
        
        self.config.validate()
        
        if not await self.db.connect_async():
            raise RuntimeError("Database connection failed")
        
        # Start WebSocket client
        if not await self.ws_client.start():
            logger.warning("WebSocket client failed to start, will retry during monitoring")
        
        logger.info(f"✓ Database connected with {self.config.database_pool_size} connections")
        logger.info(f"✓ Will monitor {len(self.config.markets)} market(s): {[m.symbol for m in self.config.markets]}")
        logger.info(f"✓ Monitoring interval: {self.config.monitoring_interval}s")
        logger.info(f"✓ Processing mode: Single-Market (LIVE {self.config.markets[0].symbol} data every {self.config.monitoring_interval}s)")
        logger.info(f"✓ Data freshness: Always fetch live data (no caching)")
        logger.info(f"✓ Database insertion: Immediate (no batching for live data)")
        logger.info(f"✓ Max concurrent requests: {self.config.max_concurrent_requests}")
        logger.info(f"✓ Batch insert enabled: {self.config.enable_batch_insert}")
        
    async def collect_market_data(self, market: MarketConfig) -> Optional[Dict[str, Any]]:
        """Collect market data from node API with concurrency control."""
        async with self.request_semaphore:
            try:
                return await self.market_fetcher.fetch_market_data(market.symbol)
            except Exception as e:
                logger.warning(f"Market data error for {market.symbol}: {e}")
                self.performance_stats["market_stats"].setdefault(market.symbol, {})["api_errors"] = \
                    self.performance_stats["market_stats"][market.symbol].get("api_errors", 0) + 1
                return None
    
    async def collect_orderbook_data(self, market: MarketConfig) -> Optional[Dict[str, Any]]:
        """Collect orderbook data from WebSocket with concurrency control."""
        async with self.request_semaphore:
            try:
                ws_start_time = time.time()
                snapshot = await self.ws_client.fetch_l4_snapshot(market.symbol, timeout=1.5, force_fresh=True)  # Always get live data
                ws_latency_ms = (time.time() - ws_start_time) * 1000
                
                if snapshot:
                    # Use executor for CPU-intensive depth calculation
                    loop = asyncio.get_event_loop()
                    orderbook_metrics = await loop.run_in_executor(
                        self.executor,
                        self.depth_calculator.calculate_liquidity_depth,
                        snapshot,
                        market
                    )
                    
                    # Add WebSocket latency to metrics
                    if orderbook_metrics:
                        orderbook_metrics["ws_latency_ms"] = int(ws_latency_ms)
                    
                    return orderbook_metrics
                else:
                    # Log WebSocket health when no data received
                    health = self.ws_client.get_connection_health()
                    if not health["is_healthy"]:
                        logger.debug(f"WebSocket unhealthy for {market.symbol}: {health}")
                
                return None
            except Exception as e:
                logger.debug(f"OrderBook error for {market.symbol}: {e}")
                self.performance_stats["market_stats"].setdefault(market.symbol, {})["ws_errors"] = \
                    self.performance_stats["market_stats"][market.symbol].get("ws_errors", 0) + 1
                return None
    
    async def process_market(self, market: MarketConfig) -> Optional[Dict[str, Any]]:
        """Process a single market with performance tracking."""
        start_time = time.time()
        
        try:
            # Fetch both data sources concurrently
            market_task = asyncio.create_task(self.collect_market_data(market))
            orderbook_task = asyncio.create_task(self.collect_orderbook_data(market))
            
            market_data, orderbook_data = await asyncio.gather(
                market_task, orderbook_task, return_exceptions=True
            )
            
            # Handle exceptions
            if isinstance(market_data, Exception):
                logger.error(f"Market data exception for {market.symbol}: {market_data}")
                market_data = None
            if isinstance(orderbook_data, Exception):
                logger.error(f"OrderBook exception for {market.symbol}: {orderbook_data}")
                orderbook_data = None
            
            if not market_data:
                logger.warning(f"No market data for {market.symbol}, skipping insertion")
                return None
            
            # Merge data
            metrics = {**market_data}
            if orderbook_data:
                for key, value in orderbook_data.items():
                    if value is not None:
                        metrics[key] = value
            
            # Track performance
            processing_time = (time.time() - start_time) * 1000
            self.performance_stats["market_stats"].setdefault(market.symbol, {})["avg_latency_ms"] = processing_time
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error processing {market.symbol}: {e}")
            self.performance_stats["market_stats"].setdefault(market.symbol, {})["processing_errors"] = \
                self.performance_stats["market_stats"][market.symbol].get("processing_errors", 0) + 1
            return None
    
    async def flush_metrics_buffer(self):
        """Flush accumulated metrics to database using batch insert."""
        if not self.metrics_buffer:
            return
            
        try:
            if self.config.enable_batch_insert and len(self.metrics_buffer) > 1:
                success = await self.db.batch_insert_metrics(self.metrics_buffer)
                if success:
                    logger.info(f"✓ Batch inserted {len(self.metrics_buffer)} metrics")
                else:
                    logger.warning(f"Batch insert failed for {len(self.metrics_buffer)} metrics")
            else:
                # Fall back to individual inserts
                for metrics in self.metrics_buffer:
                    await self.db.insert_market_metrics(metrics)
                logger.info(f"✓ Individual inserted {len(self.metrics_buffer)} metrics")
            
            self.metrics_buffer.clear()
            self.last_flush_time = time.time()
            
        except Exception as e:
            logger.error(f"Error flushing metrics: {e}")
            self.metrics_buffer.clear()  # Clear buffer to prevent memory buildup
    
    async def run_monitoring_cycle(self):
        """Run monitoring cycle with fresh LINK data every 2s."""
        cycle_start = time.time()
        self.iteration_count += 1
        
        # Process the single market (LINK)
        market = self.config.markets[0]  # Only LINK
        
        # Process single market
        result = await self.process_market(market)
        
        if isinstance(result, Exception):
            logger.error(f"Exception processing {market.symbol}: {result}")
            self.performance_stats["failed_iterations"] += 1
            valid_results = []
        elif result:
            valid_results = [result]
            # Insert immediately for fresh data - NO BATCHING
            await self.db.insert_market_metrics(result)
            logger.info(f"✓ Live {market.symbol} data inserted (ws_latency: {result.get('ws_latency_ms', 0)}ms)")
        else:
            logger.warning(f"No data received for {market.symbol}")
            valid_results = []
        
        # Update performance stats
        cycle_time = time.time() - cycle_start
        self.performance_stats["total_iterations"] += 1
        if valid_results:
            self.performance_stats["successful_iterations"] += 1
        self.performance_stats["total_latency_ms"] += cycle_time * 1000
        
        # Log performance every N iterations
        if self.iteration_count % 10 == 0:
            avg_latency = self.performance_stats["total_latency_ms"] / max(1, self.performance_stats["total_iterations"])
            success_rate = (self.performance_stats["successful_iterations"] / max(1, self.performance_stats["total_iterations"])) * 100
            
            # Include WebSocket health in performance summary
            ws_health = self.ws_client.get_connection_health()
            health_str = f"WS: {'✓' if ws_health['is_healthy'] else '✗'} (reconnects: {ws_health['total_reconnects']}, last_msg: {ws_health['last_message_age_seconds']:.1f}s ago)"
            
            logger.info(f"Performance [Single-Market]: {self.iteration_count} cycles, avg latency: {avg_latency:.1f}ms, success: {success_rate:.1f}%, {health_str}")
        
        logger.info(f"Cycle {self.iteration_count} completed in {cycle_time:.3f}s - {market.symbol} {'✓' if valid_results else '✗'}")
    
    async def shutdown(self):
        """Graceful shutdown with cleanup."""
        logger.info("Shutting down monitor...")
        self.running = False
        
        # Flush any remaining metrics
        if self.metrics_buffer:
            logger.info(f"Flushing {len(self.metrics_buffer)} remaining metrics...")
            await self.flush_metrics_buffer()
        
        # Cleanup resources
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)
        
        await self.ws_client.close()
        await self.db.disconnect()
        logger.info("✓ Shutdown complete")
    
    async def run(self):
        """Run the monitoring loop with signal handling."""
        await self.setup()
        
        # Setup signal handlers for graceful shutdown
        def signal_handler():
            logger.info("Received shutdown signal")
            self.running = False
        
        if sys.platform != 'win32':
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGTERM, signal.SIGINT]:
                loop.add_signal_handler(sig, signal_handler)
        
        logger.info("🚀 Starting high-performance monitoring loop...")
        
        try:
            while self.running:
                try:
                    await self.run_monitoring_cycle()
                    
                    # Sleep with early exit on shutdown
                    for _ in range(int(self.config.monitoring_interval * 10)):
                        if not self.running:
                            break
                        await asyncio.sleep(0.1)
                
                except KeyboardInterrupt:
                    logger.info("Received keyboard interrupt")
                    break
                except Exception as e:
                    logger.error(f"Monitoring cycle error: {e}")
                    self.performance_stats["failed_iterations"] += 1
                    await asyncio.sleep(min(self.config.monitoring_interval, 5.0))
        
        finally:
            await self.shutdown()


async def main():
    """Main entry point with enhanced error handling."""
    try:
        config = Config.from_env()
        setup_logging(config)
        
        monitor = MarketMonitor(config)
        await monitor.run()
    
    except Exception as e:
        logger.error(f"Failed to start monitor: {e}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        # Try to use uvloop for better performance if available
        import uvloop
        uvloop.install()
        logger.info("Using uvloop for enhanced performance")
    except ImportError:
        logger.info("uvloop not available, using default event loop")
    
    asyncio.run(main())
