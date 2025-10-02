"""
Enhanced Multi-Market Monitor with Real-time WebSocket Data
Uses concurrent long-lived WebSocket connections for real-time order book data.
Implements the industry-standard architecture with proper atomic operations.
"""
import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Set
import json

from config import Config
from database import Database
from market_data import MarketDataFetcher
from enhanced_websocket_client import ConcurrentWebSocketManager, OrderBookSnapshot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LiquidityDepthCalculator:
    """Calculate liquidity depths from order book data."""
    
    @staticmethod
    def calculate_depth_metrics(orderbook: OrderBookSnapshot, mark_price: float) -> Dict[str, Any]:
        """Calculate liquidity depth metrics at 5%, 10%, and 25% levels."""
        if not orderbook or not orderbook.bids or not orderbook.asks:
            return {}
        
        try:
            # Calculate spreads
            best_bid = orderbook.bids[0][0] if orderbook.bids else 0
            best_ask = orderbook.asks[0][0] if orderbook.asks else 0
            spread = best_ask - best_bid if best_bid and best_ask else 0
            spread_pct = (spread / mark_price * 100) if mark_price > 0 else 0
            
            # Calculate depths at different percentage levels
            depth_levels = [0.05, 0.10, 0.25]  # 5%, 10%, 25%
            depth_metrics = {
                'best_bid': best_bid,
                'best_ask': best_ask,
                'spread': spread,
                'spread_pct': spread_pct,
                'mid_price': (best_bid + best_ask) / 2 if best_bid and best_ask else mark_price
            }
            
            for level_pct in depth_levels:
                level_name = f"{int(level_pct * 100)}pct"
                
                # Calculate bid depth
                bid_threshold = mark_price * (1 - level_pct)
                bid_depth = sum(
                    price * size for price, size in orderbook.bids 
                    if price >= bid_threshold
                )
                
                # Calculate ask depth  
                ask_threshold = mark_price * (1 + level_pct)
                ask_depth = sum(
                    price * size for price, size in orderbook.asks
                    if price <= ask_threshold
                )
                
                depth_metrics.update({
                    f'bid_depth_{level_name}': bid_depth,
                    f'ask_depth_{level_name}': ask_depth,
                    f'total_depth_{level_name}': bid_depth + ask_depth
                })
                
            return depth_metrics
            
        except Exception as e:
            logger.error(f"Error calculating depth metrics: {e}")
            return {}

class EnhancedMarketMonitor:
    """Enhanced market monitor with real-time WebSocket data."""
    
    def __init__(self, market: str, config: Config, db: Database, ws_manager: ConcurrentWebSocketManager):
        self.market = market
        self.config = config
        self.db = db
        self.ws_manager = ws_manager
        self.running = True
        
        # Create market data fetcher
        self.market_fetcher = MarketDataFetcher(
            config.NODE_INFO_URL,
            config.PUBLIC_INFO_URL
        )
        
        # Create liquidity calculator
        self.depth_calculator = LiquidityDepthCalculator()
        
        # Performance tracking
        self.last_data_time = datetime.now(timezone.utc)
        self.metrics_inserted = 0
        
    async def run(self):
        """Run the monitoring loop for this market."""
        logger.info(f"🎯 Starting enhanced monitoring for {self.market}")
        
        while self.running:
            try:
                await self._enhanced_monitor_cycle()
                await asyncio.sleep(self.config.MONITORING_INTERVAL)
            except asyncio.CancelledError:
                logger.info(f"🛑 {self.market} monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"❌ {self.market} monitoring error: {e}")
                await asyncio.sleep(5)  # Brief pause before retry
                
    async def _enhanced_monitor_cycle(self):
        """Enhanced monitoring cycle with real-time orderbook data."""
        try:
            cycle_start = datetime.now(timezone.utc)
            
            # Fetch market data (prices, funding, etc.)
            market_data = await self.market_fetcher.fetch_market_data(self.market)
            if not market_data:
                logger.warning(f"⚠️ {self.market}: No market data received")
                return
            
            # Get real-time orderbook from WebSocket
            orderbook = self.ws_manager.get_orderbook(self.market)
            
            # Calculate latencies
            ws_latency = None
            if orderbook and orderbook.timestamp:
                ws_latency = int((cycle_start - orderbook.timestamp).total_seconds() * 1000)
            
            node_latency = market_data.get('node_latency_ms', 0)
            total_latency = (node_latency or 0) + (ws_latency or 0)
            
            # Combine all data
            combined_metrics = {
                'coin': self.market,
                'timestamp': cycle_start,
                **market_data,
                'websocket_latency_ms': ws_latency,
                'total_latency_ms': total_latency
            }
            
            # Add orderbook-derived metrics if available
            if orderbook and market_data.get('mark_price'):
                depth_metrics = self.depth_calculator.calculate_depth_metrics(
                    orderbook, 
                    market_data['mark_price']
                )
                combined_metrics.update(depth_metrics)
                
                # Log orderbook info
                logger.debug(f"📊 {self.market}: Book - {len(orderbook.bids)} bids, "
                           f"{len(orderbook.asks)} asks, spread: ${depth_metrics.get('spread', 0):.4f}")
            else:
                logger.debug(f"⚠️ {self.market}: No real-time orderbook data")
            
            # Insert into database using atomic operation per market
            success = await self._atomic_insert(combined_metrics)
            
            if success:
                self.metrics_inserted += 1
                self.last_data_time = cycle_start
                
                # Enhanced logging with performance metrics
                mark_price = market_data.get('mark_price', 0)
                total_depth = combined_metrics.get('total_depth_5pct', 0)
                
                logger.info(
                    f"📊 {self.market}: ${mark_price:.4f} | "
                    f"Depth: ${total_depth:,.0f} | "
                    f"Latency: {total_latency}ms | "
                    f"#{self.metrics_inserted}"
                )
            else:
                logger.error(f"❌ {self.market}: Failed to insert metrics")
                
        except Exception as e:
            logger.error(f"❌ {self.market} enhanced cycle error: {e}")
    
    async def _atomic_insert(self, metrics: Dict[str, Any]) -> bool:
        """
        Atomic database insert following industry-standard practices.
        Each market uses its own table with atomic operations.
        """
        try:
            # Use synchronous insert (the Database class handles connection pooling)
            success = self.db.insert_market_metrics(metrics)
            return success
        except Exception as e:
            logger.error(f"❌ {self.market} atomic insert error: {e}")
            return False
    
    async def stop(self):
        """Stop monitoring this market."""
        logger.info(f"🛑 Stopping enhanced monitor for {self.market}")
        self.running = False

class EnhancedMultiMarketManager:
    """
    Enhanced multi-market manager with real-time WebSocket integration.
    Implements concurrent processing with long-lived connections.
    """
    
    def __init__(self):
        self.config = Config()
        self.db = Database(self.config.database_url, min_connections=10, max_connections=30)
        self.running = True
        
        # WebSocket manager for real-time data
        self.ws_manager = ConcurrentWebSocketManager(
            self.config.ORDERBOOK_WS_URL,
            self.config.target_markets,
            self._handle_websocket_message
        )
        
        # Market monitors
        self.market_monitors: Dict[str, EnhancedMarketMonitor] = {}
        self.monitor_tasks: Dict[str, asyncio.Task] = {}
        
        # Performance tracking
        self.start_time = None
        self.total_messages_processed = 0
        
    async def _handle_websocket_message(self, market: str, message: Dict[str, Any]):
        """Handle incoming WebSocket messages."""
        try:
            self.total_messages_processed += 1
            
            # Log every 100th message to avoid spam
            if self.total_messages_processed % 100 == 0:
                logger.debug(f"📡 Processed {self.total_messages_processed} WebSocket messages")
                
        except Exception as e:
            logger.error(f"❌ WebSocket message handling error: {e}")
    
    async def setup(self):
        """Setup the enhanced multi-market manager."""
        logger.info("🚀 Setting up Enhanced Multi-Market Manager...")
        logger.info(f"Target Markets: {', '.join(self.config.target_markets)}")
        
        # Connect to database
        try:
            db_success = self.db.connect()
            if not db_success:
                logger.error("❌ Database connection failed")
                return False
        except Exception as e:
            logger.error(f"❌ Database connection error: {e}")
            return False
        
        # Ensure tables exist for all markets
        success = self.db.ensure_market_tables(self.config.target_markets)
        if not success:
            logger.error("❌ Failed to create required market tables")
            return False
        
        logger.info("✅ Enhanced Multi-Market Manager setup completed")
        return True
    
    async def start(self):
        """Start enhanced multi-market monitoring."""
        self.start_time = datetime.now(timezone.utc)
        
        logger.info("=" * 80)
        logger.info("🚀 STARTING ENHANCED MULTI-MARKET MONITORING")
        logger.info(f"Markets: {', '.join(self.config.target_markets)}")
        logger.info(f"WebSocket URL: {self.config.ORDERBOOK_WS_URL}")
        logger.info(f"Monitoring Interval: {self.config.MONITORING_INTERVAL}s")
        logger.info(f"Real-time: ✅ Long-lived WebSocket connections")
        logger.info(f"Concurrent: ✅ One connection per market")
        logger.info("=" * 80)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            # Start WebSocket manager
            await self.ws_manager.start()
            
            # Start market monitors
            await self._start_market_monitors()
            
            # Start hot-reload task
            hot_reload_task = asyncio.create_task(self._hot_reload_task())
            
            # Start stats reporter
            stats_task = asyncio.create_task(self._stats_reporter_task())
            
            # Wait for all tasks
            all_tasks = list(self.monitor_tasks.values()) + [hot_reload_task, stats_task]
            await asyncio.gather(*all_tasks, return_exceptions=True)
            
        except KeyboardInterrupt:
            logger.info("🛑 Keyboard interrupt received")
        except Exception as e:
            logger.critical(f"💥 Fatal error in enhanced manager: {e}")
        finally:
            await self.stop()
    
    async def _start_market_monitors(self):
        """Start enhanced monitoring tasks for all markets."""
        for market in self.config.target_markets:
            await self._add_market_monitor(market)
    
    async def _add_market_monitor(self, market: str):
        """Add enhanced market monitor."""
        if market in self.market_monitors:
            logger.warning(f"⚠️ Enhanced monitor for {market} already exists")
            return
        
        logger.info(f"➕ Adding enhanced monitor for {market}")
        
        # Ensure table exists
        self.db.ensure_market_table(market)
        
        # Add WebSocket connection
        await self.ws_manager.add_market(market)
        
        # Create enhanced monitor
        monitor = EnhancedMarketMonitor(market, self.config, self.db, self.ws_manager)
        
        # Start monitoring task
        task = asyncio.create_task(monitor.run())
        
        self.market_monitors[market] = monitor
        self.monitor_tasks[market] = task
        
        logger.info(f"✅ Started enhanced monitoring for {market}")
    
    async def _remove_market_monitor(self, market: str):
        """Remove enhanced market monitor."""
        if market not in self.market_monitors:
            return
        
        logger.info(f"➖ Removing enhanced monitor for {market}")
        
        # Stop monitor
        await self.market_monitors[market].stop()
        
        # Cancel task
        if market in self.monitor_tasks:
            self.monitor_tasks[market].cancel()
            del self.monitor_tasks[market]
        
        # Remove WebSocket connection
        await self.ws_manager.remove_market(market)
        
        del self.market_monitors[market]
        logger.info(f"🗑️ Removed enhanced monitor for {market}")
    
    async def _hot_reload_task(self):
        """Hot-reload task for dynamic market management."""
        logger.info("🔥 Enhanced hot-reload task started")
        
        while self.running:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                # Check for market changes
                added_markets = self.config.reload_markets()
                current_markets = set(self.config.target_markets)
                existing_markets = set(self.market_monitors.keys())
                
                # Add new markets
                for market in added_markets:
                    await self._add_market_monitor(market)
                
                # Remove markets no longer configured
                removed_markets = existing_markets - current_markets
                for market in removed_markets:
                    await self._remove_market_monitor(market)
                    
            except Exception as e:
                logger.error(f"❌ Hot-reload error: {e}")
                await asyncio.sleep(5)
    
    async def _stats_reporter_task(self):
        """Enhanced statistics reporting."""
        while self.running:
            try:
                await asyncio.sleep(60)  # Report every minute
                
                if not self.running:
                    break
                
                # Calculate uptime
                uptime = datetime.now(timezone.utc) - self.start_time if self.start_time else None
                
                # Get WebSocket stats
                ws_stats = self.ws_manager.get_all_stats()
                
                # Log comprehensive stats
                logger.info("=" * 80)
                logger.info("📊 ENHANCED SYSTEM STATISTICS")
                logger.info(f"⏰ Uptime: {uptime}")
                logger.info(f"🎯 Active Markets: {len(self.market_monitors)}")
                logger.info(f"📡 WebSocket Messages: {self.total_messages_processed}")
                
                for market, stats in ws_stats.items():
                    connected = "🟢" if stats["connected"] else "🔴"
                    logger.info(
                        f"{connected} {market}: "
                        f"{stats['messages_received']} msgs, "
                        f"{stats['snapshots_received']} snapshots, "
                        f"{stats['ping_count']} pings"
                    )
                
                logger.info("=" * 80)
                
            except Exception as e:
                logger.error(f"❌ Stats reporter error: {e}")
    
    async def stop(self):
        """Stop enhanced multi-market monitoring."""
        logger.info("🛑 Stopping Enhanced Multi-Market Manager...")
        self.running = False
        
        # Stop WebSocket manager
        await self.ws_manager.stop()
        
        # Stop all monitors
        for market, monitor in self.market_monitors.items():
            logger.info(f"Stopping {market} monitor...")
            await monitor.stop()
        
        # Cancel all tasks
        for market, task in self.monitor_tasks.items():
            logger.info(f"Cancelling {market} task...")
            task.cancel()
        
        # Disconnect database
        self.db.disconnect()
        
        logger.info("✅ Enhanced Multi-Market Manager stopped")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}")
        self.running = False

async def main():
    """Main entry point for enhanced monitoring."""
    manager = EnhancedMultiMarketManager()
    
    # Setup
    setup_success = await manager.setup()
    if not setup_success:
        logger.error("❌ Failed to setup enhanced manager")
        sys.exit(1)
    
    # Start enhanced monitoring
    await manager.start()

if __name__ == "__main__":
    asyncio.run(main())
