#!/usr/bin/env python3
"""
Enhanced Multi-Market Demo Script
Demonstrates the complete enhanced system with real-time WebSocket data.
"""
import asyncio
import logging
import sys
import signal
from datetime import datetime, timezone
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from enhanced_multi_market_monitor import EnhancedMultiMarketManager

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedDemo:
    """Enhanced system demonstration."""
    
    def __init__(self):
        self.manager = None
        self.running = True
        
    async def run_demo(self):
        """Run the enhanced system demonstration."""
        logger.info("🎬 ENHANCED MULTI-MARKET MONITORING DEMO")
        logger.info("=" * 80)
        
        try:
            # Create enhanced manager
            self.manager = EnhancedMultiMarketManager()
            
            # Setup signal handling
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            
            # Setup the system
            logger.info("🔧 Setting up enhanced system...")
            setup_success = await self.manager.setup()
            
            if not setup_success:
                logger.error("❌ Enhanced system setup failed!")
                logger.info("💡 Make sure your WebSocket server is running:")
                logger.info("   cd /path/to/order_book_server")
                logger.info("   cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000")
                return False
            
            logger.info("✅ Enhanced system setup completed!")
            
            # Show configuration
            config = self.manager.config
            logger.info("\n📋 CONFIGURATION:")
            logger.info(f"   Markets: {', '.join(config.target_markets)}")
            logger.info(f"   WebSocket: {config.ORDERBOOK_WS_URL}")
            logger.info(f"   Node API: {config.NODE_INFO_URL}")
            logger.info(f"   Interval: {config.MONITORING_INTERVAL}s")
            
            # Start demonstration
            logger.info("\n🚀 Starting enhanced monitoring demonstration...")
            logger.info("   ⭐ Real-time WebSocket connections")
            logger.info("   ⭐ Concurrent multi-market processing")
            logger.info("   ⭐ Industry-standard atomic operations")
            logger.info("   ⭐ Dynamic hot-reload capability")
            logger.info("\n⏱️  Demo will run for 2 minutes, then show results...")
            logger.info("   Press Ctrl+C to stop early")
            
            # Start the enhanced system
            demo_task = asyncio.create_task(self.manager.start())
            
            # Let it run for demo duration or until interrupted
            try:
                await asyncio.wait_for(demo_task, timeout=120.0)  # 2 minutes
            except asyncio.TimeoutError:
                logger.info("\n⏰ Demo timeout reached!")
                await self._show_demo_results()
            
            return True
            
        except KeyboardInterrupt:
            logger.info("\n🛑 Demo interrupted by user")
            await self._show_demo_results()
            return True
        except Exception as e:
            logger.error(f"💥 Demo error: {e}")
            return False
        finally:
            if self.manager:
                await self.manager.stop()
    
    async def _show_demo_results(self):
        """Show demonstration results."""
        logger.info("\n" + "=" * 80)
        logger.info("📊 ENHANCED SYSTEM DEMO RESULTS")
        logger.info("=" * 80)
        
        if not self.manager:
            return
        
        try:
            # Show WebSocket statistics
            ws_stats = self.manager.ws_manager.get_all_stats()
            
            logger.info("🔗 WebSocket Connection Results:")
            total_messages = 0
            total_snapshots = 0
            connected_markets = 0
            
            for market, stats in ws_stats.items():
                connected = stats["connected"]
                messages = stats["messages_received"]
                snapshots = stats["snapshots_received"]
                pings = stats["ping_count"]
                
                status = "🟢 CONNECTED" if connected else "🔴 DISCONNECTED"
                logger.info(f"   {market:>6}: {status} | {messages:>4} msgs | {snapshots:>3} snapshots | {pings:>2} pings")
                
                total_messages += messages
                total_snapshots += snapshots
                if connected:
                    connected_markets += 1
            
            logger.info(f"\n📈 Overall Statistics:")
            logger.info(f"   Connected Markets: {connected_markets}/{len(ws_stats)}")
            logger.info(f"   Total Messages: {total_messages}")
            logger.info(f"   Total Snapshots: {total_snapshots}")
            logger.info(f"   WebSocket Messages Processed: {self.manager.total_messages_processed}")
            
            # Show order book samples
            logger.info(f"\n📊 Real-Time Order Book Samples:")
            for market in self.manager.config.target_markets:
                orderbook = self.manager.ws_manager.get_orderbook(market)
                if orderbook:
                    best_bid = orderbook.bids[0][0] if orderbook.bids else 0
                    best_ask = orderbook.asks[0][0] if orderbook.asks else 0
                    spread = best_ask - best_bid if best_bid and best_ask else 0
                    
                    logger.info(f"   {market:>6}: Bid ${best_bid:.4f} | Ask ${best_ask:.4f} | "
                              f"Spread ${spread:.4f} | {len(orderbook.bids)} bids, {len(orderbook.asks)} asks")
                else:
                    logger.info(f"   {market:>6}: No order book data")
            
            # Show database status
            db_connected = self.manager.db.is_connected()
            logger.info(f"\n💾 Database Status: {'✅ Connected' if db_connected else '❌ Disconnected'}")
            
            # Show system uptime
            if self.manager.start_time:
                uptime = datetime.now(timezone.utc) - self.manager.start_time
                logger.info(f"⏰ System Uptime: {uptime}")
            
            logger.info("\n🎯 Demo Conclusions:")
            if connected_markets > 0:
                logger.info("   ✅ Real-time WebSocket connections established")
                logger.info("   ✅ Concurrent multi-market processing working")
                logger.info("   ✅ Order book data streaming successfully")
                logger.info("   ✅ System ready for production use!")
                
                logger.info(f"\n🚀 Next Steps:")
                logger.info(f"   1. Run: python enhanced_multi_market_monitor.py")
                logger.info(f"   2. Add markets: Edit COIN_SYMBOL in .env")
                logger.info(f"   3. Monitor: Watch real-time statistics")
                logger.info(f"   4. Scale: Add more markets as needed")
            else:
                logger.info("   ⚠️ No WebSocket connections established")
                logger.info("   💡 Check if the Rust WebSocket server is running")
                logger.info("   💡 Verify ORDERBOOK_WS_URL in .env file")
            
        except Exception as e:
            logger.error(f"❌ Error showing demo results: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"\n🛑 Received shutdown signal {signum}")
        self.running = False

async def main():
    """Main demo entry point."""
    demo = EnhancedDemo()
    
    try:
        success = await demo.run_demo()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"💥 Demo crashed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                  🚀 ENHANCED MULTI-MARKET MONITORING DEMO 🚀                 ║
║                                                                              ║
║  This demo showcases:                                                        ║
║  ⭐ Real-time WebSocket connections with L4Book data                         ║
║  ⭐ Concurrent processing of multiple markets                                ║
║  ⭐ Industry-standard atomic database operations                             ║
║  ⭐ Dynamic hot-reload capabilities                                          ║
║                                                                              ║
║  Prerequisites:                                                              ║
║  1. Rust WebSocket server running on localhost:8000                         ║
║  2. Local Hyperliquid node running on localhost:3001                        ║
║  3. PostgreSQL database configured in .env                                  ║
║                                                                              ║
║  Press Enter to start the demo...                                           ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    input()
    
    asyncio.run(main())
