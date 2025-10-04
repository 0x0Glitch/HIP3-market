"""
Unified Prometheus metrics server for all monitoring systems.
Serves metrics on port 9090 to avoid conflict with WebSocket server on 8001.
"""

from prometheus_client import start_http_server, generate_latest, CollectorRegistry, REGISTRY
from prometheus_client import Gauge, Counter
import threading
import time
import logging
import os
import sys
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import market metrics
try:
    from metrics_market import MARKET_REGISTRY, MetricsCollector as MarketCollector
    MARKET_METRICS_AVAILABLE = True
except ImportError:
    logger.warning("Market metrics module not found")
    MARKET_METRICS_AVAILABLE = False
    MARKET_REGISTRY = None

# Import user metrics if available
try:
    # Add path for user_monitoring if needed
    user_monitoring_path = '/Users/anshuman/anthias_hl/code/Anthias-risk-monitoring/user_monitoring'
    if os.path.exists(user_monitoring_path):
        sys.path.insert(0, user_monitoring_path)
    
    from metrics_user import USER_REGISTRY
    USER_METRICS_AVAILABLE = True
except ImportError:
    logger.warning("User metrics module not found")
    USER_METRICS_AVAILABLE = False
    USER_REGISTRY = None

# Global server status metrics
SERVER_REGISTRY = CollectorRegistry()

METRICS_SERVER_UP = Gauge(
    'metrics_server_up',
    'Metrics server status (1=up, 0=down)',
    registry=SERVER_REGISTRY
)

METRICS_SERVER_START_TIME = Gauge(
    'metrics_server_start_time_seconds',
    'Unix timestamp when metrics server started',
    registry=SERVER_REGISTRY
)

METRICS_REGISTRIES_ACTIVE = Gauge(
    'metrics_registries_active',
    'Number of active metric registries',
    registry=SERVER_REGISTRY
)

class UnifiedMetricsServer:
    """Unified metrics server combining all monitoring systems."""
    
    def __init__(self, port: int = 9090):
        self.port = port
        self.combined_registry = CollectorRegistry()
        self._setup_registries()
        self._server_thread = None
        self._collector_threads = []
        self.running = False
        
    def _setup_registries(self):
        """Combine all available metric registries."""
        registries_count = 0
        
        # Add server metrics
        self._merge_registry(SERVER_REGISTRY)
        registries_count += 1
        
        # Add market metrics if available
        if MARKET_METRICS_AVAILABLE and MARKET_REGISTRY:
            self._merge_registry(MARKET_REGISTRY)
            registries_count += 1
            logger.info("✅ Market metrics registry loaded")
        
        # Add user metrics if available
        if USER_METRICS_AVAILABLE and USER_REGISTRY:
            self._merge_registry(USER_REGISTRY)
            registries_count += 1
            logger.info("✅ User metrics registry loaded")
        
        # Update registry count
        METRICS_REGISTRIES_ACTIVE.set(registries_count)
        logger.info(f"📊 Loaded {registries_count} metric registries")
    
    def _merge_registry(self, registry: CollectorRegistry):
        """Merge a registry into the combined registry."""
        try:
            # Copy collectors from source registry to combined registry
            for collector in registry._collector_to_names:
                self.combined_registry.register(collector)
        except Exception as e:
            logger.error(f"Error merging registry: {e}")
    
    def start(self):
        """Start the metrics server and background collectors."""
        try:
            # Start HTTP server
            start_http_server(self.port, registry=self.combined_registry)
            self.running = True
            
            # Update server metrics
            METRICS_SERVER_UP.set(1)
            METRICS_SERVER_START_TIME.set(time.time())
            
            # Start background collectors
            self._start_collectors()
            
            logger.info(f"🚀 Prometheus metrics server started on port {self.port}")
            logger.info(f"📊 Metrics available at: http://localhost:{self.port}/metrics")
            
            # Print available metrics endpoints
            self._print_metrics_info()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")
            METRICS_SERVER_UP.set(0)
            return False
    
    def _start_collectors(self):
        """Start background metric collectors."""
        # Start market metrics collector if available
        if MARKET_METRICS_AVAILABLE:
            try:
                market_collector = MarketCollector(interval=30)
                market_collector.start()
                self._collector_threads.append(market_collector)
                logger.info("✅ Started market metrics collector")
            except Exception as e:
                logger.error(f"Failed to start market collector: {e}")
        
        # Start system metrics collection thread
        system_thread = threading.Thread(target=self._collect_system_metrics, daemon=True)
        system_thread.start()
        logger.info("✅ Started system metrics collector")
    
    def _collect_system_metrics(self):
        """Collect system-wide metrics periodically."""
        while self.running:
            try:
                # Collect system metrics for different processes
                if MARKET_METRICS_AVAILABLE:
                    from metrics_market import update_system_metrics
                    update_system_metrics('hip3_market')
                
                if USER_METRICS_AVAILABLE:
                    from metrics_user import update_user_system_metrics
                    update_user_system_metrics('user_monitoring')
                
                time.sleep(30)  # Collect every 30 seconds
                
            except Exception as e:
                logger.error(f"Error collecting system metrics: {e}")
                time.sleep(5)
    
    def _print_metrics_info(self):
        """Print information about available metrics."""
        print("\n" + "="*60)
        print("📊 PROMETHEUS METRICS SERVER RUNNING")
        print("="*60)
        print(f"Server URL:     http://localhost:{self.port}/metrics")
        print(f"Status:         {'✅ UP' if self.running else '❌ DOWN'}")
        print("\nAvailable Metric Groups:")
        
        if MARKET_METRICS_AVAILABLE:
            print("  ✅ Market Monitoring Metrics:")
            print("     - Market data freshness")
            print("     - Orderbook snapshots age")
            print("     - API performance & errors")
            print("     - WebSocket connections")
            print("     - Database operations")
            print("     - Liquidity depth metrics")
            
        if USER_METRICS_AVAILABLE:
            print("  ✅ User Monitoring Metrics:")
            print("     - User snapshots processed")
            print("     - Position updates")
            print("     - Component health")
            print("     - API query performance")
        
        print("\n  ✅ System Metrics:")
        print("     - Memory usage")
        print("     - CPU usage")
        print("     - Thread count")
        print("     - Asyncio tasks")
        
        print("\n📈 Grafana Configuration:")
        print(f"   Add Prometheus datasource: http://localhost:{self.port}")
        print("="*60 + "\n")
    
    def stop(self):
        """Stop the metrics server and collectors."""
        self.running = False
        METRICS_SERVER_UP.set(0)
        
        # Stop collectors
        for collector in self._collector_threads:
            if hasattr(collector, 'stop'):
                collector.stop()
        
        logger.info("Metrics server stopped")

def main():
    """Main entry point for standalone metrics server."""
    import signal
    import sys
    
    # Get port from environment or use default
    port = int(os.getenv('METRICS_PORT', '9090'))
    
    # Create and start server
    server = UnifiedMetricsServer(port=port)
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        logger.info("\nShutting down metrics server...")
        server.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start server
    if server.start():
        # Keep running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    else:
        logger.error("Failed to start metrics server")
        sys.exit(1)

if __name__ == "__main__":
    main()
