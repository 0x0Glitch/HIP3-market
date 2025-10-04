"""
Prometheus metrics for HIP3 market monitoring system.
Non-intrusive monitoring using decorators and context managers.
"""

from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry
from functools import wraps
import time
import asyncio
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Create a dedicated registry for market metrics
MARKET_REGISTRY = CollectorRegistry()

# =============================================================================
# BUSINESS METRICS (Critical for Trading Operations)
# =============================================================================

# Market Data Quality Metrics
MARKET_DATA_AGE = Gauge(
    'market_data_age_seconds',
    'Age of market data in seconds',
    ['coin', 'data_source'],
    registry=MARKET_REGISTRY
)

ORDERBOOK_SNAPSHOT_AGE = Gauge(
    'orderbook_snapshot_age_seconds', 
    'Age of orderbook snapshot',
    ['coin'],
    registry=MARKET_REGISTRY
)

# API Performance & Error Metrics
HYPERLIQUID_API_REQUESTS = Counter(
    'hyperliquid_api_requests_total',
    'Total Hyperliquid API requests',
    ['method', 'endpoint', 'status_code'],
    registry=MARKET_REGISTRY
)

HYPERLIQUID_API_DURATION = Histogram(
    'hyperliquid_api_duration_seconds',
    'Hyperliquid API response time',
    ['endpoint'],
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=MARKET_REGISTRY
)

API_RATE_LIMIT_ERRORS = Counter(
    'api_rate_limit_errors_total',
    'Number of 429 rate limit errors',
    ['api_endpoint'],
    registry=MARKET_REGISTRY
)

# Monitoring Cycle Performance Metrics
MONITORING_CYCLE_DURATION = Histogram(
    'monitoring_cycle_duration_seconds',
    'Time to complete monitoring cycle',
    ['coin'],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
    registry=MARKET_REGISTRY
)

MONITORING_CYCLES = Counter(
    'monitoring_cycles_total',
    'Total monitoring cycles',
    ['coin', 'status'],
    registry=MARKET_REGISTRY
)

CONSECUTIVE_FAILURES = Gauge(
    'consecutive_failures_count',
    'Number of consecutive failures per market',
    ['coin'],
    registry=MARKET_REGISTRY
)

ACTIVE_MARKETS = Gauge(
    'active_markets_count',
    'Number of actively monitored markets',
    registry=MARKET_REGISTRY
)

# Market-Specific Business Metrics
MARKET_SPREADS = Gauge(
    'market_spread_percentage',
    'Bid-ask spread as percentage', 
    ['coin'],
    registry=MARKET_REGISTRY
)

LIQUIDITY_DEPTH = Gauge(
    'orderbook_liquidity_depth',
    'Orderbook liquidity depth in base asset units',
    ['coin', 'percentage', 'side'],
    registry=MARKET_REGISTRY
)

MARKET_PRICES = Gauge(
    'market_price_usd',
    'Market prices in USD',
    ['coin', 'price_type'],  # mark_price, oracle_price, mid_price
    registry=MARKET_REGISTRY
)

# =============================================================================
# WEBSOCKET METRICS
# =============================================================================

WEBSOCKET_CONNECTIONS = Gauge(
    'websocket_connections_active',
    'Active WebSocket connections',
    ['coin', 'status'],
    registry=MARKET_REGISTRY
)

WEBSOCKET_RECONNECTIONS = Counter(
    'websocket_reconnections_total',
    'WebSocket reconnection attempts',
    ['coin', 'reason'],
    registry=MARKET_REGISTRY
)

WEBSOCKET_MESSAGE_RATE = Gauge(
    'websocket_message_rate_per_second',
    'Rate of WebSocket messages received',
    ['coin', 'message_type'],
    registry=MARKET_REGISTRY
)

WEBSOCKET_LATENCY = Histogram(
    'websocket_latency_milliseconds',
    'WebSocket message latency',
    ['coin'],
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
    registry=MARKET_REGISTRY
)

WEBSOCKET_SUBSCRIBE_DURATION = Histogram(
    'websocket_subscribe_duration_seconds',
    'Time to get fresh orderbook snapshot after subscription',
    ['coin'],
    buckets=(0.5, 1.0, 2.0, 3.0, 5.0, 10.0),
    registry=MARKET_REGISTRY
)

# =============================================================================
# DATABASE METRICS
# =============================================================================

DATABASE_OPERATIONS = Counter(
    'database_operations_total',
    'Database operations count',
    ['operation', 'table', 'status'],
    registry=MARKET_REGISTRY
)

DATABASE_OPERATION_DURATION = Histogram(
    'database_operation_duration_seconds',
    'Database operation duration',
    ['operation', 'table'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
    registry=MARKET_REGISTRY
)

DATABASE_CONNECTION_POOL = Gauge(
    'database_connection_pool_size',
    'Database connection pool status',
    ['status'],
    registry=MARKET_REGISTRY
)

DATABASE_INSERT_BATCH_SIZE = Histogram(
    'database_insert_batch_size',
    'Size of database insert batches',
    ['table'],
    buckets=(1, 5, 10, 25, 50, 100, 250),
    registry=MARKET_REGISTRY
)

# =============================================================================
# SYSTEM METRICS
# =============================================================================

PYTHON_MEMORY_USAGE = Gauge(
    'python_memory_usage_bytes',
    'Python process memory usage',
    ['process_name'],
    registry=MARKET_REGISTRY
)

CPU_USAGE_PERCENT = Gauge(
    'cpu_usage_percent',
    'CPU usage percentage',
    ['process_name'],
    registry=MARKET_REGISTRY
)

ASYNCIO_TASKS = Gauge(
    'asyncio_tasks_count',
    'Number of asyncio tasks',
    ['status'],
    registry=MARKET_REGISTRY
)

THREAD_COUNT = Gauge(
    'thread_count',
    'Number of active threads',
    ['process_name'],
    registry=MARKET_REGISTRY
)

# =============================================================================
# DECORATOR FUNCTIONS FOR NON-INTRUSIVE MONITORING
# =============================================================================

def monitor_api_call(endpoint: str):
    """Decorator to monitor API calls without changing business logic."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                # Success metrics
                HYPERLIQUID_API_REQUESTS.labels(
                    method='POST', 
                    endpoint=endpoint, 
                    status_code='200'
                ).inc()
                HYPERLIQUID_API_DURATION.labels(endpoint=endpoint).observe(
                    time.time() - start_time
                )
                return result
            except Exception as e:
                # Error metrics
                status_code = '429' if '429' in str(e) else '500'
                HYPERLIQUID_API_REQUESTS.labels(
                    method='POST',
                    endpoint=endpoint, 
                    status_code=status_code
                ).inc()
                if status_code == '429':
                    API_RATE_LIMIT_ERRORS.labels(api_endpoint=endpoint).inc()
                raise
        return wrapper
    return decorator

def monitor_cycle_performance(func):
    """Decorator to monitor monitoring cycle performance."""
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        coin = getattr(self, 'market', 'unknown')
        start_time = time.time()
        try:
            result = await func(self, *args, **kwargs)
            duration = time.time() - start_time
            
            # Success metrics
            MONITORING_CYCLE_DURATION.labels(coin=coin).observe(duration)
            if result:  # Assuming result is boolean for success/failure
                MONITORING_CYCLES.labels(coin=coin, status='success').inc()
                CONSECUTIVE_FAILURES.labels(coin=coin).set(0)
            else:
                MONITORING_CYCLES.labels(coin=coin, status='partial').inc()
            
            return result
        except Exception as e:
            # Failure metrics
            duration = time.time() - start_time
            MONITORING_CYCLES.labels(coin=coin, status='failed').inc()
            
            # Increment consecutive failures
            try:
                current_failures = CONSECUTIVE_FAILURES.labels(coin=coin)._value.get()
                CONSECUTIVE_FAILURES.labels(coin=coin).set(current_failures + 1)
            except:
                CONSECUTIVE_FAILURES.labels(coin=coin).set(1)
            
            raise
    return wrapper

def monitor_database_operation(operation: str, table: str):
    """Decorator for database operations."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                DATABASE_OPERATIONS.labels(
                    operation=operation, 
                    table=table, 
                    status='success'
                ).inc()
                DATABASE_OPERATION_DURATION.labels(
                    operation=operation, 
                    table=table
                ).observe(duration)
                
                return result
            except Exception as e:
                DATABASE_OPERATIONS.labels(
                    operation=operation,
                    table=table, 
                    status='error'
                ).inc()
                raise
        return wrapper
    return decorator

def monitor_websocket_operation(func):
    """Decorator for WebSocket operations."""
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        coin = getattr(self, 'coin', 'unknown')
        start_time = time.time()
        try:
            result = await func(self, *args, **kwargs)
            
            # Track subscription duration for get_fresh_metrics
            if func.__name__ == 'get_fresh_metrics':
                duration = time.time() - start_time
                WEBSOCKET_SUBSCRIBE_DURATION.labels(coin=coin).observe(duration)
            
            return result
        except Exception as e:
            if 'reconnect' in str(e).lower() or 'disconnect' in str(e).lower():
                WEBSOCKET_RECONNECTIONS.labels(
                    coin=coin, 
                    reason='connection_error'
                ).inc()
            raise
    return wrapper

# =============================================================================
# HELPER FUNCTIONS FOR METRICS COLLECTION
# =============================================================================

def update_market_metrics(coin: str, market_data: Dict[str, Any], orderbook_data: Dict[str, Any]):
    """Update market-specific business metrics."""
    
    try:
        # Market data metrics
        if market_data:
            # Price metrics
            if 'mark_price' in market_data:
                MARKET_PRICES.labels(coin=coin, price_type='mark_price').set(
                    float(market_data['mark_price'])
                )
            if 'oracle_price' in market_data:
                MARKET_PRICES.labels(coin=coin, price_type='oracle_price').set(
                    float(market_data['oracle_price'])
                )
            
            # Spread metrics
            if 'spread_pct' in market_data:
                MARKET_SPREADS.labels(coin=coin).set(
                    float(market_data['spread_pct'])
                )
            
            # Data freshness
            if 'timestamp' in market_data:
                data_age = time.time() - market_data['timestamp']
                MARKET_DATA_AGE.labels(coin=coin, data_source='rest_api').set(data_age)
        
        # Orderbook metrics
        if orderbook_data:
            # Liquidity depth metrics
            for pct in ['5pct', '10pct', '25pct']:
                bid_key = f'bid_depth_{pct}'
                ask_key = f'ask_depth_{pct}'
                
                if bid_key in orderbook_data:
                    LIQUIDITY_DEPTH.labels(
                        coin=coin, 
                        percentage=pct, 
                        side='bid'
                    ).set(float(orderbook_data[bid_key]))
                
                if ask_key in orderbook_data:
                    LIQUIDITY_DEPTH.labels(
                        coin=coin,
                        percentage=pct, 
                        side='ask'
                    ).set(float(orderbook_data[ask_key]))
            
            # Mid price if available
            if 'mid_price' in orderbook_data:
                MARKET_PRICES.labels(coin=coin, price_type='mid_price').set(
                    float(orderbook_data['mid_price'])
                )
    
    except Exception as e:
        logger.error(f"Error updating market metrics for {coin}: {e}")

def update_websocket_metrics(coin: str, ws_client):
    """Update WebSocket connection metrics."""
    
    try:
        # Connection status
        if hasattr(ws_client, 'ws') and ws_client.ws:
            if ws_client.ws.closed:
                WEBSOCKET_CONNECTIONS.labels(coin=coin, status='disconnected').set(1)
                WEBSOCKET_CONNECTIONS.labels(coin=coin, status='connected').set(0)
            else:
                WEBSOCKET_CONNECTIONS.labels(coin=coin, status='connected').set(1)
                WEBSOCKET_CONNECTIONS.labels(coin=coin, status='disconnected').set(0)
        else:
            WEBSOCKET_CONNECTIONS.labels(coin=coin, status='disconnected').set(1)
            WEBSOCKET_CONNECTIONS.labels(coin=coin, status='connected').set(0)
        
        # Snapshot age
        if hasattr(ws_client, '_snapshot_time') and ws_client._snapshot_time:
            age = time.time() - ws_client._snapshot_time
            ORDERBOOK_SNAPSHOT_AGE.labels(coin=coin).set(age)
    
    except Exception as e:
        logger.error(f"Error updating WebSocket metrics for {coin}: {e}")

def update_database_pool_metrics(pool):
    """Update database connection pool metrics."""
    
    try:
        if hasattr(pool, 'size'):
            DATABASE_CONNECTION_POOL.labels(status='total').set(pool.size)
        if hasattr(pool, 'num_idle'):
            DATABASE_CONNECTION_POOL.labels(status='idle').set(pool.num_idle)
        if hasattr(pool, 'num_active'):
            DATABASE_CONNECTION_POOL.labels(status='active').set(pool.num_active)
    except Exception as e:
        logger.error(f"Error updating database pool metrics: {e}")

def update_system_metrics(process_name: str = 'hip3_market'):
    """Update system-level metrics."""
    
    try:
        import psutil
        import threading
        
        # Get current process
        process = psutil.Process()
        
        # Memory metrics
        memory_info = process.memory_info()
        PYTHON_MEMORY_USAGE.labels(process_name=process_name).set(memory_info.rss)
        
        # CPU metrics
        cpu_percent = process.cpu_percent(interval=0.1)
        CPU_USAGE_PERCENT.labels(process_name=process_name).set(cpu_percent)
        
        # Thread count
        THREAD_COUNT.labels(process_name=process_name).set(threading.active_count())
        
        # Asyncio tasks
        try:
            all_tasks = asyncio.all_tasks()
            pending_tasks = [t for t in all_tasks if not t.done()]
            ASYNCIO_TASKS.labels(status='pending').set(len(pending_tasks))
            ASYNCIO_TASKS.labels(status='total').set(len(all_tasks))
        except RuntimeError:
            # No event loop in this thread
            pass
    
    except Exception as e:
        logger.error(f"Error updating system metrics: {e}")

# =============================================================================
# BACKGROUND METRICS COLLECTION
# =============================================================================

class MetricsCollector:
    """Background metrics collection thread."""
    
    def __init__(self, interval: int = 30):
        self.interval = interval
        self.running = False
        self._thread = None
    
    def start(self):
        """Start background metrics collection."""
        self.running = True
        import threading
        self._thread = threading.Thread(target=self._collect_loop, daemon=True)
        self._thread.start()
        logger.info(f"Started metrics collector with {self.interval}s interval")
    
    def stop(self):
        """Stop background metrics collection."""
        self.running = False
        if self._thread:
            self._thread.join(timeout=5)
    
    def _collect_loop(self):
        """Main collection loop."""
        while self.running:
            try:
                update_system_metrics()
                time.sleep(self.interval)
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")
                time.sleep(5)  # Brief pause on error
