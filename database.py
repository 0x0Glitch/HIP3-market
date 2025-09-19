"""Database module with connection pooling and batch operations."""

import psycopg2
from psycopg2 import pool, extras
from contextlib import contextmanager
import threading
import time
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os

logger = logging.getLogger(__name__)


class Database:
    """Database manager with connection pooling and batch operations."""
    
    def __init__(self, connection_url: str, pool_size: int = 5, max_overflow: int = 10):
        self.connection_url = connection_url
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.connection_pool = None
        self._executor = ThreadPoolExecutor(max_workers=pool_size)
        self._lock = threading.Lock()
        self._batch_buffer: List[Dict[str, Any]] = []
        self._batch_lock = threading.Lock()
        
    def connect(self) -> bool:
        """Initialize connection pool."""
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.pool_size + self.max_overflow,
                dsn=self.connection_url,
                cursor_factory=extras.RealDictCursor
            )
            logger.info(f"Database connection pool created (size: {self.pool_size})")
            
            # Load and execute schema
            schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
            with open(schema_path, 'r') as f:
                schema = f.read()
            
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(schema)
                    conn.commit()
                    logger.info("Database schema loaded")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            return False
    
    @contextmanager
    def _get_connection(self):
        """Get connection from pool with automatic cleanup."""
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    def insert_metric(self, metric: Dict[str, Any]) -> bool:
        """Insert single metric (non-blocking)."""
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        self._get_insert_query(),
                        self._prepare_metric_values(metric)
                    )
                    conn.commit()
                    return True
                    
        except Exception as e:
            logger.error(f"Failed to insert metric: {e}")
            return False
    
    async def insert_metric_async(self, metric: Dict[str, Any]) -> bool:
        """Insert metric asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.insert_metric, metric)
    
    def insert_metrics_batch(self, metrics: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Insert multiple metrics in a single transaction."""
        if not metrics:
            return 0, 0
        
        succeeded = 0
        failed = 0
        
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Prepare batch data
                    batch_values = []
                    for metric in metrics:
                        try:
                            batch_values.append(self._prepare_metric_values(metric))
                        except Exception as e:
                            logger.error(f"Failed to prepare metric: {e}")
                            failed += 1
                    
                    if batch_values:
                        # Use execute_batch for better performance
                        extras.execute_batch(
                            cursor,
                            self._get_insert_query(),
                            batch_values,
                            page_size=100
                        )
                        conn.commit()
                        succeeded = len(batch_values)
                        
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            failed += len(metrics)
        
        logger.info(f"Batch insert: {succeeded} succeeded, {failed} failed")
        return succeeded, failed
    
    def add_to_batch(self, metric: Dict[str, Any]):
        """Add metric to batch buffer."""
        with self._batch_lock:
            self._batch_buffer.append(metric)
    
    def flush_batch(self) -> Tuple[int, int]:
        """Flush batch buffer to database."""
        with self._batch_lock:
            if not self._batch_buffer:
                return 0, 0
            
            metrics = self._batch_buffer.copy()
            self._batch_buffer.clear()
        
        return self.insert_metrics_batch(metrics)
    
    async def flush_batch_async(self) -> Tuple[int, int]:
        """Flush batch asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.flush_batch)
    
    def _get_insert_query(self) -> str:
        """Get insert query."""
        return """
            INSERT INTO market_metrics (
                coin, mark_price, oracle_price, mid_price,
                best_bid, best_ask, spread, spread_pct,
                funding_rate_pct, open_interest, volume_24h,
                bid_depth_5pct, ask_depth_5pct, total_depth_5pct,
                bid_depth_10pct, ask_depth_10pct, total_depth_10pct,
                bid_depth_25pct, ask_depth_25pct, total_depth_25pct,
                premium, impact_px_bid, impact_px_ask,
                node_latency_ms, ws_latency_ms, orderbook_levels, 
                total_bids, total_asks
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
    
    def _prepare_metric_values(self, metric: Dict[str, Any]) -> Tuple:
        """Prepare metric values for insertion."""
        if not metric.get('coin'):
            raise ValueError("'coin' field is required")
        
        return (
            metric['coin'],
            metric.get('mark_price'),
            metric.get('oracle_price'),
            metric.get('mid_price'),
            metric.get('best_bid'),
            metric.get('best_ask'),
            metric.get('spread'),
            metric.get('spread_pct'),
            metric.get('funding_rate_pct'),
            metric.get('open_interest'),
            metric.get('volume_24h'),
            metric.get('bid_depth_5pct'),
            metric.get('ask_depth_5pct'),
            metric.get('total_depth_5pct'),
            metric.get('bid_depth_10pct'),
            metric.get('ask_depth_10pct'),
            metric.get('total_depth_10pct'),
            metric.get('bid_depth_25pct'),
            metric.get('ask_depth_25pct'),
            metric.get('total_depth_25pct'),
            metric.get('premium'),
            metric.get('impact_px_bid'),
            metric.get('impact_px_ask'),
            metric.get('node_latency_ms'),
            metric.get('ws_latency_ms'),
            metric.get('orderbook_levels'),
            metric.get('total_bids'),
            metric.get('total_asks')
        )
    
    def get_recent_metrics(self, coin: str, limit: int = 100) -> List[Dict]:
        """Get recent metrics for a coin."""
        try:
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT * FROM market_metrics
                        WHERE coin = %s
                        ORDER BY timestamp DESC
                        LIMIT %s
                    """, (coin, limit))
                    
                    return cursor.fetchall()
                    
        except Exception as e:
            logger.error(f"Failed to get recent metrics: {e}")
            return []
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get database performance statistics."""
        try:
            with self._get_connection() as conn:
                with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
                    stats = {
                        "pool_size": self.pool_size,
                        "batch_buffer_size": len(self._batch_buffer)
                    }
                    
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_rows,
                            COUNT(DISTINCT coin) as unique_coins,
                            MIN(timestamp) as oldest_record,
                            MAX(timestamp) as newest_record,
                            AVG(ws_latency_ms) as avg_ws_latency,
                            AVG(node_latency_ms) as avg_node_latency
                        FROM market_metrics
                        WHERE timestamp > NOW() - INTERVAL '1 hour'
                    """)
                    
                    stats.update(cursor.fetchone() or {})
                    return stats
                    
        except Exception as e:
            logger.error(f"Failed to get performance stats: {e}")
            return {}
    
    def close(self):
        """Close connection pool and cleanup."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connection pool closed")
        
        self._executor.shutdown(wait=True)
        logger.info("Database executor shut down")
