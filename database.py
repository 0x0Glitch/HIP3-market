"""Database operations for market monitoring with connection pooling."""

import psycopg2
import psycopg2.pool
import psycopg2.extras
import threading
import time
import os
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def load_schema() -> str:
    """Load database schema from SQL file."""
    schema_path = Path(__file__).parent / 'schema' / 'market_metrics.sql'
    if schema_path.exists():
        with open(schema_path, 'r') as f:
            return f.read()
    else:
        logger.warning(f"Schema file not found at {schema_path}, using embedded schema")
        # Fallback to embedded schema
        return """
        CREATE TABLE IF NOT EXISTS link_metrics_raw (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            coin VARCHAR(20) NOT NULL,
            mark_price DECIMAL(20, 8),
            oracle_price DECIMAL(20, 8),
            mid_price DECIMAL(20, 8),
            best_bid DECIMAL(20, 8),
            best_ask DECIMAL(20, 8),
            spread DECIMAL(20, 8),
            spread_pct DECIMAL(10, 6),
            funding_rate_pct DECIMAL(12, 10),
            open_interest DECIMAL(20, 8),
            volume_24h DECIMAL(20, 8),
            bid_depth_5pct DECIMAL(20, 8),
            ask_depth_5pct DECIMAL(20, 8),
            total_depth_5pct DECIMAL(20, 8),
            bid_depth_10pct DECIMAL(20, 8),
            ask_depth_10pct DECIMAL(20, 8),
            total_depth_10pct DECIMAL(20, 8),
            bid_depth_25pct DECIMAL(20, 8),
            ask_depth_25pct DECIMAL(20, 8),
            total_depth_25pct DECIMAL(20, 8),
            premium DECIMAL(12, 10),
            impact_px_bid DECIMAL(20, 8),
            impact_px_ask DECIMAL(20, 8),
            node_latency_ms INTEGER,
            orderbook_levels INTEGER,
            total_bids INTEGER,
            total_asks INTEGER
        );              
        CREATE INDEX IF NOT EXISTS idx_timestamp ON link_metrics_raw (timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_coin_timestamp ON link_metrics_raw (coin, timestamp DESC);
        """

class Database:
    def __init__(self, connection_url: str, min_connections: int = 1, max_connections: int = 5):
        self.connection_url = connection_url
        self.pool = None
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.lock = threading.Lock()

    def connect(self):
        """Create database connection pool."""
        try:
            # Create connection pool
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                self.min_connections,
                self.max_connections,
                self.connection_url
            )
            logger.info(f"Database connection pool established (min={self.min_connections}, max={self.max_connections})")

            # Create schema using a connection from the pool
            conn = self.pool.getconn()
            try:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    schema = load_schema()
                    # Execute schema statements separately
                    for statement in schema.split(';'):
                        if statement.strip():
                            try:
                                cursor.execute(statement + ';')
                            except psycopg2.ProgrammingError as e:
                                # Ignore errors for IF NOT EXISTS statements
                                if 'already exists' not in str(e):
                                    logger.warning(f"Schema statement warning: {e}")
                logger.info("Database schema created/verified")
            finally:
                self.pool.putconn(conn)

            return True

        except Exception as e:
            logger.error(f"Failed to create database connection pool: {e}")
            self.pool = None
            return False

    def disconnect(self):
        """Close all database connections in the pool."""
        if self.pool:
            try:
                self.pool.closeall()
                logger.info("Database connection pool closed")
            except Exception as e:
                logger.error(f"Error closing database connection pool: {e}")
            finally:
                self.pool = None

    def is_connected(self) -> bool:
        """Check if database pool is available."""
        if not self.pool:
            return False
        try:
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
            finally:
                self.pool.putconn(conn)
        except Exception:
            return False

    def reconnect(self) -> bool:
        """Attempt to reconnect to database."""
        logger.info("Attempting to reconnect to database...")
        self.disconnect()
        return self.connect()

    def insert_market_metrics(self, metrics: Dict[str, Any]) -> bool:
        """Insert market metrics into database."""
        # Validate required field
        if not metrics.get('coin'):
            logger.error("Cannot insert metrics: 'coin' field is required")
            return False

        if not self.is_connected():
            if not self.reconnect():
                logger.error("Cannot insert metrics: database not connected")
                return False

        query = """
            INSERT INTO link_metrics_raw (
                coin, mark_price, oracle_price, mid_price,
                best_bid, best_ask, spread, spread_pct,
                funding_rate_pct, open_interest, volume_24h,
                bid_depth_5pct, ask_depth_5pct, total_depth_5pct,
                bid_depth_10pct, ask_depth_10pct, total_depth_10pct,
                bid_depth_25pct, ask_depth_25pct, total_depth_25pct,
                premium, impact_px_bid, impact_px_ask,
                node_latency_ms, orderbook_levels, total_bids, total_asks
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
        """

        try:
            with self.lock:
                conn = self.pool.getconn()
                conn.autocommit = True
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            query,
                            (
                            metrics['coin'],
                            metrics.get('mark_price'),
                            metrics.get('oracle_price'),
                            metrics.get('mid_price'),
                            metrics.get('best_bid'),
                            metrics.get('best_ask'),
                            metrics.get('spread'),
                            metrics.get('spread_pct'),
                            metrics.get('funding_rate_pct'),
                            metrics.get('open_interest'),
                            metrics.get('volume_24h'),
                            metrics.get('bid_depth_5pct'),
                            metrics.get('ask_depth_5pct'),
                            metrics.get('total_depth_5pct'),
                            metrics.get('bid_depth_10pct'),
                            metrics.get('ask_depth_10pct'),
                            metrics.get('total_depth_10pct'),
                            metrics.get('bid_depth_25pct'),
                            metrics.get('ask_depth_25pct'),
                            metrics.get('total_depth_25pct'),
                            metrics.get('premium'),
                            metrics.get('impact_px_bid'),
                            metrics.get('impact_px_ask'),
                            metrics.get('node_latency_ms'),
                            metrics.get('orderbook_levels'),
                            metrics.get('total_bids'),
                            metrics.get('total_asks')
                        )
                        )
                        logger.info(f"Inserted market metrics for {metrics['coin']}")
                        return True
                finally:
                    self.pool.putconn(conn)

        except Exception as e:
            logger.error(f"Failed to insert market metrics: {e}")
            # Pool will handle reconnection automatically
            return False