import psycopg2
import psycopg2.pool
import psycopg2.extras
import threading
import time
import os
from datetime import datetime
from typing import Dict, Any, Optional, List, Set
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def get_table_schema(coin_symbol: str) -> str:
    """Get table schema for a specific coin."""
    table_name = f"{coin_symbol.lower()}_metrics_raw"
    return f"""
    CREATE TABLE IF NOT EXISTS market_metrics.{table_name} (
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
        node_latency_ms INTEGER,
        premium DECIMAL(12, 10),
        impact_px_bid DECIMAL(20, 8),
        impact_px_ask DECIMAL(20, 8),
        websocket_latency_ms INTEGER,
        total_latency_ms INTEGER,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(timestamp, coin)
    );
    
    CREATE INDEX IF NOT EXISTS idx_{coin_symbol.lower()}_metrics_timestamp 
        ON market_metrics.{table_name}(timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_{coin_symbol.lower()}_metrics_coin_timestamp 
        ON market_metrics.{table_name}(coin, timestamp DESC);
    """

class Database:
    def __init__(self, connection_url: str, min_connections: int = 1, max_connections: int = 5):
        self.connection_url = connection_url
        self.pool = None
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.lock = threading.Lock()
        self.created_tables: Set[str] = set()

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

            # Create market_metrics schema
            self._create_schema()
            logger.info("Database schema created/verified")

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
                logger.error(f"Failed to close database pool: {e}")
            finally:
                self.pool = None

    def _create_schema(self):
        """Create market_metrics schema if it doesn't exist."""
        conn = self.pool.getconn()
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS market_metrics;")
                logger.info("Schema 'market_metrics' created/verified")
        finally:
            self.pool.putconn(conn)
    
    def ensure_market_table(self, coin_symbol: str) -> bool:
        """Ensure table exists for a specific market."""
        table_name = f"{coin_symbol.lower()}_metrics_raw"
        
        if table_name in self.created_tables:
            return True
            
        conn = self.pool.getconn()
        try:
            conn.autocommit = True
            with conn.cursor() as cursor:
                schema_sql = get_table_schema(coin_symbol)
                cursor.execute(schema_sql)
                self.created_tables.add(table_name)
                logger.info(f"✓ Created/verified table: market_metrics.{table_name}")
                return True
        except Exception as e:
            logger.error(f"Failed to create table for {coin_symbol}: {e}")
            return False
        finally:
            self.pool.putconn(conn)
    
    def ensure_market_tables(self, coin_symbols: List[str]) -> bool:
        """Ensure tables exist for all specified markets."""
        success = True
        for coin_symbol in coin_symbols:
            if not self.ensure_market_table(coin_symbol):
                success = False
        return success
    
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
        coin_symbol = metrics.get('coin')
        if not coin_symbol:
            logger.error("Cannot insert metrics: 'coin' field is required")
            return False

        if not self.is_connected():
            if not self.reconnect():
                logger.error("Cannot insert metrics: database not connected")
                return False

        # Use market-specific table
        table_name = f"{coin_symbol.lower()}_metrics_raw"
        
        query = f"""
            INSERT INTO market_metrics.{table_name} (
                coin, mark_price, oracle_price, mid_price,
                best_bid, best_ask, spread, spread_pct,
                funding_rate_pct, open_interest, volume_24h,
                bid_depth_5pct, ask_depth_5pct, total_depth_5pct,
                bid_depth_10pct, ask_depth_10pct, total_depth_10pct,
                bid_depth_25pct, ask_depth_25pct, total_depth_25pct,
                premium, impact_px_bid, impact_px_ask,
                node_latency_ms, websocket_latency_ms, total_latency_ms
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
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
                                metrics.get('websocket_latency_ms'),
                                metrics.get('total_latency_ms')
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