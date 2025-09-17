"""Database schema and operations for market monitoring."""

import psycopg2
import psycopg2.extras
import threading
import time
from datetime import datetime
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS market_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    coin VARCHAR(20) NOT NULL,
    
    -- Prices
    mark_price DECIMAL(20, 8),
    oracle_price DECIMAL(20, 8),
    mid_price DECIMAL(20, 8),
    
    -- Spread
    best_bid DECIMAL(20, 8),
    best_ask DECIMAL(20, 8),
    spread DECIMAL(20, 8),
    spread_pct DECIMAL(10, 6),
    
    -- Market metrics
    funding_rate DECIMAL(12, 10),
    open_interest DECIMAL(20, 8),
    volume_24h DECIMAL(20, 8),
    
    -- Liquidity depth (in base currency)
    bid_depth_5pct DECIMAL(20, 8),
    ask_depth_5pct DECIMAL(20, 8),
    total_depth_5pct DECIMAL(20, 8),
    
    bid_depth_10pct DECIMAL(20, 8),
    ask_depth_10pct DECIMAL(20, 8),
    total_depth_10pct DECIMAL(20, 8),
    
    bid_depth_25pct DECIMAL(20, 8),
    ask_depth_25pct DECIMAL(20, 8),
    total_depth_25pct DECIMAL(20, 8),
    
    -- Premium/Impact prices
    premium DECIMAL(12, 10),
    impact_px_bid DECIMAL(20, 8),
    impact_px_ask DECIMAL(20, 8),
    
    -- Metadata
    node_latency_ms INTEGER,
    orderbook_levels INTEGER,
    total_bids INTEGER,
    total_asks INTEGER
);

CREATE INDEX IF NOT EXISTS idx_timestamp ON market_metrics (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_coin_timestamp ON market_metrics (coin, timestamp DESC);

-- Create a hypertable if using TimescaleDB (optional)
-- SELECT create_hypertable('market_metrics', 'timestamp', if_not_exists => TRUE);
"""

class Database:
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.connection = None
        self.lock = threading.Lock()
        
    def connect(self):
        """Create database connection."""
        try:
            self.connection = psycopg2.connect(self.connection_url)
            self.connection.autocommit = True
            logger.info("Database connection established")
            
            # Create schema
            with self.connection.cursor() as cursor:
                cursor.execute(SCHEMA)
                logger.info("Database schema created/verified")
                
            return True
                
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            self.connection = None
            return False
            
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
            finally:
                self.connection = None
                
    def is_connected(self) -> bool:
        """Check if database is connected."""
        if not self.connection:
            return False
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
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
            INSERT INTO market_metrics (
                coin, mark_price, oracle_price, mid_price,
                best_bid, best_ask, spread, spread_pct,
                funding_rate, open_interest, volume_24h,
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
                with self.connection.cursor() as cursor:
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
                            metrics.get('funding_rate'),
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
                
        except Exception as e:
            logger.error(f"Failed to insert market metrics: {e}")
            # Try to reconnect on next attempt
            self.connection = None
            return False
