"""Simple database operations."""

import psycopg2
from psycopg2 import extras
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import os

logger = logging.getLogger(__name__)


class Database:
    """Simple database manager."""
    
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.connection = None
        
    def connect(self) -> bool:
        """Connect to database."""
        try:
            self.connection = psycopg2.connect(self.connection_url)
            self.connection.autocommit = True
            logger.info("Database connected")
            
            # Load and execute schema
            schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
            with open(schema_path, 'r') as f:
                schema = f.read()
            
            with self.connection.cursor() as cursor:
                cursor.execute(schema)
                logger.info("Database schema loaded")
            
            return True
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def insert_metric(self, metric: Dict[str, Any]) -> bool:
        """Insert metric into database."""
        if not metric.get('coin'):
            return False
            
        if not self.connection:
            return False
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
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
                """, (
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
                ))
                return True
                
        except Exception as e:
            logger.error(f"Insert failed: {e}")
            return False
    
    def close(self):
        """Close connection."""
        if self.connection:
            self.connection.close()
