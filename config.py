import os
from typing import List
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Simple configuration class."""
    
    def __init__(self):
        # Essential settings only
        self.database_url = os.getenv("DATABASE_URL", "")
        self.orderbook_ws_url = os.getenv("ORDERBOOK_WS_URL", "ws://localhost:8000")
        self.node_info_url = os.getenv("NODE_INFO_URL", "http://localhost:3001/info")
        self.monitoring_interval = float(os.getenv("MONITORING_INTERVAL", "2.0"))
        
        # Load markets
        markets_env = os.getenv("MARKETS", "LINK")
        self.markets = [s.strip() for s in markets_env.split(",") if s.strip()]
        
        # Fixed defaults
        self.public_info_url = "https://api.hyperliquid.xyz/info"
        self.request_timeout_ms = 2500
    
    def validate(self):
        """Validate configuration."""
        if not self.database_url:
            raise ValueError("DATABASE_URL is required")
        if not self.markets:
            raise ValueError("MARKETS is required")
