"""Configuration for multi-market monitoring system."""

import os
from typing import List
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database - use DATABASE_URL directly
    DATABASE_URL = os.getenv("DATABASE_URL")

    # Node endpoints
    NODE_INFO_URL = os.getenv("NODE_INFO_URL", "http://localhost:3001/info")
    PUBLIC_INFO_URL = os.getenv("PUBLIC_INFO_URL", "https://api.hyperliquid.xyz/info")

    # WebSocket
    ORDERBOOK_WS_URL = os.getenv("ORDERBOOK_WS_URL", "ws://localhost:8000")

    # Monitoring
    MONITORING_INTERVAL = float(os.getenv("MONITORING_INTERVAL", "0.5"))
    
    # API Rate Limiting
    API_REQUEST_DELAY = float(os.getenv("API_REQUEST_DELAY", "0.2"))  # Delay between API calls in seconds
    
    @property
    def target_markets(self) -> List[str]:
        """Get list of target markets from environment."""
        coin_symbols = os.getenv("COIN_SYMBOL", "LINK")
        return [symbol.strip().upper() for symbol in coin_symbols.split(",") if symbol.strip()]
    
    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL."""
        return self.DATABASE_URL

