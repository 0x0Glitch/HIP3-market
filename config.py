"""Configuration for LINK market monitoring system."""

import os
from typing import Optional
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
    MONITORING_INTERVAL = float(os.getenv("MONITORING_INTERVAL", "60.0"))
    COIN_SYMBOL = os.getenv("COIN_SYMBOL", "LINK")

    # Timeouts and retries
    REQUEST_TIMEOUT_MS = 250
    MAX_RETRIES = 3
    NODE_STALENESS_THRESHOLD_S = 30

    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL."""
        return self.DATABASE_URL