"""Configuration for LINK market monitoring system."""

import os
from typing import Optional
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database - prefer DATABASE_URL if provided
    DATABASE_URL = os.getenv("DATABASE_URL")
    
    # Fallback individual components
    DB_HOST = os.getenv("DB_HOST", "localhost")
    try:
        DB_PORT = int(os.getenv("DB_PORT", "5432"))
    except (TypeError, ValueError):
        DB_PORT = 5432
    DB_NAME = os.getenv("DB_NAME", "Markets")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    
    # Node endpoints
    NODE_INFO_URL = os.getenv("NODE_INFO_URL", "http://localhost:3001/info")
    PUBLIC_INFO_URL = os.getenv("PUBLIC_INFO_URL", "https://api.hyperliquid.xyz/info")
    
    # WebSocket
    ORDERBOOK_WS_URL = os.getenv("ORDERBOOK_WS_URL", "ws://localhost:8000")
    
    # Monitoring
    try:
        MONITORING_INTERVAL = int(os.getenv("MONITORING_INTERVAL", "60"))  # seconds
    except (TypeError, ValueError):
        MONITORING_INTERVAL = 60
    COIN_SYMBOL = os.getenv("COIN_SYMBOL", "LINK")
    
    # Timeouts and retries
    REQUEST_TIMEOUT_MS = 250
    MAX_RETRIES = 3
    NODE_STALENESS_THRESHOLD_S = 3
    
    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL."""
        # Use DATABASE_URL if provided, otherwise construct from components
        if self.DATABASE_URL:
            return self.DATABASE_URL
            
        # Fallback: construct from individual components
        encoded_password = quote_plus(self.DB_PASSWORD) if self.DB_PASSWORD else ''
        encoded_user = quote_plus(self.DB_USER) if self.DB_USER else ''
        
        if self.DB_PASSWORD:
            return f"postgresql://{encoded_user}:{encoded_password}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        else:
            return f"postgresql://{encoded_user}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
