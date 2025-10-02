"""Configuration for multi-market monitoring system."""

import os
from typing import Optional, List, Set
from urllib.parse import quote_plus
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)

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
    
    def __init__(self):
        self._cached_target_markets = None
    
    # Multi-market support
    @property
    def target_markets(self) -> List[str]:
        """Get list of target markets from environment."""
        if self._cached_target_markets is not None:
            return self._cached_target_markets
        coin_symbols = os.getenv("COIN_SYMBOL", "LINK")
        return [symbol.strip().upper() for symbol in coin_symbols.split(",") if symbol.strip()]
    
    @property
    def coin_symbol(self) -> str:
        """Backward compatibility - returns first market."""
        return self.target_markets[0] if self.target_markets else "LINK"
    
    @property  
    def COIN_SYMBOL(self) -> str:
        """Backward compatibility - returns first market."""
        return self.coin_symbol

    # Timeouts and retries
    REQUEST_TIMEOUT_MS = 250
    MAX_RETRIES = 3
    NODE_STALENESS_THRESHOLD_S = 30

    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL."""
        return self.DATABASE_URL
    
    def reload_markets(self) -> Set[str]:
        """Reload markets from environment and return newly added markets."""
        current_markets = set(self.target_markets)
        
        # Reload from environment
        coin_symbols = os.getenv("COIN_SYMBOL", "LINK")
        new_markets = [symbol.strip().upper() for symbol in coin_symbols.split(",") if symbol.strip()]
        
        added_markets = set(new_markets) - current_markets
        removed_markets = current_markets - set(new_markets)
        
        # Update the cached markets list (need to invalidate the property cache)
        self._cached_target_markets = new_markets
        
        if added_markets:
            logger.info(f"🔄 Hot-reload: Added markets: {', '.join(added_markets)}")
        if removed_markets:
            logger.info(f"🔄 Hot-reload: Removed markets: {', '.join(removed_markets)}")
            
        return added_markets