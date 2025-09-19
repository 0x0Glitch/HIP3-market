import os
from typing import List
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class MarketConfig:
    """Configuration for a single market."""
    symbol: str
    enabled: bool = True
    orderbook_levels: int = 100
    depth_percentages: List[float] = field(default_factory=lambda: [0.05, 0.1, 0.25])


@dataclass
class Config:
    """Configuration class with essential settings only."""
    
    # Essential settings from environment
    database_url: str = field(default_factory=lambda: os.getenv("DATABASE_URL", ""))
    orderbook_ws_url: str = field(default_factory=lambda: os.getenv("ORDERBOOK_WS_URL", "ws://localhost:8000"))
    node_info_url: str = field(default_factory=lambda: os.getenv("NODE_INFO_URL", "http://localhost:3001/info"))
    monitoring_interval: float = field(default_factory=lambda: float(os.getenv("MONITORING_INTERVAL", "2.0")))
    
    # Markets
    markets: List[MarketConfig] = field(default_factory=list)
    
    # Performance settings (hardcoded defaults)
    public_info_url: str = "https://api.hyperliquid.xyz/info"
    request_timeout_ms: int = 2500
    database_pool_size: int = 5
    database_max_overflow: int = 10
    
    # Batch processing settings
    enable_batch_insert: bool = True
    batch_insert_size: int = 10
    batch_flush_interval: float = 5.0
    
    # Concurrency settings
    max_concurrent_markets: int = 10
    max_concurrent_requests: int = 5
    
    # WebSocket settings
    ws_reconnect_delay: float = 1.0
    ws_max_reconnect_delay: float = 30.0
    
    def __post_init__(self):
        """Load markets from environment."""
        markets_env = os.getenv("MARKETS", "LINK")
        market_symbols = [s.strip() for s in markets_env.split(",") if s.strip()]
        
        self.markets = []
        for symbol in market_symbols:
            self.markets.append(MarketConfig(
                symbol=symbol,
                enabled=True,
                orderbook_levels=100
            ))
    
    def get_enabled_markets(self) -> List[MarketConfig]:
        """Get list of enabled markets."""
        return [m for m in self.markets if m.enabled]
    
    def validate(self):
        """Validate configuration."""
        if not self.database_url:
            raise ValueError("DATABASE_URL is required")
        if not self.markets:
            raise ValueError("MARKETS is required")
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        return cls()
