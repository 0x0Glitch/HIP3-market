-- Market metrics table
CREATE TABLE IF NOT EXISTS market_metrics (
    id BIGSERIAL PRIMARY KEY,
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
    funding_rate_pct DECIMAL(12, 10),
    open_interest DECIMAL(20, 8),
    volume_24h DECIMAL(20, 8),
    
    -- Liquidity depth (USD value)
    bid_depth_5pct DECIMAL(20, 8),
    ask_depth_5pct DECIMAL(20, 8),
    total_depth_5pct DECIMAL(20, 8),
    
    bid_depth_10pct DECIMAL(20, 8),
    ask_depth_10pct DECIMAL(20, 8),
    total_depth_10pct DECIMAL(20, 8),
    
    bid_depth_25pct DECIMAL(20, 8),
    ask_depth_25pct DECIMAL(20, 8),
    total_depth_25pct DECIMAL(20, 8),
    
    -- Premium/Impact
    premium DECIMAL(12, 10),
    impact_px_bid DECIMAL(20, 8),
    impact_px_ask DECIMAL(20, 8),
    
    -- Performance metadata
    node_latency_ms INTEGER,
    ws_latency_ms INTEGER,
    orderbook_levels INTEGER,
    total_bids INTEGER,
    total_asks INTEGER
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_timestamp ON market_metrics (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_coin_timestamp ON market_metrics (coin, timestamp DESC);
