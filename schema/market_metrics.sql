-- Schema for market metrics monitoring system
-- This file contains the database schema definitions for the market monitoring system

-- Create market_metrics_testing table
CREATE TABLE IF NOT EXISTS link_metrics_raw (
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
    funding_rate_pct DECIMAL(12, 10),
    open_interest DECIMAL(20, 8),
    volume_24h DECIMAL(20, 8),

    -- Liquidity depth (in USD)
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

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_timestamp ON link_metrics_raw (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_coin_timestamp ON link_metrics_raw (coin, timestamp DESC);

-- Create hypertable if using TimescaleDB (optional)
-- SELECT create_hypertable('market_metrics_testing', 'timestamp', if_not_exists => TRUE);

-- Comments on columns
COMMENT ON TABLE market_metrics_testing IS 'Market metrics for cryptocurrency perpetual futures monitoring';
COMMENT ON COLUMN market_metrics_testing.coin IS 'Symbol of the cryptocurrency (e.g., LINK, BTC)';
COMMENT ON COLUMN market_metrics_testing.mark_price IS 'Mark price from exchange';
COMMENT ON COLUMN market_metrics_testing.oracle_price IS 'Oracle price from external source';
COMMENT ON COLUMN market_metrics_testing.mid_price IS 'Mid price between best bid and ask';
COMMENT ON COLUMN market_metrics_testing.spread IS 'Absolute spread between best bid and ask';
COMMENT ON COLUMN market_metrics_testing.spread_pct IS 'Spread as percentage of mid price';
COMMENT ON COLUMN market_metrics_testing.funding_rate_pct IS 'Funding rate as percentage';
COMMENT ON COLUMN market_metrics_testing.open_interest IS 'Open interest in USD';
COMMENT ON COLUMN market_metrics_testing.volume_24h IS '24-hour trading volume in USD';
COMMENT ON COLUMN market_metrics_testing.bid_depth_5pct IS 'Total bid liquidity within 5% of mid price';
COMMENT ON COLUMN market_metrics_testing.ask_depth_5pct IS 'Total ask liquidity within 5% of mid price';
COMMENT ON COLUMN market_metrics_testing.total_depth_5pct IS 'Total liquidity within 5% of mid price';
COMMENT ON COLUMN market_metrics_testing.premium IS 'Premium between mark and oracle price';
COMMENT ON COLUMN market_metrics_testing.node_latency_ms IS 'Latency to node in milliseconds';
COMMENT ON COLUMN market_metrics_testing.orderbook_levels IS 'Total number of orderbook levels';

-- Function to clean old data (optional, for data retention)
CREATE OR REPLACE FUNCTION cleanup_old_market_metrics()
RETURNS void AS $$
BEGIN
    DELETE FROM link_metrics_raw 
    WHERE timestamp < NOW() - INTERVAL '30 days';
END;
$$ LANGUAGE plpgsql;

-- View for recent metrics
CREATE OR REPLACE VIEW recent_market_metrics AS
SELECT * FROM link_metrics_raw
WHERE timestamp >= NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC;
