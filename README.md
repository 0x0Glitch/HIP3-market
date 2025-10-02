# LINK Perpetual Market Monitoring System (v1.1.0)

An optimized monitoring system for Hyperliquid LINK perpetual market that tracks real-time market metrics and liquidity depth with persistent connections and data freshness guarantees.

## Features

- **Real-time Market Data**: Fetches mark price, oracle price, funding rate, open interest, and 24h volume
- **Liquidity Depth Analysis**: Calculates bid/ask depth at 5%, 10%, and 25% price levels from L4 orderbook
- **Persistent WebSocket Connection**: Maintains persistent WebSocket connection with automatic reconnection
- **Data Freshness Guarantee**: Ensures data is fresh based on monitoring interval (configurable)
- **Connection Pooling**: PostgreSQL connection pooling for better performance and reliability
- **Dual Data Sources**: Uses local Hyperliquid node for low latency with public API fallback
- **Robust Error Handling**: Never terminates automatically, only stops on Ctrl+C
- **PostgreSQL Storage**: Stores all metrics with timestamps for historical analysis
- **Industry Standards**: SQL schema in separate file, proper module structure, type hints

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Local Node    │    │ OrderBook Server │    │   PostgreSQL    │
│ (port 3001)     │◄──►│ (port 8000)      │    │   Database      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         ▲                       ▲                       ▲
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────────┐
                    │ LINK Market Monitor │
                    │   (main.py)         │
                    └─────────────────────┘
```

## Setup

### 1. Prerequisites

- Python 3.10+
- PostgreSQL database
- Hyperliquid non-validating node running on port 3001
- Local orderbook server running on port 8000

### 2. Install Dependencies

```bash
cd link-market
uv sync
```

### 3. Configure Environment

Copy the example environment file and configure:

```bash
cp .env.example .env
```

Edit `.env` with your database connection details:

```bash
# Database Configuration (recommended: use DATABASE_URL)
DATABASE_URL=postgresql://postgres:your_password_here@localhost:5432/link_market_monitoring

# Node Configuration
NODE_INFO_URL=http://localhost:3001/info
PUBLIC_INFO_URL=https://api.hyperliquid.xyz/info

# WebSocket Configuration
ORDERBOOK_WS_URL=ws://localhost:8000

# Monitoring Configuration
MONITORING_INTERVAL=2.0  # seconds (supports float for sub-second intervals)
COIN_SYMBOL=LINK
```

**Database Configuration Options:**

- **Option 1 (Recommended)**: Use `DATABASE_URL` - single connection string
- **Option 2**: Use individual components (`DB_HOST`, `DB_PORT`, etc.) - see `.env.example` for details

### 4. Database Setup

Create the PostgreSQL database:

```sql
CREATE DATABASE link_market_monitoring;
```

The schema will be created automatically when you first run the application.

### 5. Start Hyperliquid Infrastructure

Ensure you have:

1. **Hyperliquid Node**: Running on port 3001 with `/info` endpoint available
2. **OrderBook Server**: Running on port 8000 with L4 book WebSocket support

Start the orderbook server:

```bash
cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000
```

## Usage

### Run the Monitor

```bash
python main.py
```

The system will:

1. Connect to database and create schema if needed
2. Connect to node `/info` endpoint and orderbook WebSocket
3. Begin monitoring every 60 seconds (configurable)
4. Log comprehensive market summaries
5. Store all metrics to PostgreSQL

### Sample Output

```
2025-01-09 12:34:56 - __main__ - INFO - LINK Market Summary:
  Mark Price: $23.45
  Oracle Price: $23.44
  Mid Price: $23.445
  Spread: $0.01 (0.043%)
  Funding Rate: 0.0001
  Open Interest: 1234567.8
  24h Volume: $98765432.1
  5% Depth: 45678.9 LINK
  10% Depth: 87654.3 LINK
  25% Depth: 123456.7 LINK
```

### Graceful Shutdown

Press `Ctrl+C` to stop the monitoring system. It will clean up connections and shut down gracefully.

## Database Schema

The system creates a `market_metrics` table with the following structure:

```sql
CREATE TABLE market_metrics (
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

    -- Liquidity depth (5%, 10%, 25%)
    bid_depth_5pct DECIMAL(20, 8),
    ask_depth_5pct DECIMAL(20, 8),
    total_depth_5pct DECIMAL(20, 8),
    -- ... and 10%, 25% variants

    -- Premium/Impact prices
    premium DECIMAL(12, 10),
    impact_px_bid DECIMAL(20, 8),
    impact_px_ask DECIMAL(20, 8),

    -- Metadata
    node_latency_ms INTEGER,
    orderbook_levels INTEGER
);
```

## Error Handling

The system is designed to **never terminate automatically**:

- **Database errors**: Logs error, attempts reconnection, continues monitoring
- **Network errors**: Logs error, retries next iteration
- **WebSocket errors**: Logs error, attempts reconnection, continues with market data only
- **Node staleness**: Automatically switches to public API fallback
- **Consecutive failures**: After 10 consecutive failures, adds 30-second delay but continues

Only `Ctrl+C` (SIGINT) or `kill` (SIGTERM) will stop the system.

## Monitoring Frequency

Default: 60 seconds (configurable via `MONITORING_INTERVAL`)

Each iteration:

1. Fetches market data from `/info` endpoint (local node or public API)
2. Fetches L4 orderbook snapshot via WebSocket
3. Calculates liquidity depth at 5%, 10%, 25% levels
4. Stores complete metrics to database
5. Logs human-readable summary

## Troubleshooting

### Common Issues

1. **Database connection fails**:

   - Check PostgreSQL is running
   - Verify connection string in `.env`
   - Check database permissions

2. **OrderBook WebSocket fails**:

   - Ensure orderbook server is running on port 8000
   - Check if Hyperliquid node is writing L4 data
   - Monitor will continue with market data only

3. **Node endpoint unreachable**:
   - System automatically falls back to public API
   - Check node is running on port 3001
   - Verify `/info` endpoint responds

### Logs

The system provides detailed logging:

- **INFO**: Normal operation, market summaries
- **WARNING**: Non-critical issues, fallbacks
- **ERROR**: Failed operations, will retry
- **DEBUG**: Detailed data for troubleshooting

## License

This monitoring system is provided "as is" for educational and research purposes.
