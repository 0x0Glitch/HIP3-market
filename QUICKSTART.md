# Multi-Market Monitor - Quick Start

## 🚀 Installation & Setup

### 1. Install Dependencies
```bash
# Install dependencies using uv
uv sync

# Or install in development mode
uv sync --dev
```

### 2. Configure Environment
```bash
# Check your .env file (already configured):
cat .env

# Should contain:
DATABASE_URL=postgresql://localhost:5432/market_metrics
ORDERBOOK_WS_URL=ws://localhost:8000/ws
NODE_INFO_URL=http://localhost:3001/info
MONITORING_INTERVAL=1.0
COIN_SYMBOL=LINK,ASTER,XPL
```

### 3. Database Setup
```bash
# Create PostgreSQL database
createdb market_metrics

# Or connect to your existing database and ensure it exists
```

## 🎯 Running the System

### Method 1: Direct Python Execution
```bash
# Run with uv
uv run python multi_market_main.py

# Or activate the virtual environment first
source .venv/bin/activate  # Linux/Mac
python multi_market_main.py
```

### Method 2: Using the Installed Script
```bash
# Install the package first
uv pip install -e .

# Then run the script
multi-market-monitor
```

## 🔄 Adding/Removing Markets (Hot-Reload)

### Add Markets
```bash
# Edit .env file
echo "COIN_SYMBOL=LINK,ASTER,XPL,ETH,BTC" > .env

# System will detect changes within 10 seconds and start monitoring new markets
# Watch the logs for: "🔄 Hot-reload: Added markets: ETH, BTC"
```

### Remove Markets  
```bash
# Edit .env file
echo "COIN_SYMBOL=LINK,ASTER" > .env

# System will stop monitoring removed markets
# Watch the logs for: "🔄 Hot-reload: Removed markets: XPL"
```

## 📊 Expected Output

```
🚀 Setting up Multi-Market Monitor...
Target Markets: LINK, ASTER, XPL
✓ Created/verified table: market_metrics.link_metrics_raw
✓ Created/verified table: market_metrics.aster_metrics_raw  
✓ Created/verified table: market_metrics.xpl_metrics_raw
✅ Started monitoring LINK
✅ Started monitoring ASTER
✅ Started monitoring XPL
📊 LINK: $11.2845 - Data inserted
📊 ASTER: $0.0756 - Data inserted
📊 XPL: $0.4523 - Data inserted
```

## 🛑 Stopping the System

```bash
# Graceful shutdown with Ctrl+C
# System will:
# - Stop all market monitors
# - Close WebSocket connections
# - Disconnect from database
# - Clean up resources

# Expected shutdown output:
🛑 Stopping Multi-Market Monitor...
🛑 Stopping LINK monitor
🛑 Stopping ASTER monitor  
🛑 Stopping XPL monitor
✅ Multi-Market Monitor stopped gracefully
```

## 🔍 Database Queries

```sql
-- View latest data for LINK
SELECT * FROM market_metrics.link_metrics_raw 
ORDER BY timestamp DESC LIMIT 10;

-- View latest data for all markets
SELECT 'LINK' as market, mark_price, timestamp 
FROM market_metrics.link_metrics_raw ORDER BY timestamp DESC LIMIT 1
UNION ALL
SELECT 'ASTER' as market, mark_price, timestamp  
FROM market_metrics.aster_metrics_raw ORDER BY timestamp DESC LIMIT 1
UNION ALL
SELECT 'XPL' as market, mark_price, timestamp
FROM market_metrics.xpl_metrics_raw ORDER BY timestamp DESC LIMIT 1;
```

## ⚠️ Prerequisites

1. **PostgreSQL database** running and accessible
2. **OrderBook WebSocket server** running on `ws://localhost:8000/ws`
3. **Hyperliquid node** accessible at `http://localhost:3001/info`
4. **Python 3.10+** installed
5. **uv package manager** installed

## 🐛 Troubleshooting

- **Database connection failed**: Check DATABASE_URL and ensure PostgreSQL is running
- **WebSocket connection failed**: Ensure your orderbook server is running
- **No market data**: Check if your Hyperliquid node is accessible
- **Hot-reload not working**: Check .env file permissions and format

## 🎉 Success Indicators

✅ Multiple markets monitored concurrently  
✅ Hot-reload working (add markets without restart)  
✅ Database tables created automatically  
✅ Graceful shutdown on Ctrl+C  
✅ Independent error handling per market  
✅ Atomic database operations per market  

The system is now ready for production use! 🚀
