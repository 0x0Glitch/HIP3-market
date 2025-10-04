#!/bin/bash

# Start HIP3 Market Monitoring with Prometheus Metrics
# This script starts both the monitoring system and metrics server

echo "=========================================="
echo "🚀 Starting HIP3 Market Monitoring System"
echo "=========================================="

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
source .venv/bin/activate

# Install requirements if needed
echo "Checking dependencies..."
pip install -q psycopg2-binary aiohttp websockets prometheus-client psutil python-dotenv

# Start metrics server in background
echo "Starting metrics server on port 9090..."
python metrics_server.py &
METRICS_PID=$!
echo "Metrics server PID: $METRICS_PID"

# Wait for metrics server to start
sleep 3

# Check if metrics server is running
if ps -p $METRICS_PID > /dev/null; then
    echo "✅ Metrics server running at http://localhost:9090/metrics"
else
    echo "❌ Failed to start metrics server"
    exit 1
fi

# Start main monitoring system
echo "Starting main monitoring system..."
python multi_market_main.py &
MAIN_PID=$!
echo "Main monitoring PID: $MAIN_PID"

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down..."
    kill $MAIN_PID 2>/dev/null
    kill $METRICS_PID 2>/dev/null
    wait $MAIN_PID 2>/dev/null
    wait $METRICS_PID 2>/dev/null
    echo "✅ Shutdown complete"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Keep script running and show status
echo ""
echo "=========================================="
echo "✅ System Running"
echo "=========================================="
echo "📊 Metrics: http://localhost:9090/metrics"
echo "📈 Grafana: http://localhost:3000 (if docker-compose is running)"
echo ""
echo "Press Ctrl+C to stop..."

# Wait for background processes
wait
