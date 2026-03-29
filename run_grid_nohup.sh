#!/usr/bin/env bash
# Start grid dry-run in background (survives SSH disconnect)
# Usage: bash run_grid_nohup.sh [CONFIG]  (default: grid_config.json)
set -euo pipefail

CONFIG="${1:-grid_config.json}"
SYMBOL="${MARKET_SYMBOL:-BTC}"
LOG_DIR="logs"

mkdir -p "$LOG_DIR"

# Build Cython if not compiled
if ! python -c "import _vol_obi_fast" 2>/dev/null; then
    echo "Building Cython extension..."
    python setup_cython.py build_ext --inplace
fi

# Check not already running
if pgrep -f "market_maker_v2.py.*--grid" > /dev/null 2>&1; then
    echo "Grid is already running:"
    ps aux | grep "market_maker_v2.py.*--grid" | grep -v grep
    echo "Kill it first or use run_grid_3d.sh for monitored mode."
    exit 1
fi

echo "Starting grid dry-run (symbol=$SYMBOL, config=$CONFIG)..."
nohup python -u market_maker_v2.py --symbol "$SYMBOL" --grid "$CONFIG" \
    > "$LOG_DIR/grid_console.log" 2>&1 &
PID=$!
echo "Started (PID $PID). Logs:"
echo "  Console:  tail -f $LOG_DIR/grid_console.log"
echo "  Debug:    tail -f $LOG_DIR/grid_debug.log"
echo "  Summary:  tail -80 $LOG_DIR/grid/summary.log"
echo "  Results:  python check_grid_results.py"
echo "  Stop:     kill $PID"
