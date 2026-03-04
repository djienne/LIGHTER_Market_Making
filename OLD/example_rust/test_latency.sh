#!/bin/bash
# StandX API Latency Test Script
# Tests both REST API and WebSocket latency

set -e

# Configuration
REST_URL="https://perps.standx.com/api/query_symbol_info?symbol=BTC-USD"
WS_URL="wss://perps.standx.com/ws-stream/v1"
NUM_TESTS=${1:-50}  # Default 50, or pass as argument

echo "=========================================="
echo "StandX API Latency Test"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

###########################################
# REST API Latency Test
###########################################
echo -e "${GREEN}[1/2] Testing REST API Latency${NC}"
echo "URL: $REST_URL"
echo "Running $NUM_TESTS requests..."
echo ""

rest_times=()
for i in $(seq 1 $NUM_TESTS); do
    # Use curl with timing, capture only time_total
    time_ms=$(curl -s -o /dev/null -w "%{time_total}" "$REST_URL" | awk '{printf "%.0f", $1 * 1000}')
    rest_times+=($time_ms)
    echo "  Request $i: ${time_ms}ms"
    sleep 0.1
done

# Calculate REST stats
rest_sum=0
rest_min=${rest_times[0]}
rest_max=${rest_times[0]}
for t in "${rest_times[@]}"; do
    rest_sum=$((rest_sum + t))
    ((t < rest_min)) && rest_min=$t
    ((t > rest_max)) && rest_max=$t
done
rest_avg=$((rest_sum / NUM_TESTS))

echo ""
echo "REST API Results:"
echo "  Min: ${rest_min}ms"
echo "  Max: ${rest_max}ms"
echo "  Avg: ${rest_avg}ms"
echo ""

###########################################
# WebSocket Latency Test
###########################################
echo -e "${GREEN}[2/2] Testing WebSocket Latency${NC}"
echo "URL: $WS_URL"
echo ""

# Check if websocat is available, otherwise use Python
if command -v websocat &> /dev/null; then
    echo "Using websocat for WebSocket test..."
    echo ""

    ws_times=()
    for i in $(seq 1 $NUM_TESTS); do
        start_time=$(date +%s%3N)
        timeout 5 websocat -n1 "$WS_URL" <<< '{"method":"subscribe","params":["depth_book.update.BTC-USD"]}' > /dev/null 2>&1 || true
        end_time=$(date +%s%3N)
        time_ms=$((end_time - start_time))
        ws_times+=($time_ms)
        echo "  Connection $i: ${time_ms}ms"
        sleep 0.2
    done

    # Calculate WS stats
    ws_sum=0
    ws_min=${ws_times[0]}
    ws_max=${ws_times[0]}
    for t in "${ws_times[@]}"; do
        ws_sum=$((ws_sum + t))
        ((t < ws_min)) && ws_min=$t
        ((t > ws_max)) && ws_max=$t
    done
    ws_avg=$((ws_sum / NUM_TESTS))

    echo ""
    echo "WebSocket Results:"
    echo "  Min: ${ws_min}ms"
    echo "  Max: ${ws_max}ms"
    echo "  Avg: ${ws_avg}ms"

elif command -v python3 &> /dev/null; then
    echo "Using Python for WebSocket test..."
    echo ""

    # Create a temporary Python script for WS testing
    python3 - "$NUM_TESTS" << 'PYEOF'
import asyncio
import time
import sys

try:
    import websockets
except ImportError:
    print("  websockets module not found, installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets", "-q"])
    import websockets

WS_URL = "wss://perps.standx.com/ws-stream/v1"
NUM_TESTS = int(sys.argv[1]) if len(sys.argv) > 1 else 50

async def test_ws_latency():
    times = []
    for i in range(NUM_TESTS):
        start = time.time()
        try:
            async with websockets.connect(WS_URL, close_timeout=2) as ws:
                # Send subscribe
                await ws.send('{"method":"subscribe","params":["depth_book.update.BTC-USD"]}')
                # Wait for first message
                msg = await asyncio.wait_for(ws.recv(), timeout=3)
                end = time.time()
                latency_ms = int((end - start) * 1000)
                times.append(latency_ms)
                print(f"  Connection {i+1}: {latency_ms}ms")
        except Exception as e:
            print(f"  Connection {i+1}: ERROR - {e}")
        await asyncio.sleep(0.2)

    if times:
        print(f"\nWebSocket Results:")
        print(f"  Min: {min(times)}ms")
        print(f"  Max: {max(times)}ms")
        print(f"  Avg: {sum(times)//len(times)}ms")
        # Write avg to temp file for bash to read
        with open("/tmp/ws_avg.txt", "w") as f:
            f.write(str(sum(times)//len(times)))

asyncio.run(test_ws_latency())
PYEOF

    # Read the avg from temp file
    if [ -f /tmp/ws_avg.txt ]; then
        ws_avg=$(cat /tmp/ws_avg.txt)
        rm -f /tmp/ws_avg.txt
    fi

else
    echo -e "${YELLOW}Neither websocat nor python3 found. Skipping WebSocket test.${NC}"
    echo "Install websocat: cargo install websocat"
    echo "Or ensure python3 is available with: pip install websockets"
    ws_avg="N/A"
fi

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo "REST API avg latency: ${rest_avg}ms"
if [ -n "$ws_avg" ] && [ "$ws_avg" != "N/A" ]; then
    echo "WebSocket avg latency: ${ws_avg}ms"
fi
echo ""

# DNS lookup test
echo -e "${GREEN}[Bonus] DNS Lookup Time${NC}"
dns_time=$(curl -s -o /dev/null -w "%{time_namelookup}" "$REST_URL" | awk '{printf "%.0f", $1 * 1000}')
echo "DNS lookup: ${dns_time}ms"

# TLS handshake
tls_time=$(curl -s -o /dev/null -w "%{time_appconnect}" "$REST_URL" | awk '{printf "%.0f", $1 * 1000}')
echo "TLS handshake: ${tls_time}ms"

echo ""
echo "Done!"
