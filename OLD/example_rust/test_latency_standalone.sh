#!/bin/bash
# StandX API Latency Test Script (Standalone version)
# Designed to run on a fresh Ubuntu 22.04 VPS
# Tests both REST API and WebSocket latency

echo "=========================================="
echo "StandX API Latency Test (Standalone)"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

###########################################
# Step 0: Install Dependencies
###########################################
echo -e "${GREEN}[0/3] Installing dependencies...${NC}"
echo ""

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then
    SUDO="sudo"
else
    SUDO=""
fi

# Update package list
echo "Updating package list..."
$SUDO DEBIAN_FRONTEND=noninteractive apt-get update -qq

# Install curl if not present
if ! command -v curl &> /dev/null; then
    echo "Installing curl..."
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y -qq curl
else
    echo "curl already installed"
fi

# Install Python3 and pip
if ! command -v python3 &> /dev/null; then
    echo "Installing python3..."
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 python3-pip
else
    echo "python3 already installed"
fi

# Ensure pip is available
if ! command -v pip3 &> /dev/null; then
    echo "Installing pip3..."
    $SUDO DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-pip
fi

# Install websockets Python package
echo "Installing websockets Python package..."
pip3 install websockets -q --break-system-packages 2>/dev/null || pip3 install websockets -q 2>/dev/null || python3 -m pip install websockets -q --break-system-packages 2>/dev/null || python3 -m pip install websockets -q 2>/dev/null || true

echo ""
echo -e "${GREEN}Dependencies installed!${NC}"
echo ""

###########################################
# Configuration
###########################################
REST_URL="https://perps.standx.com/api/query_symbol_info?symbol=BTC-USD"
WS_URL="wss://perps.standx.com/ws-stream/v1"
NUM_TESTS=${1:-50}  # Default 50, or pass as argument

echo "Configuration:"
echo "  REST URL: $REST_URL"
echo "  WS URL:   $WS_URL"
echo "  Tests:    $NUM_TESTS"
echo ""

###########################################
# REST API Latency Test
###########################################
echo -e "${GREEN}[1/3] Testing REST API Latency${NC}"
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
echo -e "${GREEN}[2/3] Testing WebSocket Latency${NC}"
echo "Using Python for WebSocket test..."
echo ""

# Create a temporary Python script for WS testing
(python3 - "$NUM_TESTS" << 'PYEOF'
import asyncio
import time
import sys

try:
    import websockets
except ImportError:
    print("  ERROR: websockets module not found")
    sys.exit(1)

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
        # Write results to temp file for bash to read
        with open("/tmp/ws_results.txt", "w") as f:
            f.write(f"{min(times)} {max(times)} {sum(times)//len(times)}")

try:
    asyncio.run(test_ws_latency())
except:
    pass
sys.exit(0)
PYEOF
) || true

# Read results from temp file
if [ -f /tmp/ws_results.txt ]; then
    read ws_min ws_max ws_avg < /tmp/ws_results.txt
    rm -f /tmp/ws_results.txt
else
    ws_avg="N/A"
fi

echo ""

###########################################
# Network Diagnostics
###########################################
echo -e "${GREEN}[3/3] Network Diagnostics${NC}"
echo ""

# DNS lookup test
dns_time=$(curl -s -o /dev/null -w "%{time_namelookup}" "$REST_URL" | awk '{printf "%.0f", $1 * 1000}')
echo "DNS lookup: ${dns_time}ms"

# TCP connect time
tcp_time=$(curl -s -o /dev/null -w "%{time_connect}" "$REST_URL" | awk '{printf "%.0f", $1 * 1000}')
echo "TCP connect: ${tcp_time}ms"

# TLS handshake
tls_time=$(curl -s -o /dev/null -w "%{time_appconnect}" "$REST_URL" | awk '{printf "%.0f", $1 * 1000}')
echo "TLS handshake: ${tls_time}ms"

# Time to first byte
ttfb=$(curl -s -o /dev/null -w "%{time_starttransfer}" "$REST_URL" | awk '{printf "%.0f", $1 * 1000}')
echo "Time to first byte: ${ttfb}ms"

# Server info
echo ""
echo "Server info:"
curl -sI "$REST_URL" 2>/dev/null | grep -i "server\|cf-ray\|x-served-by" || echo "  (no server headers)"

# Traceroute to server (if available)
if command -v traceroute &> /dev/null; then
    echo ""
    echo "Traceroute to perps.standx.com (first 5 hops):"
    traceroute -m 5 -w 1 perps.standx.com 2>/dev/null || echo "  (traceroute failed)"
elif command -v mtr &> /dev/null; then
    echo ""
    echo "MTR to perps.standx.com:"
    mtr -c 1 -r perps.standx.com 2>/dev/null || echo "  (mtr failed)"
fi

# Fetch location info for final report
server_info=$(curl -s https://ipinfo.io 2>/dev/null || echo "{}")
server_ip=$(echo "$server_info" | sed -n 's/.*"ip"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
server_city=$(echo "$server_info" | sed -n 's/.*"city"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
server_region=$(echo "$server_info" | sed -n 's/.*"region"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
server_country=$(echo "$server_info" | sed -n 's/.*"country"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
server_org=$(echo "$server_info" | sed -n 's/.*"org"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
server_ip=${server_ip:-unknown}

# Build location string
if [ -n "$server_city" ] && [ -n "$server_country" ]; then
    location_str="${server_city}, ${server_region:+$server_region, }${server_country}"
elif [ -n "$server_country" ]; then
    location_str="$server_country"
else
    location_str="Unknown"
fi

echo ""
echo ""
echo -e "${GREEN}==========================================${NC}"
echo -e "${GREEN}          FINAL LATENCY REPORT           ${NC}"
echo -e "${GREEN}==========================================${NC}"
echo ""
echo "LOCATION"
echo "  Server IP:  $server_ip"
echo -e "  Geography:  ${RED}${location_str}${NC}"
if [ -n "$server_org" ]; then
    echo "  Provider:   $server_org"
fi
echo ""
echo "------------------------------------------"
echo ""
echo "REST API LATENCY"
echo "  Min:  ${rest_min} ms"
echo "  Max:  ${rest_max} ms"
echo "  Avg:  ${rest_avg} ms"
echo ""
echo "------------------------------------------"
echo ""
echo "WEBSOCKET LATENCY (connect + first message)"
if [ -n "$ws_avg" ] && [ "$ws_avg" != "N/A" ]; then
    echo "  Min:  ${ws_min} ms"
    echo "  Max:  ${ws_max} ms"
    echo "  Avg:  ${ws_avg} ms"
else
    echo "  (test failed or unavailable)"
fi
echo ""
echo "------------------------------------------"
echo ""
echo "NETWORK BREAKDOWN"
echo "  DNS Lookup:    ${dns_time} ms"
echo "  TCP Connect:   ${tcp_time} ms"
echo "  TLS Handshake: ${tls_time} ms"
echo "  Time to First Byte: ${ttfb} ms"
echo ""
echo -e "${GREEN}==========================================${NC}"
echo ""
