#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
export LIGHTER_MM_CONFIG="${LIGHTER_MM_CONFIG:-$PWD/configs/btc_live_100.json}"
exec "$PWD/venv/bin/python" -u market_maker_v2.py --symbol BTC --live
