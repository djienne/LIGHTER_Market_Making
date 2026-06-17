#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/dashboard"
exec npm run dev -- --port "${PORT:-5174}"
