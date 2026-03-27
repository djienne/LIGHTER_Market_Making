#!/usr/bin/env bash
# Dry-run monitor: checks bot health every hour, logs to logs/monitor.log
# Runs for 3 days (72 checks). Auto-restarts bot if it crashes.
# Usage: run inside tmux session "mm_monitor"

set -uo pipefail
# Note: no set -e — grep returning 0 matches (exit 1) must not kill the script
cd /mnt/c/Users/david/Desktop/freqtrade/lighter_MM

LOG="logs/monitor.log"
BOT_SESSION="mm_dryrun"
MAX_CHECKS=72
CHECK_INTERVAL=3600  # 1 hour

mkdir -p logs

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"
}

is_bot_running() {
    tmux has-session -t "$BOT_SESSION" 2>/dev/null
}

restart_bot() {
    log "RESTART: launching bot in tmux session $BOT_SESSION"
    tmux new-session -d -s "$BOT_SESSION" -c "$(pwd)" \
        "python -u market_maker_v2.py --symbol BTC 2>&1 | tee -a logs/dry_run_console.log"
    sleep 5
    if is_bot_running; then
        log "RESTART: bot started successfully"
    else
        log "RESTART: FAILED to start bot!"
    fi
}

log "========================================"
log "MONITOR STARTED — $MAX_CHECKS checks, every ${CHECK_INTERVAL}s"
log "========================================"

for ((i=1; i<=MAX_CHECKS; i++)); do
    log "--- CHECK $i/$MAX_CHECKS ---"

    # 1. Is the bot alive?
    if is_bot_running; then
        log "STATUS: bot tmux session ALIVE"
    else
        log "STATUS: bot tmux session DEAD — attempting restart"
        # Grab last 20 lines before restart for crash diagnosis
        log "CRASH LOG (last 20 lines):"
        tail -20 logs/dry_run_console.log 2>/dev/null | while read -r line; do
            log "  $line"
        done
        restart_bot
        # After restart, wait for warmup before next check
        log "Waiting for warmup (sleeping 15 min before next check)"
        sleep 900
        continue
    fi

    # 2. State snapshot
    if [ -f logs/dry_run_state.json ]; then
        STATE=$(cat logs/dry_run_state.json)
        CAPITAL=$(echo "$STATE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'capital=\${d[\"available_capital\"]:.2f} pos={d[\"position\"]:.6f} pnl=\${d[\"realized_pnl\"]:.4f} fills={d[\"fill_count\"]} vol=\${d[\"total_volume\"]:.2f} pv=\${d[\"portfolio_value\"]:.2f}')" 2>/dev/null || echo "PARSE ERROR")
        log "STATE: $CAPITAL"
    else
        log "STATE: no state file found"
    fi

    # 3. Recent fills count
    TOTAL_FILLS=$(grep -c "DRY-RUN FILLED\|DRY-RUN PARTIAL" logs/dry_run_console.log 2>/dev/null || true)
    TOTAL_FILLS=${TOTAL_FILLS:-0}
    log "FILLS: $TOTAL_FILLS total in log"

    # 4. Trade log line count
    TRADE_LINES=$(wc -l < logs/trades_BTC.csv 2>/dev/null || echo "0")
    log "TRADES CSV: $TRADE_LINES lines"

    # 5. Recent errors/warnings
    RECENT_ERRORS=$(grep -i "error\|exception\|traceback" logs/dry_run_console.log 2>/dev/null | tail -5 || true)
    if [ -n "$RECENT_ERRORS" ]; then
        log "ERRORS (last 5):"
        echo "$RECENT_ERRORS" | while read -r line; do
            log "  $line"
        done
    else
        log "ERRORS: none"
    fi

    # 6. Last quoting line (proves loop is active)
    LAST_QUOTE=$(grep "QUOTING" logs/dry_run_console.log 2>/dev/null | tail -1 || true)
    if [ -n "$LAST_QUOTE" ]; then
        log "LAST QUOTE: $LAST_QUOTE"
    else
        log "LAST QUOTE: none found (may still be in warmup)"
    fi

    # 7. Last summary line
    LAST_SUMMARY=$(grep "DRY-RUN SUMMARY" logs/dry_run_console.log 2>/dev/null | tail -1 || true)
    if [ -n "$LAST_SUMMARY" ]; then
        log "LAST SUMMARY: $LAST_SUMMARY"
    fi

    # 8. Check if quoting is stale (last quote > 10 min old)
    if [ -n "$LAST_QUOTE" ]; then
        QUOTE_TIME=$(echo "$LAST_QUOTE" | grep -oP '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}' || true)
        if [ -n "$QUOTE_TIME" ]; then
            QUOTE_EPOCH=$(date -d "$QUOTE_TIME" +%s 2>/dev/null || echo "0")
            NOW_EPOCH=$(date +%s)
            STALE_SEC=$(( NOW_EPOCH - QUOTE_EPOCH ))
            if [ "$STALE_SEC" -gt 600 ]; then
                log "WARNING: last quote is ${STALE_SEC}s old — bot may be stuck!"
            fi
        fi
    fi

    log "--- END CHECK $i ---"
    log ""

    # Sleep until next check (unless this is the last one)
    if [ "$i" -lt "$MAX_CHECKS" ]; then
        sleep "$CHECK_INTERVAL"
    fi
done

log "========================================"
log "MONITOR COMPLETE — $MAX_CHECKS checks done"
log "========================================"
