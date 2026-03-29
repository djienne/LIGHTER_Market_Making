#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# 3-day grid dry-run with hourly monitoring and auto-restart
# Usage:  bash run_grid_3d.sh
# Stop:   kill $(cat logs/grid_monitor.pid)
# ──────────────────────────────────────────────────────────────
set -euo pipefail

SYMBOL="BTC"
DURATION_SEC=$((3 * 24 * 3600))
CHECK_INTERVAL=3600
MAX_RESTARTS=50
WARMUP_SEC=660
LOG_DIR="logs"
GRID_DIR="$LOG_DIR/grid"
CONSOLE_LOG="$LOG_DIR/grid_console.log"
DEBUG_LOG="$LOG_DIR/grid_debug.log"
MONITOR_LOG="$LOG_DIR/grid_monitor.log"
PID_FILE="$LOG_DIR/grid_monitor.pid"
GRID_CONFIG="grid_config.json"

mkdir -p "$LOG_DIR" "$GRID_DIR"
echo $$ > "$PID_FILE"

START_TS=$(date +%s)
END_TS=$((START_TS + DURATION_SEC))
RESTART_COUNT=0

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$MONITOR_LOG"; }

cleanup() {
    log "Monitor shutting down."
    if [[ -n "${BOT_PID:-}" ]] && kill -0 "$BOT_PID" 2>/dev/null; then
        log "Sending SIGTERM to grid bot (PID $BOT_PID)..."
        kill -TERM "$BOT_PID" 2>/dev/null || true
        wait "$BOT_PID" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    log "Done."
}
trap cleanup EXIT INT TERM

start_bot() {
    log "Starting grid dry-run (symbol=$SYMBOL, config=$GRID_CONFIG)..."
    python -u market_maker_v2.py --symbol "$SYMBOL" --grid "$GRID_CONFIG" \
        >> "$CONSOLE_LOG" 2>&1 &
    BOT_PID=$!
    log "Grid bot started (PID $BOT_PID)."
}

check_health() {
    local ok=true

    # 1) Process alive?
    if ! kill -0 "$BOT_PID" 2>/dev/null; then
        log "ERROR: Grid bot process $BOT_PID is dead!"
        ok=false
    fi

    # 2) Read latest grid summary
    if [[ -f "$GRID_DIR/summary.log" ]]; then
        local last_summary
        last_summary=$(grep "^GRID SUMMARY" "$GRID_DIR/summary.log" | tail -1)
        log "SUMMARY: $last_summary"

        # Best slot
        local best
        best=$(grep "^Best:" "$GRID_DIR/summary.log" | tail -1)
        if [[ -n "$best" ]]; then
            log "$best"
        fi

        # Total fills across all slots
        local total_fills
        total_fills=$(grep -oP 'Fills.*?(\d+)' "$GRID_DIR/summary.log" | tail -64 | awk -F'|' '{s+=$1} END{print s+0}')
        # Simpler: count FILL events in debug log
        total_fills=$(grep -c "FILL" "$DEBUG_LOG" 2>/dev/null || echo 0)
        log "Total fill events: $total_fills"
    else
        log "WARNING: No summary file yet"
    fi

    # 3) Check for critical errors in debug log (last 100 lines)
    if [[ -f "$DEBUG_LOG" ]]; then
        local recent_errors
        recent_errors=$(tail -100 "$DEBUG_LOG" | grep -ciE 'CRITICAL|FATAL|Traceback' || true)
        if [[ "$recent_errors" -gt 0 ]]; then
            log "WARNING: $recent_errors critical errors in debug log (last 100 lines)"
            tail -100 "$DEBUG_LOG" | grep -iE 'CRITICAL|FATAL|Traceback' | tail -5 | while IFS= read -r line; do log "  ERR> $line"; done
        fi
    fi

    # 4) Check debug log freshness (should update every ~60s from summaries)
    if [[ -f "$DEBUG_LOG" ]]; then
        local file_age
        file_age=$(( $(date +%s) - $(stat -c %Y "$DEBUG_LOG" 2>/dev/null || echo 0) ))
        if [[ "$file_age" -gt 600 ]]; then
            log "WARNING: Debug log is ${file_age}s old (>10min) — bot may be stuck"
            ok=false
        fi
    fi

    # 5) Count state files (should be 64)
    local state_count
    state_count=$(ls "$GRID_DIR"/state_*.json 2>/dev/null | wc -l)
    log "State files: $state_count/64"

    $ok
}

log "=========================================="
log "3-DAY GRID DRY RUN — started at $(date)"
log "End time: $(date -d @$END_TS)"
log "Check interval: ${CHECK_INTERVAL}s (1 hour)"
log "Grid config: $GRID_CONFIG"
log "=========================================="

start_bot

log "Warmup: waiting ${WARMUP_SEC}s for bot to initialize..."
sleep "$WARMUP_SEC"

if ! kill -0 "$BOT_PID" 2>/dev/null; then
    log "ERROR: Bot died during warmup! Stderr:"
    tail -30 "$CONSOLE_LOG" 2>/dev/null | while IFS= read -r line; do log "  $line"; done
    exit 1
fi
log "Warmup complete — bot alive (PID $BOT_PID)."

while true; do
    NOW=$(date +%s)
    if [[ "$NOW" -ge "$END_TS" ]]; then
        log "3-day duration reached. Stopping."
        break
    fi

    REMAINING=$(( (END_TS - NOW) / 3600 ))
    log "--- Hourly check ($REMAINING hours remaining) ---"

    if check_health; then
        log "OK: Grid bot healthy."
    else
        log "Grid bot needs restart (restart #$((RESTART_COUNT + 1)))."
        if kill -0 "$BOT_PID" 2>/dev/null; then
            kill -TERM "$BOT_PID" 2>/dev/null || true
            sleep 5
            kill -9 "$BOT_PID" 2>/dev/null || true
        fi
        wait "$BOT_PID" 2>/dev/null || true

        RESTART_COUNT=$((RESTART_COUNT + 1))
        if [[ "$RESTART_COUNT" -ge "$MAX_RESTARTS" ]]; then
            log "FATAL: Hit max restart cap ($MAX_RESTARTS). Giving up."
            exit 1
        fi

        log "Waiting 10s before restart..."
        sleep 10
        start_bot
        log "Warmup: waiting ${WARMUP_SEC}s..."
        sleep "$WARMUP_SEC"
        if ! kill -0 "$BOT_PID" 2>/dev/null; then
            log "ERROR: Bot died during warmup after restart!"
            continue
        fi
        log "Warmup complete after restart."
    fi

    # Sleep until next check, wake early if bot dies
    for ((i=0; i<CHECK_INTERVAL; i+=30)); do
        if ! kill -0 "$BOT_PID" 2>/dev/null; then
            log "Bot died between checks! Triggering immediate restart."
            break
        fi
        sleep 30
    done
done

log "Grid dry run complete after $RESTART_COUNT restarts."
