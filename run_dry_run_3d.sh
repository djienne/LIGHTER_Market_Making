#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# 3-day dry-run with hourly monitoring and auto-restart
# Usage:  bash run_dry_run_3d.sh
# Stop:   kill the script (Ctrl-C) or:  kill $(cat logs/dry_run_monitor.pid)
# ──────────────────────────────────────────────────────────────
set -euo pipefail

SYMBOL="BTC"
DURATION_SEC=$((3 * 24 * 3600))   # 3 days
CHECK_INTERVAL=3600               # 1 hour
MAX_RESTARTS=50                   # safety cap
LOG_DIR="logs"
CONSOLE_LOG="$LOG_DIR/dry_run_console.log"
STDERR_LOG="$LOG_DIR/dry_run_stderr.log"
MONITOR_LOG="$LOG_DIR/monitor.log"
STATE_FILE="$LOG_DIR/dry_run_state.json"
PID_FILE="$LOG_DIR/dry_run_monitor.pid"

mkdir -p "$LOG_DIR"
echo $$ > "$PID_FILE"

START_TS=$(date +%s)
END_TS=$((START_TS + DURATION_SEC))
RESTART_COUNT=0

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$MONITOR_LOG"; }

# ── cleanup on exit ──────────────────────────────────────────
cleanup() {
    log "Monitor shutting down."
    if [[ -n "${BOT_PID:-}" ]] && kill -0 "$BOT_PID" 2>/dev/null; then
        log "Sending SIGTERM to bot (PID $BOT_PID)..."
        kill -TERM "$BOT_PID" 2>/dev/null || true
        wait "$BOT_PID" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    log "Done."
}
trap cleanup EXIT INT TERM

# ── start the bot ────────────────────────────────────────────
start_bot() {
    log "Starting dry-run (symbol=$SYMBOL)..."
    python -u market_maker_v2.py --symbol "$SYMBOL" \
        >> "$CONSOLE_LOG" 2>> "$STDERR_LOG" &
    BOT_PID=$!
    log "Bot started (PID $BOT_PID)."
}

# ── health check ─────────────────────────────────────────────
check_health() {
    local ok=true

    # 1) Is process alive?
    if ! kill -0 "$BOT_PID" 2>/dev/null; then
        log "ERROR: Bot process $BOT_PID is dead!"
        ok=false
    fi

    # 2) Read latest state
    if [[ -f "$STATE_FILE" ]]; then
        local state
        state=$(cat "$STATE_FILE")
        local pv rpnl fills pos updated
        pv=$(echo "$state"   | python3 -c "import sys,json; print(json.load(sys.stdin).get('portfolio_value',0))")
        rpnl=$(echo "$state" | python3 -c "import sys,json; print(json.load(sys.stdin).get('realized_pnl',0))")
        fills=$(echo "$state"| python3 -c "import sys,json; print(json.load(sys.stdin).get('fill_count',0))")
        pos=$(echo "$state"  | python3 -c "import sys,json; print(json.load(sys.stdin).get('position',0))")
        updated=$(echo "$state" | python3 -c "import sys,json; print(json.load(sys.stdin).get('updated_at','?'))")
        log "HEALTH | portfolio=\$$pv | realized_pnl=\$$rpnl | fills=$fills | pos=$pos | updated=$updated"
    else
        log "WARNING: No state file yet ($STATE_FILE)"
    fi

    # 3) Check stderr for recent CRITICAL/FATAL errors
    if [[ -f "$STDERR_LOG" ]]; then
        local recent_errors
        recent_errors=$(tail -50 "$STDERR_LOG" | grep -ciE 'CRITICAL|FATAL|Traceback' || true)
        if [[ "$recent_errors" -gt 0 ]]; then
            log "WARNING: Found $recent_errors critical errors in stderr (last 50 lines)"
            tail -20 "$STDERR_LOG" | while IFS= read -r line; do log "  STDERR> $line"; done
        fi
    fi

    # 4) Check console log for circuit breaker
    if [[ -f "$CONSOLE_LOG" ]]; then
        local cb_count
        cb_count=$(tail -200 "$CONSOLE_LOG" | grep -ci 'circuit.breaker\|Trading paused' || true)
        if [[ "$cb_count" -gt 3 ]]; then
            log "WARNING: Circuit breaker triggered $cb_count times recently"
        fi
    fi

    # 5) Stale state check — if state hasn't been updated in >5 minutes, something is wrong
    if [[ -f "$STATE_FILE" ]]; then
        local file_age
        file_age=$(( $(date +%s) - $(stat -c %Y "$STATE_FILE" 2>/dev/null || echo 0) ))
        if [[ "$file_age" -gt 600 ]]; then
            log "WARNING: State file is ${file_age}s old (>10min) — bot may be stuck"
            ok=false
        fi
    fi

    $ok
}

# ── main loop ────────────────────────────────────────────────
WARMUP_SEC=660  # bot has 600s data warmup + 60s for first state write

log "=========================================="
log "3-DAY DRY RUN — started at $(date)"
log "End time: $(date -d @$END_TS)"
log "Check interval: ${CHECK_INTERVAL}s (1 hour)"
log "=========================================="

start_bot

log "Warmup: waiting ${WARMUP_SEC}s for bot to initialize..."
sleep "$WARMUP_SEC"

# Verify bot survived warmup
if ! kill -0 "$BOT_PID" 2>/dev/null; then
    log "ERROR: Bot died during warmup! Stderr:"
    tail -30 "$STDERR_LOG" 2>/dev/null | while IFS= read -r line; do log "  $line"; done
    log "Console tail:"
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
        log "OK: Bot healthy."
    else
        log "Bot needs restart (restart #$((RESTART_COUNT + 1)))."
        # Kill if still lingering
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
    fi

    # Sleep until next check (but wake early if bot dies)
    for ((i=0; i<CHECK_INTERVAL; i+=30)); do
        if ! kill -0 "$BOT_PID" 2>/dev/null; then
            log "Bot died between checks! Triggering immediate restart."
            break
        fi
        sleep 30
    done
done

log "Dry run complete after $RESTART_COUNT restarts."
