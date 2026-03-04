# Monitoring Runbook

This file captures the current monitoring setup and tips for long runs (e.g., 8h).

## Start a run (full debug logs)

```bash
RUST_LOG=debug ./target/release/standx-orderbook config.json > run_debug_8h.log 2>&1 &
echo $! > run_debug_8h.pid
```

## Periodic checks (every ~10 minutes)

A background watcher scans the log and appends summaries to `run_debug_8h_watch.log`.
It records:
- alerts (WARN/ERROR/panic/etc.)
- sanity drift (non-zero REST vs WS bid/ask diffs)

To start the watcher:

```bash
(while true; do ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ"); \
  hits=$(rg -n "WARN|ERROR|panic|Orderbook drift|Crossed book|Orderbook validation failed" run_debug_8h.log | tail -n 20); \
  sanity_hits=$(tail -n 2000 run_debug_8h.log | awk '/SANITY/ { if (match($0, /diff: bid=([-0-9.]+) ask=([-0-9.]+)/, m)) { if (m[1] != "0.00" || m[2] != "0.00") print $0 } }' | tail -n 20); \
  { printf "=== %s ===\n" "$ts"; \
    if [ -n "$hits" ]; then printf "[alerts]\n%s\n" "$hits"; else printf "[alerts]\n(no hits)\n"; fi; \
    if [ -n "$sanity_hits" ]; then printf "[sanity_nonzero]\n%s\n" "$sanity_hits"; else printf "[sanity_nonzero]\n(no hits)\n"; fi; } >> run_debug_8h_watch.log; \
  sleep 600; done) &
echo $! > run_debug_8h_watch.pid
```

## Log rotation (size-based)

A lightweight rotator keeps logs from filling disk. It checks every 5 minutes,
rotates `run_debug_8h.log` at 50MB, and keeps the 5 newest archives.

```bash
(while true; do size=$(stat -c %s run_debug_8h.log 2>/dev/null || echo 0); max=$((50*1024*1024)); \
  if [ "$size" -ge "$max" ]; then ts=$(date -u +"%Y%m%dT%H%M%SZ"); \
    cp run_debug_8h.log "run_debug_8h.log.$ts"; : > run_debug_8h.log; \
    ls -1t run_debug_8h.log.20* 2>/dev/null | tail -n +6 | xargs -r rm --; fi; \
  sleep 300; done) &
echo $! > run_debug_8h_rotate.pid
```

## Quick checks

- Latest warnings/errors:
  ```bash
  rg -n "WARN|ERROR|panic|Orderbook drift|Crossed book|Orderbook validation failed" run_debug_8h.log | tail -n 50
  ```

- Latest watch summary:
  ```bash
  tail -n 40 run_debug_8h_watch.log
  ```

- Latest quote lines:
  ```bash
  rg -n "Quote: bid=|bid=|ask=" run_debug_8h.log | tail -n 5
  ```

- Latest position lines:
  ```bash
  rg -n "Position:" run_debug_8h.log | tail -n 5
  ```

- Latest warmup status:
  ```bash
  rg -n "\[WARM" run_debug_8h.log | tail -n 1
  ```

- Latest sanity check (REST vs WS):
  ```bash
  rg -n "SANITY" run_debug_8h.log | tail -n 5
  ```

- Live tail (debug stream):
  ```bash
  tail -f run_debug_8h.log
  ```

## Process status

- Confirm only one orderbook process:
  ```bash
  ps -ef | rg -n "standx-orderbook"
  ```

- Check watcher/rotator PIDs:
  ```bash
  ps -p "$(cat run_debug_8h_watch.pid)" -o pid,cmd
  ps -p "$(cat run_debug_8h_rotate.pid)" -o pid,cmd
  ```

## Disk usage

- Check available space:
  ```bash
  df -h /home/ubuntu/standx
  ```

- Check current log sizes:
  ```bash
  ls -lh run_debug_8h.log run_debug_8h_watch.log
  ```

## Troubleshooting notes

- If you see frequent `Orderbook drift` corrections:
  - Confirm WS ordering is monotonic (watch for SANITY diffs).
  - Consider raising `orderbook_sanity_check.drift_threshold_bps`
    or increasing `interval_secs`.
- If you see `Crossed book` or validation failures:
  - Verify bid/ask ordering in upstream feed.
  - Inspect latest SANITY line for non-zero diffs.
- If `disconnected`/`reconnect` messages appear:
  - Check network stability and WebSocket endpoints in `config.json`.

## Rotate cleanup

- Keep only the newest 5 archives:
  ```bash
  ls -1t run_debug_8h.log.20* 2>/dev/null | tail -n +6 | xargs -r rm --
  ```

## Restart sequence

```bash
kill "$(cat run_debug_8h_watch.pid)"
kill "$(cat run_debug_8h_rotate.pid)"
kill "$(cat run_debug_8h.pid)"
RUST_LOG=debug ./target/release/standx-orderbook config.json > run_debug_8h.log 2>&1 &
echo $! > run_debug_8h.pid
```

## Stop

```bash
kill "$(cat run_debug_8h_watch.pid)"
kill "$(cat run_debug_8h_rotate.pid)"
kill "$(cat run_debug_8h.pid)"
```

## Notes

- Logs live in the workspace: `run_debug_8h.log` and `run_debug_8h_watch.log`.
- The watcher scans the last 2000 lines for sanity diffs and reports non-zero entries.
