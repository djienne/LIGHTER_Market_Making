"""Append-only CSV trade logger.

Records every fill (both dry-run and live) to ``logs/trades_{symbol}.csv``.

``log_fill()`` is O(1) — it appends to an in-memory buffer.  Actual disk I/O
happens only in ``flush()`` (called periodically from the main loop and on
shutdown).
"""

from __future__ import annotations

import csv
import io
import os
import threading
from datetime import datetime, timezone

_HEADER = [
    "timestamp",
    "symbol",
    "side",
    "price",
    "size",
    "level",
    "position_after",
    "realized_pnl",
    "available_capital",
    "portfolio_value",
    "simulated",
    "notional_usd",
    "fee_usd",
    "entry_vwap_after",
    "realized_pnl_cumulative",
    "mid_at_fill",
    "spread_capture_bps",
    "inventory_after_usd",
    "client_order_index",
    "exchange_order_index",
    "fill_source",
]


class TradeLogger:
    """Buffered, append-only CSV trade log."""

    def __init__(self, log_dir: str, symbol: str):
        os.makedirs(log_dir, exist_ok=True)
        self._path = os.path.join(log_dir, f"trades_{symbol}.csv")
        self._symbol = symbol
        self._buffer: list[list] = []
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._ensure_header()

    @property
    def path(self) -> str:
        return self._path

    def _ensure_header(self) -> None:
        """Write CSV header if file doesn't exist or is empty."""
        with self._write_lock:
            if os.path.exists(self._path) and os.path.getsize(self._path) > 0:
                with open(self._path, newline="") as f:
                    rows = list(csv.reader(f))
                if rows and rows[0] == _HEADER:
                    return
                if rows and rows[0] and rows[0] != _HEADER:
                    padded_rows = [rows[0] + [h for h in _HEADER[len(rows[0]):]]]
                    for row in rows[1:]:
                        padded_rows.append(row + [""] * max(0, len(_HEADER) - len(row)))
                    with open(self._path, "w", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(_HEADER)
                        writer.writerows(padded_rows[1:])
                    return
            with open(self._path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(_HEADER)

    # ------------------------------------------------------------------
    # Hot-path: in-memory only
    # ------------------------------------------------------------------

    def log_fill(
        self,
        *,
        side: str,
        price: float,
        size: float,
        level: int,
        position_after: float,
        realized_pnl: float,
        available_capital: float,
        portfolio_value: float,
        simulated: bool,
        notional_usd: float | None = None,
        fee_usd: float | None = None,
        entry_vwap_after: float | None = None,
        realized_pnl_cumulative: float | None = None,
        mid_at_fill: float | None = None,
        spread_capture_bps: float | None = None,
        inventory_after_usd: float | None = None,
        client_order_index: int | str | None = None,
        exchange_order_index: int | str | None = None,
        fill_source: str = "",
    ) -> None:
        """Buffer one fill row (no disk I/O)."""
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        notional = price * size if notional_usd is None else notional_usd
        row = [
            ts,
            self._symbol,
            side,
            f"{price:.10g}",
            f"{size:.6f}",
            level,
            f"{position_after:.6f}",
            f"{realized_pnl:.4f}",
            f"{available_capital:.2f}",
            f"{portfolio_value:.2f}",
            str(simulated).lower(),
            "" if notional is None else f"{notional:.6f}",
            "" if fee_usd is None else f"{fee_usd:.8f}",
            "" if entry_vwap_after is None else f"{entry_vwap_after:.10g}",
            "" if realized_pnl_cumulative is None else f"{realized_pnl_cumulative:.6f}",
            "" if mid_at_fill is None else f"{mid_at_fill:.10g}",
            "" if spread_capture_bps is None else f"{spread_capture_bps:.4f}",
            "" if inventory_after_usd is None else f"{inventory_after_usd:.6f}",
            "" if client_order_index is None else str(client_order_index),
            "" if exchange_order_index is None else str(exchange_order_index),
            fill_source,
        ]
        with self._lock:
            self._buffer.append(row)

    # ------------------------------------------------------------------
    # Slow-path: disk I/O (call from main loop or shutdown)
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """Write buffered rows to disk.

        On write failure, rows are prepended back into the buffer so
        they can be retried on the next flush.
        """
        with self._lock:
            if not self._buffer:
                return
            rows = self._buffer[:]
            self._buffer.clear()
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerows(rows)
        try:
            with self._write_lock:
                with open(self._path, "a", newline="") as f:
                    f.write(buf.getvalue())
        except OSError:
            with self._lock:
                self._buffer[:0] = rows  # prepend for retry
            raise

    def clear(self) -> None:
        """Delete the trade log and reset (used on capital reset)."""
        with self._lock:
            self._buffer.clear()
        with self._write_lock:
            if os.path.exists(self._path):
                os.remove(self._path)
        self._ensure_header()
