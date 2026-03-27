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
        if os.path.exists(self._path) and os.path.getsize(self._path) > 0:
            return
        with self._write_lock:
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
    ) -> None:
        """Buffer one fill row (no disk I/O)."""
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        row = [
            ts,
            self._symbol,
            side,
            f"{price:.2f}",
            f"{size:.6f}",
            level,
            f"{position_after:.6f}",
            f"{realized_pnl:.4f}",
            f"{available_capital:.2f}",
            f"{portfolio_value:.2f}",
            str(simulated).lower(),
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
