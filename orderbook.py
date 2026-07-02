"""Shared order book update logic used by market_maker_v2 and grid_dry_run."""

import math

# Memoized "does this container expose the CBookSide bulk methods?" check,
# keyed by container type.  The container type never changes after startup,
# so this avoids a hasattr() call per WS message on the hottest path.
_BULK_TYPES: dict = {}


def _has_bulk_methods(book) -> bool:
    book_type = type(book)
    flag = _BULK_TYPES.get(book_type)
    if flag is None:
        flag = hasattr(book, 'apply_snapshot_from_wire')
        _BULK_TYPES[book_type] = flag
    return flag


def _validate_level(price: float, size: float) -> None:
    """Match CBookSide._validate_level semantics for the Python fallback."""
    if not (math.isfinite(price) and price > 0.0):
        raise ValueError(f"invalid order book price: {price}")
    if not (math.isfinite(size) and size >= 0.0):
        raise ValueError(f"invalid order book size: {size}")


def apply_orderbook_update(book_bids, book_asks, initialized, bids_in, asks_in, snapshot_threshold=100,
                           *, is_snapshot_hint=None):
    """Apply an order book update (snapshot or delta) to bid/ask mappings.

    Works with both plain ``dict`` and ``SortedDict`` containers.
    When the containers are ``CBookSide`` (Cython), bulk wire-update
    methods are used to avoid per-level Python overhead.

    Args:
        book_bids: Mutable mapping of price -> size for bids.
        book_asks: Mutable mapping of price -> size for asks.
        initialized: Whether the book has been initialized previously.
        bids_in: List of ``{'price': ..., 'size': ...}`` dicts from the wire.
        asks_in: List of ``{'price': ..., 'size': ...}`` dicts from the wire.
        snapshot_threshold: If either side exceeds this count and the book is
            already initialized, treat the message as a snapshot (legacy
            heuristic, used only when ``is_snapshot_hint`` is ``None``).
        is_snapshot_hint: Authoritative protocol hint.  ``True`` when the
            message type marks a snapshot (``subscribed/order_book``) —
            forces snapshot handling even for small books.  ``False``
            (``update/order_book``) forces delta handling even for large
            updates — live data shows busy deltas can exceed 100 levels, and
            the size heuristic would wrongly clear the book on those.
            ``None`` falls back to the size heuristic (legacy callers).

    Returns:
        is_snapshot (bool): Whether the update was treated as a full snapshot.

    Note:
        A message explicitly hinted as a *delta* is skipped entirely while
        the book is uninitialized: a delta only contains changed levels, so
        seeding an empty book from it would produce a sparse fake book with
        a wrong best bid/ask.  The caller's health checks handle triggering
        a reconnect for a fresh snapshot.
    """
    is_snapshot = False
    if not initialized:
        if is_snapshot_hint is False:
            return False
        is_snapshot = True
    elif is_snapshot_hint is not None:
        is_snapshot = is_snapshot_hint
    elif len(bids_in) > snapshot_threshold or len(asks_in) > snapshot_threshold:
        is_snapshot = True

    # Fast path: CBookSide bulk methods (avoids per-level Python overhead)
    _has_bulk = _has_bulk_methods(book_bids)

    if is_snapshot:
        if _has_bulk:
            book_bids.apply_snapshot_from_wire(bids_in)
            book_asks.apply_snapshot_from_wire(asks_in)
        else:
            parsed_bids = []
            for item in bids_in:
                price, size = float(item['price']), float(item['size'])
                _validate_level(price, size)
                parsed_bids.append((price, size))
            parsed_asks = []
            for item in asks_in:
                price, size = float(item['price']), float(item['size'])
                _validate_level(price, size)
                parsed_asks.append((price, size))
            book_bids.clear()
            book_asks.clear()
            for price, size in parsed_bids:
                if size > 0:
                    book_bids[price] = size
            for price, size in parsed_asks:
                if size > 0:
                    book_asks[price] = size
    else:
        if _has_bulk:
            book_bids.apply_delta_from_wire(bids_in)
            book_asks.apply_delta_from_wire(asks_in)
        else:
            # Parse + validate everything before mutating so one malformed
            # level cannot leave the book half-updated (matches CBookSide).
            parsed_bids = []
            for item in bids_in:
                price, size = float(item['price']), float(item['size'])
                _validate_level(price, size)
                parsed_bids.append((price, size))
            parsed_asks = []
            for item in asks_in:
                price, size = float(item['price']), float(item['size'])
                _validate_level(price, size)
                parsed_asks.append((price, size))
            for price, size in parsed_bids:
                if size == 0:
                    book_bids.pop(price, None)
                else:
                    book_bids[price] = size
            for price, size in parsed_asks:
                if size == 0:
                    book_asks.pop(price, None)
                else:
                    book_asks[price] = size

    return is_snapshot
