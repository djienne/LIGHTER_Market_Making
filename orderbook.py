"""Shared order book update logic used by both market_maker_v2 and gather_lighter_data."""


def apply_orderbook_update(book_bids, book_asks, initialized, bids_in, asks_in, snapshot_threshold=100):
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
            already initialized, treat the message as a snapshot.

    Returns:
        is_snapshot (bool): Whether the update was treated as a full snapshot.
    """
    is_snapshot = False
    if not initialized:
        is_snapshot = True
    elif len(bids_in) > snapshot_threshold or len(asks_in) > snapshot_threshold:
        is_snapshot = True

    # Fast path: CBookSide bulk methods (avoids per-level Python overhead)
    _has_bulk = hasattr(book_bids, 'apply_snapshot_from_wire')

    if is_snapshot:
        if _has_bulk:
            book_bids.apply_snapshot_from_wire(bids_in)
            book_asks.apply_snapshot_from_wire(asks_in)
        else:
            book_bids.clear()
            book_asks.clear()
            for item in bids_in:
                price, size = float(item['price']), float(item['size'])
                if size > 0:
                    book_bids[price] = size
            for item in asks_in:
                price, size = float(item['price']), float(item['size'])
                if size > 0:
                    book_asks[price] = size
    else:
        if _has_bulk:
            book_bids.apply_delta_from_wire(bids_in)
            book_asks.apply_delta_from_wire(asks_in)
        else:
            for item in bids_in:
                price, size = float(item['price']), float(item['size'])
                if size == 0:
                    book_bids.pop(price, None)
                else:
                    book_bids[price] = size
            for item in asks_in:
                price, size = float(item['price']), float(item['size'])
                if size == 0:
                    book_asks.pop(price, None)
                else:
                    book_asks[price] = size

    return is_snapshot
