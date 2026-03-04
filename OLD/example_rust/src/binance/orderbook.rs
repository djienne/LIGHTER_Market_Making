//! Local orderbook state with incremental updates.
//!
//! Maintains a local copy of the Binance orderbook using BTreeMap for efficient
//! sorted access and incremental delta application.

use std::collections::BTreeMap;

use ordered_float::OrderedFloat;
use thiserror::Error;

use crate::types::{OrderbookSnapshot, PriceLevel, Symbol, MAX_LEVELS};

use super::messages::{parse_level, BinanceDepthSnapshot, BinanceDepthUpdate, ParseError};

/// Errors during orderbook synchronization.
#[derive(Debug, Error)]
pub enum SyncError {
    /// Sequence gap detected - need to re-sync from REST snapshot
    #[error("Sequence gap: expected U <= {expected} + 1, got U = {got}")]
    SequenceGap { expected: u64, got: u64 },

    /// Sequence break in continuous stream
    #[error("Sequence break: expected U = {expected}, got U = {got}")]
    SequenceBreak { expected: u64, got: u64 },

    /// Parse error in price/quantity
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),

    /// Not yet synced (haven't received valid first update after snapshot)
    #[error("Not yet synced with snapshot")]
    NotSynced,
}

/// Local orderbook state for Binance.
///
/// Uses BTreeMap with OrderedFloat keys for O(log n) updates while maintaining
/// sorted order. Bids are stored with negated keys for descending order.
#[derive(Debug)]
pub struct BinanceOrderbook {
    /// Symbol name (e.g., "BTCUSDT")
    symbol: String,

    /// Bid levels: price -> quantity (sorted descending by price)
    /// We use negative keys to get descending order from BTreeMap
    bids: BTreeMap<OrderedFloat<f64>, f64>,

    /// Ask levels: price -> quantity (sorted ascending by price)
    asks: BTreeMap<OrderedFloat<f64>, f64>,

    /// Last update ID from REST snapshot
    snapshot_update_id: u64,

    /// Last processed final update ID from WebSocket
    last_update_id: u64,

    /// Whether we've successfully synced with the first WS update after snapshot
    is_synced: bool,

    /// Event time of last update (milliseconds)
    last_event_time: u64,
}

impl BinanceOrderbook {
    /// Create a new empty orderbook.
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            snapshot_update_id: 0,
            last_update_id: 0,
            is_synced: false,
            last_event_time: 0,
        }
    }

    /// Initialize orderbook from a REST API snapshot.
    ///
    /// This resets the sync state and requires re-syncing with WebSocket updates.
    pub fn init_from_snapshot(&mut self, snapshot: BinanceDepthSnapshot) -> Result<(), SyncError> {
        self.bids.clear();
        self.asks.clear();

        // Parse and insert bids
        for level in &snapshot.bids {
            let (price, qty) = parse_level(level)?;
            if qty > 0.0 {
                // Store with original price (we'll iterate in reverse for descending)
                self.bids.insert(OrderedFloat(price), qty);
            }
        }

        // Parse and insert asks
        for level in &snapshot.asks {
            let (price, qty) = parse_level(level)?;
            if qty > 0.0 {
                self.asks.insert(OrderedFloat(price), qty);
            }
        }

        self.snapshot_update_id = snapshot.last_update_id;
        self.last_update_id = snapshot.last_update_id;
        self.is_synced = false;

        Ok(())
    }

    /// Apply an incremental depth update from WebSocket.
    ///
    /// Returns `Ok(true)` if update was applied, `Ok(false)` if skipped (stale),
    /// or `Err` if sequence gap detected (need to re-sync).
    ///
    /// Synchronization algorithm differs between Spot and Futures:
    ///
    /// **Spot** (no `pu` field):
    /// 1. First update after snapshot: U <= lastUpdateId+1 AND u >= lastUpdateId+1
    /// 2. Subsequent updates: U == previous_u + 1
    ///
    /// **Futures** (has `pu` field):
    /// 1. First update after snapshot: U <= lastUpdateId AND u >= lastUpdateId
    /// 2. Subsequent updates: pu == previous_u
    pub fn apply_update(&mut self, update: &BinanceDepthUpdate) -> Result<bool, SyncError> {
        let is_futures = update.is_futures();

        // Skip updates that are completely stale (before our snapshot)
        if update.final_update_id < self.snapshot_update_id {
            return Ok(false);
        }

        if !self.is_synced {
            // First update after snapshot - check sync condition
            let sync_ok = if is_futures {
                // Futures: U <= lastUpdateId AND u >= lastUpdateId
                update.first_update_id <= self.snapshot_update_id
                    && update.final_update_id >= self.snapshot_update_id
            } else {
                // Spot: U <= lastUpdateId+1 AND u >= lastUpdateId+1
                update.first_update_id <= self.snapshot_update_id + 1
                    && update.final_update_id >= self.snapshot_update_id + 1
            };

            if sync_ok {
                self.is_synced = true;
                // Fall through to apply the update
            } else if is_futures {
                // Futures: check if we should skip or report gap
                if update.first_update_id > self.snapshot_update_id {
                    return Err(SyncError::SequenceGap {
                        expected: self.snapshot_update_id,
                        got: update.first_update_id,
                    });
                }
                // Update is before our sync point, skip it
                return Ok(false);
            } else {
                // Spot: check if we should skip or report gap
                if update.first_update_id > self.snapshot_update_id + 1 {
                    return Err(SyncError::SequenceGap {
                        expected: self.snapshot_update_id,
                        got: update.first_update_id,
                    });
                }
                // Update is before our sync point, skip it
                return Ok(false);
            }
        } else {
            // Subsequent updates - check continuity
            if is_futures {
                // Futures: pu == previous_u
                if let Some(pu) = update.prev_final_update_id {
                    if pu != self.last_update_id {
                        return Err(SyncError::SequenceBreak {
                            expected: self.last_update_id,
                            got: pu,
                        });
                    }
                }
            } else {
                // Spot: U == previous_u + 1
                if update.first_update_id != self.last_update_id + 1 {
                    return Err(SyncError::SequenceBreak {
                        expected: self.last_update_id + 1,
                        got: update.first_update_id,
                    });
                }
            }
        }

        // Apply bid deltas
        for level in &update.bids {
            let (price, qty) = parse_level(level)?;
            if qty == 0.0 {
                self.bids.remove(&OrderedFloat(price));
            } else {
                self.bids.insert(OrderedFloat(price), qty);
            }
        }

        // Apply ask deltas
        for level in &update.asks {
            let (price, qty) = parse_level(level)?;
            if qty == 0.0 {
                self.asks.remove(&OrderedFloat(price));
            } else {
                self.asks.insert(OrderedFloat(price), qty);
            }
        }

        self.last_update_id = update.final_update_id;
        self.last_event_time = update.event_time;

        Ok(true)
    }

    /// Convert local orderbook state to an OrderbookSnapshot.
    ///
    /// This is lock-free and produces a snapshot suitable for strategy consumption.
    pub fn to_snapshot(&self) -> OrderbookSnapshot {
        let mut snapshot = OrderbookSnapshot::new(Symbol::new(&self.symbol));
        snapshot.sequence = self.last_update_id;
        snapshot.timestamp_ns = (self.last_event_time as i64) * 1_000_000; // ms to ns
        snapshot.received_at_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);

        // Copy bids (descending order - iterate in reverse since BTreeMap is ascending)
        let mut bid_count = 0;
        for (price, qty) in self.bids.iter().rev() {
            if bid_count >= MAX_LEVELS {
                break;
            }
            snapshot.bids[bid_count] = PriceLevel::new(price.0, *qty);
            bid_count += 1;
        }
        snapshot.bid_count = bid_count as u8;

        // Copy asks (ascending order - BTreeMap natural order)
        let mut ask_count = 0;
        for (price, qty) in self.asks.iter() {
            if ask_count >= MAX_LEVELS {
                break;
            }
            snapshot.asks[ask_count] = PriceLevel::new(price.0, *qty);
            ask_count += 1;
        }
        snapshot.ask_count = ask_count as u8;

        snapshot
    }

    /// Check if the orderbook is synced with the WebSocket stream.
    #[inline]
    pub fn is_synced(&self) -> bool {
        self.is_synced
    }

    /// Get the last update ID.
    #[inline]
    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    /// Get the snapshot update ID (for sync debugging).
    #[inline]
    pub fn snapshot_update_id(&self) -> u64 {
        self.snapshot_update_id
    }

    /// Get the number of bid levels.
    #[inline]
    pub fn bid_count(&self) -> usize {
        self.bids.len()
    }

    /// Get the number of ask levels.
    #[inline]
    pub fn ask_count(&self) -> usize {
        self.asks.len()
    }

    /// Get best bid price.
    #[inline]
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next_back().map(|k| k.0)
    }

    /// Get best ask price.
    #[inline]
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|k| k.0)
    }

    /// Reset sync state (call before re-fetching snapshot).
    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.snapshot_update_id = 0;
        self.last_update_id = 0;
        self.is_synced = false;
        self.last_event_time = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snapshot(last_update_id: u64, bids: &[(f64, f64)], asks: &[(f64, f64)]) -> BinanceDepthSnapshot {
        BinanceDepthSnapshot {
            last_update_id,
            bids: bids
                .iter()
                .map(|(p, q)| [p.to_string(), q.to_string()])
                .collect(),
            asks: asks
                .iter()
                .map(|(p, q)| [p.to_string(), q.to_string()])
                .collect(),
        }
    }

    /// Create a Spot-style update (no pu field)
    fn make_update(first_id: u64, final_id: u64, bids: &[(f64, f64)], asks: &[(f64, f64)]) -> BinanceDepthUpdate {
        BinanceDepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1234567890,
            symbol: "BTCUSDT".to_string(),
            first_update_id: first_id,
            final_update_id: final_id,
            prev_final_update_id: None, // Spot style
            bids: bids
                .iter()
                .map(|(p, q)| [p.to_string(), q.to_string()])
                .collect(),
            asks: asks
                .iter()
                .map(|(p, q)| [p.to_string(), q.to_string()])
                .collect(),
        }
    }

    /// Create a Futures-style update (with pu field)
    fn make_futures_update(first_id: u64, final_id: u64, prev_id: u64, bids: &[(f64, f64)], asks: &[(f64, f64)]) -> BinanceDepthUpdate {
        BinanceDepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1234567890,
            symbol: "BTCUSDT".to_string(),
            first_update_id: first_id,
            final_update_id: final_id,
            prev_final_update_id: Some(prev_id), // Futures style
            bids: bids
                .iter()
                .map(|(p, q)| [p.to_string(), q.to_string()])
                .collect(),
            asks: asks
                .iter()
                .map(|(p, q)| [p.to_string(), q.to_string()])
                .collect(),
        }
    }

    #[test]
    fn test_bids_sorted_descending() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(
            100,
            &[(99.0, 1.0), (100.0, 2.0), (98.0, 3.0)], // Unsorted input
            &[(101.0, 1.0)],
        );

        book.init_from_snapshot(snapshot).unwrap();
        let snap = book.to_snapshot();

        // Best bid (highest price) should be first
        assert_eq!(snap.bid_count, 3);
        assert_eq!(snap.bids[0].price, 100.0);
        assert_eq!(snap.bids[1].price, 99.0);
        assert_eq!(snap.bids[2].price, 98.0);
    }

    #[test]
    fn test_asks_sorted_ascending() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(
            100,
            &[(99.0, 1.0)],
            &[(103.0, 3.0), (101.0, 1.0), (102.0, 2.0)], // Unsorted input
        );

        book.init_from_snapshot(snapshot).unwrap();
        let snap = book.to_snapshot();

        // Best ask (lowest price) should be first
        assert_eq!(snap.ask_count, 3);
        assert_eq!(snap.asks[0].price, 101.0);
        assert_eq!(snap.asks[1].price, 102.0);
        assert_eq!(snap.asks[2].price, 103.0);
    }

    #[test]
    fn test_incremental_update_removes_zero_qty() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(
            100,
            &[(99.0, 1.0), (100.0, 2.0)],
            &[(101.0, 1.0), (102.0, 2.0)],
        );

        book.init_from_snapshot(snapshot).unwrap();
        assert_eq!(book.bid_count(), 2);
        assert_eq!(book.ask_count(), 2);

        // Apply update that removes a bid level (qty = 0)
        let update = make_update(
            101, // U <= 100+1 AND u >= 100+1 (sync condition)
            101,
            &[(100.0, 0.0)], // Remove bid at 100.0
            &[(101.0, 0.0)], // Remove ask at 101.0
        );

        let applied = book.apply_update(&update).unwrap();
        assert!(applied);
        assert!(book.is_synced());
        assert_eq!(book.bid_count(), 1);
        assert_eq!(book.ask_count(), 1);

        let snap = book.to_snapshot();
        assert_eq!(snap.bids[0].price, 99.0);
        assert_eq!(snap.asks[0].price, 102.0);
    }

    #[test]
    fn test_sequence_gap_detection() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(100, &[(99.0, 1.0)], &[(101.0, 1.0)]);

        book.init_from_snapshot(snapshot).unwrap();

        // First update with gap (U > lastUpdateId + 1)
        let update = make_update(
            105, // Gap! Expected <= 101
            110,
            &[(98.0, 1.0)],
            &[(102.0, 1.0)],
        );

        let result = book.apply_update(&update);
        assert!(matches!(result, Err(SyncError::SequenceGap { .. })));
    }

    #[test]
    fn test_sequence_break_detection() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(100, &[(99.0, 1.0)], &[(101.0, 1.0)]);

        book.init_from_snapshot(snapshot).unwrap();

        // First update - valid sync
        let update1 = make_update(101, 105, &[], &[]);
        book.apply_update(&update1).unwrap();
        assert!(book.is_synced());

        // Second update with gap (U != previous_u + 1)
        let update2 = make_update(
            110, // Gap! Expected 106
            115,
            &[],
            &[],
        );

        let result = book.apply_update(&update2);
        assert!(matches!(result, Err(SyncError::SequenceBreak { .. })));
    }

    #[test]
    fn test_snapshot_consistency_after_updates() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(
            100,
            &[(99.0, 1.0), (100.0, 2.0)],
            &[(101.0, 1.0), (102.0, 2.0)],
        );

        book.init_from_snapshot(snapshot).unwrap();

        // Apply a series of updates
        let update1 = make_update(101, 101, &[(100.5, 1.5)], &[(101.5, 1.5)]); // Add levels
        book.apply_update(&update1).unwrap();

        let update2 = make_update(102, 102, &[(99.0, 0.0)], &[(102.0, 0.0)]); // Remove levels
        book.apply_update(&update2).unwrap();

        let update3 = make_update(103, 103, &[(100.0, 5.0)], &[(101.0, 5.0)]); // Update quantities
        book.apply_update(&update3).unwrap();

        let snap = book.to_snapshot();

        // Verify final state
        assert!(snap.is_valid());
        assert_eq!(snap.bid_count, 2); // 100.5, 100.0 (99.0 removed)
        assert_eq!(snap.ask_count, 2); // 101.0, 101.5 (102.0 removed)

        // Bids descending
        assert_eq!(snap.bids[0].price, 100.5);
        assert_eq!(snap.bids[1].price, 100.0);
        assert_eq!(snap.bids[1].quantity, 5.0); // Updated quantity

        // Asks ascending
        assert_eq!(snap.asks[0].price, 101.0);
        assert_eq!(snap.asks[0].quantity, 5.0); // Updated quantity
        assert_eq!(snap.asks[1].price, 101.5);
    }

    #[test]
    fn test_stale_updates_skipped() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(100, &[(99.0, 1.0)], &[(101.0, 1.0)]);

        book.init_from_snapshot(snapshot).unwrap();

        // Update with final_update_id <= snapshot_update_id should be skipped
        let stale_update = make_update(95, 98, &[(98.0, 1.0)], &[(102.0, 1.0)]);

        let applied = book.apply_update(&stale_update).unwrap();
        assert!(!applied);
        assert!(!book.is_synced()); // Still not synced
    }

    // ==================== Futures-specific tests ====================

    #[test]
    fn test_futures_sync_condition() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(100, &[(99.0, 1.0)], &[(101.0, 1.0)]);

        book.init_from_snapshot(snapshot).unwrap();
        assert!(!book.is_synced());

        // Futures sync condition: U <= lastUpdateId AND u >= lastUpdateId
        // This should sync (U=95 <= 100, u=105 >= 100)
        let update = make_futures_update(95, 105, 90, &[(98.0, 1.0)], &[]);
        let applied = book.apply_update(&update).unwrap();

        assert!(applied);
        assert!(book.is_synced());
        assert_eq!(book.last_update_id(), 105);
    }

    #[test]
    fn test_futures_continuity_with_pu() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(100, &[(99.0, 1.0)], &[(101.0, 1.0)]);

        book.init_from_snapshot(snapshot).unwrap();

        // First update - sync
        let update1 = make_futures_update(95, 105, 90, &[], &[]);
        book.apply_update(&update1).unwrap();
        assert!(book.is_synced());

        // Second update - pu should equal previous u (105)
        let update2 = make_futures_update(106, 110, 105, &[(98.0, 2.0)], &[]);
        let applied = book.apply_update(&update2).unwrap();

        assert!(applied);
        assert_eq!(book.last_update_id(), 110);
    }

    #[test]
    fn test_futures_sequence_break_via_pu() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(100, &[(99.0, 1.0)], &[(101.0, 1.0)]);

        book.init_from_snapshot(snapshot).unwrap();

        // First update - sync
        let update1 = make_futures_update(95, 105, 90, &[], &[]);
        book.apply_update(&update1).unwrap();
        assert!(book.is_synced());

        // Second update - pu doesn't match previous u (should be 105, but got 100)
        let update2 = make_futures_update(106, 110, 100, &[], &[]);
        let result = book.apply_update(&update2);

        assert!(matches!(result, Err(SyncError::SequenceBreak { expected: 105, got: 100 })));
    }

    #[test]
    fn test_futures_gap_detection() {
        let mut book = BinanceOrderbook::new("BTCUSDT");
        let snapshot = make_snapshot(100, &[(99.0, 1.0)], &[(101.0, 1.0)]);

        book.init_from_snapshot(snapshot).unwrap();

        // Futures: first update with U > lastUpdateId should trigger gap
        // (We can't sync if U=105 > lastUpdateId=100)
        let update = make_futures_update(105, 110, 100, &[], &[]);
        let result = book.apply_update(&update);

        assert!(matches!(result, Err(SyncError::SequenceGap { .. })));
    }
}
