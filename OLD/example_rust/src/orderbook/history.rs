//! Lock-free ring buffer for orderbook history.
//!
//! This module provides a high-performance, lock-free ring buffer for storing
//! orderbook snapshots. It uses atomic operations for synchronization,
//! allowing concurrent reads and writes without blocking.
//!
//! # Design
//!
//! The buffer uses a monotonically increasing write position with wrap-around.
//! Readers can safely iterate over recent entries by tracking the write position
//! and checking sequence numbers for consistency.
//!
//! # Memory Layout
//!
//! The buffer is pre-allocated at initialization time to avoid runtime allocations.
//! Each slot contains an OrderbookSnapshot plus a generation counter for
//! detecting overwrites during reads.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::types::{OrderbookSnapshot, Symbol};

/// A slot in the ring buffer containing a snapshot and its generation.
struct HistorySlot {
    /// The orderbook snapshot stored in this slot
    snapshot: OrderbookSnapshot,
    /// Generation counter incremented on each write to detect overwrites
    generation: AtomicU64,
}

impl Default for HistorySlot {
    fn default() -> Self {
        Self {
            snapshot: OrderbookSnapshot::default(),
            generation: AtomicU64::new(0),
        }
    }
}

/// Lock-free ring buffer for orderbook history.
///
/// Provides O(1) write operations and O(n) iteration over recent snapshots
/// within the retention window.
pub struct OrderbookHistory {
    /// Pre-allocated buffer of slots
    buffer: Box<[HistorySlot]>,
    /// Buffer capacity
    capacity: usize,
    /// Current write position (monotonically increasing, wraps via modulo)
    write_pos: AtomicU64,
    /// Retention window in nanoseconds
    retention_ns: u64,
    /// Startup time for relative timestamps
    #[allow(dead_code)]
    start_time: Instant,
    /// Statistics: total writes
    total_writes: AtomicU64,
}

impl OrderbookHistory {
    /// Create a new orderbook history buffer.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Number of snapshots to store
    /// * `retention_minutes` - How long to retain snapshots (for filtering)
    pub fn new(capacity: usize, retention_minutes: u64) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");

        // Pre-allocate the buffer
        let buffer: Vec<HistorySlot> = (0..capacity)
            .map(|_| HistorySlot::default())
            .collect();

        Self {
            buffer: buffer.into_boxed_slice(),
            capacity,
            write_pos: AtomicU64::new(0),
            retention_ns: retention_minutes * 60 * 1_000_000_000,
            start_time: Instant::now(),
            total_writes: AtomicU64::new(0),
        }
    }

    /// Push a new snapshot into the buffer.
    ///
    /// This operation is lock-free and will overwrite the oldest entry
    /// when the buffer is full.
    pub fn push(&self, snapshot: OrderbookSnapshot) {
        // Get current position and increment atomically
        let pos = self.write_pos.fetch_add(1, Ordering::AcqRel);
        let idx = (pos as usize) % self.capacity;

        // Get the slot
        let slot = &self.buffer[idx];

        // Increment generation before writing
        slot.generation.fetch_add(1, Ordering::Release);

        // Write the snapshot (this is not atomic, but we use generation to detect)
        // SAFETY: We're the only writer to this slot due to the atomic increment
        let slot_ptr = slot as *const HistorySlot as *mut HistorySlot;
        unsafe {
            (*slot_ptr).snapshot = snapshot;
        }

        // Memory barrier to ensure snapshot is visible before generation update
        std::sync::atomic::fence(Ordering::Release);

        // Increment generation after writing (odd = writing, even = stable)
        slot.generation.fetch_add(1, Ordering::Release);

        self.total_writes.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the most recent snapshot.
    ///
    /// Returns `None` if no snapshots have been written.
    pub fn latest(&self) -> Option<OrderbookSnapshot> {
        let pos = self.write_pos.load(Ordering::Acquire);
        if pos == 0 {
            return None;
        }

        let idx = ((pos - 1) as usize) % self.capacity;
        self.read_slot(idx)
    }

    /// Get the most recent snapshot for a specific symbol.
    pub fn latest_for_symbol(&self, symbol: &Symbol) -> Option<OrderbookSnapshot> {
        let pos = self.write_pos.load(Ordering::Acquire);
        if pos == 0 {
            return None;
        }

        // Search backwards from the most recent
        let start = pos.saturating_sub(1);
        let search_count = self.capacity.min(pos as usize);

        for i in 0..search_count {
            let idx = ((start - i as u64) as usize) % self.capacity;
            if let Some(snapshot) = self.read_slot(idx) {
                if &snapshot.symbol == symbol {
                    return Some(snapshot);
                }
            }
        }

        None
    }

    /// Read a snapshot from a slot with consistency check.
    ///
    /// Returns `None` if the slot was being written during the read.
    fn read_slot(&self, idx: usize) -> Option<OrderbookSnapshot> {
        let slot = &self.buffer[idx];

        // Read generation before reading snapshot
        let gen_before = slot.generation.load(Ordering::Acquire);

        // If generation is odd, slot is being written
        if gen_before % 2 != 0 {
            return None;
        }

        // Read the snapshot
        let snapshot = slot.snapshot.clone();

        // Memory barrier
        std::sync::atomic::fence(Ordering::Acquire);

        // Read generation after reading snapshot
        let gen_after = slot.generation.load(Ordering::Acquire);

        // If generation changed, the read was inconsistent
        if gen_after != gen_before {
            return None;
        }

        Some(snapshot)
    }

    /// Iterate over snapshots within the retention window.
    ///
    /// Returns an iterator yielding snapshots from newest to oldest.
    /// Snapshots that were being written during iteration are skipped.
    pub fn recent_snapshots(&self) -> impl Iterator<Item = OrderbookSnapshot> + '_ {
        let now_ns = self.current_time_ns();
        let cutoff_ns = now_ns.saturating_sub(self.retention_ns as i64);

        let pos = self.write_pos.load(Ordering::Acquire);
        let count = self.capacity.min(pos as usize);

        RecentSnapshotsIter {
            history: self,
            current_pos: pos,
            remaining: count,
            cutoff_ns,
        }
    }

    /// Get snapshots for a specific symbol within the retention window.
    pub fn recent_for_symbol(&self, symbol: &Symbol) -> Vec<OrderbookSnapshot> {
        self.recent_snapshots()
            .filter(|s| &s.symbol == symbol)
            .collect()
    }

    /// Get the current time in nanoseconds since epoch.
    fn current_time_ns(&self) -> i64 {
        // Use system time for absolute timestamps
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    /// Get the number of snapshots currently in the buffer.
    pub fn len(&self) -> usize {
        let pos = self.write_pos.load(Ordering::Relaxed);
        self.capacity.min(pos as usize)
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.write_pos.load(Ordering::Relaxed) == 0
    }

    /// Get the total number of writes since creation.
    pub fn total_writes(&self) -> u64 {
        self.total_writes.load(Ordering::Relaxed)
    }

    /// Get the buffer capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the retention window in minutes.
    pub fn retention_minutes(&self) -> u64 {
        self.retention_ns / 60 / 1_000_000_000
    }
}

/// Iterator over recent snapshots.
struct RecentSnapshotsIter<'a> {
    history: &'a OrderbookHistory,
    current_pos: u64,
    remaining: usize,
    cutoff_ns: i64,
}

impl<'a> Iterator for RecentSnapshotsIter<'a> {
    type Item = OrderbookSnapshot;

    fn next(&mut self) -> Option<Self::Item> {
        while self.remaining > 0 && self.current_pos > 0 {
            self.current_pos -= 1;
            self.remaining -= 1;

            let idx = (self.current_pos as usize) % self.history.capacity;

            if let Some(snapshot) = self.history.read_slot(idx) {
                // Check if within retention window
                if snapshot.timestamp_ns >= self.cutoff_ns {
                    return Some(snapshot);
                } else {
                    // Past the retention window, stop iterating
                    return None;
                }
            }
            // Slot was being written, skip and try next
        }

        None
    }
}

// SAFETY: OrderbookHistory uses atomic operations for all shared state
unsafe impl Send for OrderbookHistory {}
unsafe impl Sync for OrderbookHistory {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_and_latest() {
        let history = OrderbookHistory::new(100, 10);

        assert!(history.is_empty());
        assert!(history.latest().is_none());

        let mut snapshot = OrderbookSnapshot::new(Symbol::new("TEST-USD"));
        snapshot.sequence = 1;
        snapshot.timestamp_ns = 1000;
        history.push(snapshot);

        assert!(!history.is_empty());
        assert_eq!(history.len(), 1);

        let latest = history.latest().unwrap();
        assert_eq!(latest.sequence, 1);
    }

    #[test]
    fn test_wrap_around() {
        let history = OrderbookHistory::new(10, 10);

        // Write more than capacity
        for i in 0..25 {
            let mut snapshot = OrderbookSnapshot::new(Symbol::new("TEST-USD"));
            snapshot.sequence = i;
            history.push(snapshot);
        }

        assert_eq!(history.len(), 10); // Capped at capacity
        assert_eq!(history.total_writes(), 25);

        // Latest should be the last written
        let latest = history.latest().unwrap();
        assert_eq!(latest.sequence, 24);
    }

    #[test]
    fn test_symbol_filter() {
        let history = OrderbookHistory::new(100, 10);

        let mut btc = OrderbookSnapshot::new(Symbol::new("TEST-USD"));
        btc.sequence = 1;
        history.push(btc);

        let mut eth = OrderbookSnapshot::new(Symbol::new("ETH-USD"));
        eth.sequence = 2;
        history.push(eth);

        let latest_btc = history.latest_for_symbol(&Symbol::new("TEST-USD")).unwrap();
        assert_eq!(latest_btc.sequence, 1);

        let latest_eth = history.latest_for_symbol(&Symbol::new("ETH-USD")).unwrap();
        assert_eq!(latest_eth.sequence, 2);
    }
}
