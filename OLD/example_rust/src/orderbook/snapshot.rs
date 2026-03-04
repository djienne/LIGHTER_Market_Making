//! Lock-free triple buffer for current orderbook state.
//!
//! This module provides a triple buffer implementation that allows
//! a single writer and multiple readers to access the current orderbook
//! state without blocking each other.
//!
//! # Design
//!
//! The triple buffer uses three pre-allocated buffers and atomic indices:
//! - Buffer 0, 1, 2: Three identical OrderbookSnapshot buffers
//! - write_idx: Index of the buffer currently being written
//! - read_idx: Index of the buffer available for reading
//! - swap_idx: Index of the intermediate buffer for exchange
//!
//! Writer workflow:
//! 1. Write to buffer[write_idx]
//! 2. Atomically swap write_idx with swap_idx
//!
//! Reader workflow:
//! 1. Atomically swap read_idx with swap_idx
//! 2. Read from buffer[read_idx]

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::cell::UnsafeCell;

use crate::types::{OrderbookSnapshot, Symbol};

/// Lock-free triple buffer for the current orderbook state.
///
/// Provides wait-free read and write operations with guaranteed
/// consistency. Writers never block readers and vice versa.
pub struct CurrentOrderbook {
    /// Three pre-allocated buffers
    buffers: [UnsafeCell<OrderbookSnapshot>; 3],

    /// Index of the buffer being written (0, 1, or 2)
    write_idx: AtomicU8,

    /// Index of the buffer available for reading (0, 1, or 2)
    read_idx: AtomicU8,

    /// Index of the intermediate swap buffer (0, 1, or 2)
    swap_idx: AtomicU8,

    /// Flag indicating if any data has been written
    has_data: AtomicU8,

    /// Sequence number for updates
    update_sequence: AtomicU64,

    /// Symbol this orderbook is for
    symbol: Symbol,
}

impl CurrentOrderbook {
    /// Create a new triple buffer for the given symbol.
    pub fn new(symbol: Symbol) -> Self {
        Self {
            buffers: [
                UnsafeCell::new(OrderbookSnapshot::new(symbol)),
                UnsafeCell::new(OrderbookSnapshot::new(symbol)),
                UnsafeCell::new(OrderbookSnapshot::new(symbol)),
            ],
            write_idx: AtomicU8::new(0),
            read_idx: AtomicU8::new(1),
            swap_idx: AtomicU8::new(2),
            has_data: AtomicU8::new(0),
            update_sequence: AtomicU64::new(0),
            symbol,
        }
    }

    /// Get the symbol this orderbook is for.
    #[inline]
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// Update the current orderbook state.
    ///
    /// This operation is wait-free and will not block readers.
    /// The update function receives a mutable reference to the write buffer.
    pub fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut OrderbookSnapshot),
    {
        let write_idx = self.write_idx.load(Ordering::Acquire) as usize;

        // Get mutable access to the write buffer
        // SAFETY: Only one writer at a time, and we own write_idx
        let buffer = unsafe { &mut *self.buffers[write_idx].get() };

        // Apply the update
        f(buffer);

        // Swap write buffer with swap buffer
        // This makes the updated buffer available to readers via swap_idx
        let old_swap = self.swap_idx.swap(write_idx as u8, Ordering::AcqRel);
        self.write_idx.store(old_swap, Ordering::Release);

        // Mark as having data
        self.has_data.store(1, Ordering::Release);

        // Increment sequence AFTER swap completes to ensure readers see
        // consistent data. If sequence was incremented before swap, a reader
        // could see the new sequence but read stale buffer data.
        self.update_sequence.fetch_add(1, Ordering::Release);
    }

    /// Update with a complete snapshot replacement.
    pub fn update_snapshot(&self, snapshot: OrderbookSnapshot) {
        self.update(|buffer| {
            *buffer = snapshot;
        });
    }

    /// Read the current orderbook state.
    ///
    /// This operation is wait-free and returns a clone of the current state.
    /// Returns `None` if no data has been written yet.
    pub fn read(&self) -> Option<OrderbookSnapshot> {
        if self.has_data.load(Ordering::Acquire) == 0 {
            return None;
        }

        // Swap read buffer with swap buffer to get the latest
        let old_swap = self.swap_idx.load(Ordering::Acquire);
        let read_idx = self.read_idx.swap(old_swap, Ordering::AcqRel);
        self.swap_idx.store(read_idx, Ordering::Release);

        // Read from the buffer we just swapped in
        let new_read_idx = self.read_idx.load(Ordering::Acquire) as usize;

        // SAFETY: We now own this buffer for reading
        let buffer = unsafe { &*self.buffers[new_read_idx].get() };
        Some(buffer.clone())
    }

    /// Read the current state without swapping buffers.
    ///
    /// This is slightly faster but may return slightly older data
    /// if the writer is active.
    pub fn peek(&self) -> Option<OrderbookSnapshot> {
        if self.has_data.load(Ordering::Acquire) == 0 {
            return None;
        }

        // Read from swap buffer (most recently completed write)
        let swap_idx = self.swap_idx.load(Ordering::Acquire) as usize;

        // SAFETY: Reading from swap buffer is safe, writer uses write_idx
        let buffer = unsafe { &*self.buffers[swap_idx].get() };
        Some(buffer.clone())
    }

    /// Get the best bid price without copying the entire snapshot.
    #[inline]
    pub fn best_bid(&self) -> Option<f64> {
        if self.has_data.load(Ordering::Acquire) == 0 {
            return None;
        }
        let swap_idx = self.swap_idx.load(Ordering::Acquire) as usize;
        // SAFETY: Reading from swap buffer is safe
        let buffer = unsafe { &*self.buffers[swap_idx].get() };
        buffer.best_bid_price()
    }

    /// Get the best ask price without copying the entire snapshot.
    #[inline]
    pub fn best_ask(&self) -> Option<f64> {
        if self.has_data.load(Ordering::Acquire) == 0 {
            return None;
        }
        let swap_idx = self.swap_idx.load(Ordering::Acquire) as usize;
        // SAFETY: Reading from swap buffer is safe
        let buffer = unsafe { &*self.buffers[swap_idx].get() };
        buffer.best_ask_price()
    }

    /// Get the current spread without copying the entire snapshot.
    #[inline]
    pub fn spread(&self) -> Option<f64> {
        if self.has_data.load(Ordering::Acquire) == 0 {
            return None;
        }
        let swap_idx = self.swap_idx.load(Ordering::Acquire) as usize;
        // SAFETY: Reading from swap buffer is safe
        let buffer = unsafe { &*self.buffers[swap_idx].get() };
        buffer.spread()
    }

    /// Get the current mid price without copying the entire snapshot.
    #[inline]
    pub fn mid_price(&self) -> Option<f64> {
        if self.has_data.load(Ordering::Acquire) == 0 {
            return None;
        }
        let swap_idx = self.swap_idx.load(Ordering::Acquire) as usize;
        // SAFETY: Reading from swap buffer is safe
        let buffer = unsafe { &*self.buffers[swap_idx].get() };
        buffer.mid_price()
    }

    /// Get the update sequence number.
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.update_sequence.load(Ordering::Acquire)
    }

    /// Check if data has been written.
    #[inline]
    pub fn has_data(&self) -> bool {
        self.has_data.load(Ordering::Acquire) != 0
    }
}

// SAFETY: CurrentOrderbook uses atomic operations and UnsafeCell correctly
unsafe impl Send for CurrentOrderbook {}
unsafe impl Sync for CurrentOrderbook {}

/// Manager for multiple symbols' orderbooks.
///
/// Provides efficient lookup and iteration over orderbooks
/// for multiple trading pairs.
pub struct OrderbookManager {
    /// Orderbooks indexed by symbol
    orderbooks: Vec<CurrentOrderbook>,
    /// Symbol lookup map (linear search is fine for small N)
    symbols: Vec<Symbol>,
}

impl OrderbookManager {
    /// Create a new orderbook manager for the given symbols.
    pub fn new(symbols: &[String]) -> Self {
        let symbols: Vec<Symbol> = symbols.iter().map(|s| Symbol::new(s)).collect();
        let orderbooks = symbols.iter().map(|s| CurrentOrderbook::new(*s)).collect();

        Self { orderbooks, symbols }
    }

    /// Get the orderbook for a symbol.
    pub fn get(&self, symbol: &str) -> Option<&CurrentOrderbook> {
        let sym = Symbol::new(symbol);
        self.symbols
            .iter()
            .position(|s| s == &sym)
            .map(|idx| &self.orderbooks[idx])
    }

    /// Get all orderbooks.
    pub fn all(&self) -> &[CurrentOrderbook] {
        &self.orderbooks
    }

    /// Get the number of orderbooks.
    pub fn len(&self) -> usize {
        self.orderbooks.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.orderbooks.is_empty()
    }

    /// Iterate over all symbols.
    pub fn symbols(&self) -> impl Iterator<Item = &Symbol> {
        self.symbols.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PriceLevel;

    #[test]
    fn test_triple_buffer_basic() {
        let ob = CurrentOrderbook::new(Symbol::new("TEST-USD"));

        // Initially no data
        assert!(!ob.has_data());
        assert!(ob.read().is_none());

        // Write some data
        ob.update(|snapshot| {
            snapshot.bids[0] = PriceLevel::new(100.0, 1.0);
            snapshot.bid_count = 1;
            snapshot.asks[0] = PriceLevel::new(101.0, 1.0);
            snapshot.ask_count = 1;
        });

        // Now we should have data
        assert!(ob.has_data());
        let snapshot = ob.read().unwrap();
        assert_eq!(snapshot.best_bid_price(), Some(100.0));
        assert_eq!(snapshot.best_ask_price(), Some(101.0));
    }

    #[test]
    fn test_triple_buffer_updates() {
        let ob = CurrentOrderbook::new(Symbol::new("TEST-USD"));

        // Multiple updates
        for i in 0..10 {
            ob.update(|snapshot| {
                snapshot.bids[0] = PriceLevel::new(100.0 + i as f64, 1.0);
                snapshot.bid_count = 1;
                snapshot.sequence = i;
            });
        }

        let snapshot = ob.read().unwrap();
        assert_eq!(snapshot.sequence, 9);
        assert_eq!(snapshot.best_bid_price(), Some(109.0));
    }

    #[test]
    fn test_orderbook_manager() {
        let manager = OrderbookManager::new(&["TEST-USD".to_string(), "ETH-USD".to_string()]);

        assert_eq!(manager.len(), 2);

        let btc = manager.get("TEST-USD").unwrap();
        btc.update(|s| {
            s.bids[0] = PriceLevel::new(50000.0, 1.0);
            s.bid_count = 1;
        });

        let eth = manager.get("ETH-USD").unwrap();
        eth.update(|s| {
            s.bids[0] = PriceLevel::new(3000.0, 1.0);
            s.bid_count = 1;
        });

        assert_eq!(manager.get("TEST-USD").unwrap().best_bid(), Some(50000.0));
        assert_eq!(manager.get("ETH-USD").unwrap().best_bid(), Some(3000.0));
        assert!(manager.get("SOL-USD").is_none());
    }
}
