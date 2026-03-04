//! Orderbook management module.
//!
//! This module provides high-performance, lock-free data structures
//! for storing and accessing orderbook data:
//!
//! - [`CurrentOrderbook`]: Triple buffer for the latest orderbook state
//! - [`OrderbookHistory`]: Ring buffer for historical snapshots
//! - [`OrderbookManager`]: Manager for multiple symbols

mod history;
pub mod sanity_check;
mod snapshot;

pub use history::OrderbookHistory;
pub use sanity_check::{OrderbookSanityChecker, SanityCheckerConfig, SanityCheckerHandle, SanityCheckerStats};
pub use snapshot::{CurrentOrderbook, OrderbookManager};

use crate::types::OrderbookSnapshot;

/// Combined orderbook state for a single symbol.
///
/// Provides both current state access and historical data.
pub struct SymbolOrderbook {
    /// Current orderbook state (triple buffer)
    pub current: CurrentOrderbook,
    /// Historical snapshots (ring buffer)
    pub history: OrderbookHistory,
}

impl SymbolOrderbook {
    /// Create a new symbol orderbook.
    ///
    /// # Arguments
    ///
    /// * `symbol` - Trading symbol (e.g., "TEST-USD")
    /// * `history_capacity` - Number of historical snapshots to store
    /// * `retention_minutes` - How long to retain historical data
    pub fn new(symbol: &str, history_capacity: usize, retention_minutes: u64) -> Self {
        let sym = crate::types::Symbol::new(symbol);
        Self {
            current: CurrentOrderbook::new(sym),
            history: OrderbookHistory::new(history_capacity, retention_minutes),
        }
    }

    /// Update the orderbook with a new snapshot.
    ///
    /// Updates both the current state and adds to history.
    pub fn update(&self, snapshot: OrderbookSnapshot) {
        // Update current state
        self.current.update_snapshot(snapshot.clone());

        // Add to history
        self.history.push(snapshot);
    }

    /// Get the latest orderbook snapshot.
    pub fn latest(&self) -> Option<OrderbookSnapshot> {
        self.current.read()
    }

    /// Get a snapshot without swapping indices.
    ///
    /// This is safe for concurrent readers (e.g., sanity checker)
    /// as it reads from the stable swap buffer without modifying state.
    pub fn peek(&self) -> Option<OrderbookSnapshot> {
        self.current.peek()
    }

    /// Get the best bid price.
    #[inline]
    pub fn best_bid(&self) -> Option<f64> {
        self.current.best_bid()
    }

    /// Get the best ask price.
    #[inline]
    pub fn best_ask(&self) -> Option<f64> {
        self.current.best_ask()
    }

    /// Get the current spread.
    #[inline]
    pub fn spread(&self) -> Option<f64> {
        self.current.spread()
    }

    /// Get the current mid price.
    #[inline]
    pub fn mid_price(&self) -> Option<f64> {
        self.current.mid_price()
    }

    /// Get recent historical snapshots.
    pub fn recent_history(&self) -> impl Iterator<Item = OrderbookSnapshot> + '_ {
        self.history.recent_snapshots()
    }

    /// Calculate average spread over recent history.
    pub fn average_spread(&self) -> Option<f64> {
        let spreads: Vec<f64> = self.recent_history()
            .filter_map(|s| s.spread())
            .collect();

        if spreads.is_empty() {
            return None;
        }

        Some(spreads.iter().sum::<f64>() / spreads.len() as f64)
    }

    /// Calculate spread volatility (standard deviation) over recent history.
    pub fn spread_volatility(&self) -> Option<f64> {
        let spreads: Vec<f64> = self.recent_history()
            .filter_map(|s| s.spread())
            .collect();

        if spreads.len() < 2 {
            return None;
        }

        let mean = spreads.iter().sum::<f64>() / spreads.len() as f64;
        let variance = spreads.iter()
            .map(|s| (s - mean).powi(2))
            .sum::<f64>() / (spreads.len() - 1) as f64;

        Some(variance.sqrt())
    }
}

/// Manager for multiple symbol orderbooks with history.
pub struct OrderbookStore {
    /// Orderbooks by symbol
    orderbooks: Vec<SymbolOrderbook>,
    /// Symbol names for lookup
    symbols: Vec<String>,
}

impl OrderbookStore {
    /// Create a new orderbook store.
    ///
    /// # Arguments
    ///
    /// * `symbols` - List of trading symbols
    /// * `history_capacity` - Number of historical snapshots per symbol
    /// * `retention_minutes` - How long to retain historical data
    pub fn new(symbols: &[String], history_capacity: usize, retention_minutes: u64) -> Self {
        let orderbooks = symbols
            .iter()
            .map(|s| SymbolOrderbook::new(s, history_capacity, retention_minutes))
            .collect();

        Self {
            orderbooks,
            symbols: symbols.to_vec(),
        }
    }

    /// Get the orderbook for a symbol.
    pub fn get(&self, symbol: &str) -> Option<&SymbolOrderbook> {
        self.symbols
            .iter()
            .position(|s| s == symbol)
            .map(|idx| &self.orderbooks[idx])
    }

    /// Get all orderbooks.
    pub fn all(&self) -> &[SymbolOrderbook] {
        &self.orderbooks
    }

    /// Get the number of symbols.
    pub fn len(&self) -> usize {
        self.orderbooks.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.orderbooks.is_empty()
    }

    /// Get all symbol names.
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// Get statistics for all orderbooks.
    pub fn stats(&self) -> Vec<OrderbookStats> {
        self.orderbooks
            .iter()
            .zip(self.symbols.iter())
            .map(|(ob, symbol)| OrderbookStats {
                symbol: symbol.clone(),
                has_data: ob.current.has_data(),
                update_count: ob.current.sequence(),
                history_count: ob.history.len(),
                history_total_writes: ob.history.total_writes(),
                best_bid: ob.best_bid(),
                best_ask: ob.best_ask(),
                spread: ob.spread(),
            })
            .collect()
    }
}

/// Statistics for a single orderbook.
#[derive(Debug, Clone)]
pub struct OrderbookStats {
    pub symbol: String,
    pub has_data: bool,
    pub update_count: u64,
    pub history_count: usize,
    pub history_total_writes: u64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_orderbook() {
        let ob = SymbolOrderbook::new("TEST-USD", 1000, 10);

        let mut snapshot = OrderbookSnapshot::new(crate::types::Symbol::new("TEST-USD"));
        snapshot.set_bids(&[(100.0, 1.0)], 20);
        snapshot.set_asks(&[(101.0, 1.0)], 20);

        ob.update(snapshot);

        assert_eq!(ob.best_bid(), Some(100.0));
        assert_eq!(ob.best_ask(), Some(101.0));
        assert_eq!(ob.spread(), Some(1.0));
    }

    #[test]
    fn test_orderbook_store() {
        let store = OrderbookStore::new(
            &["TEST-USD".to_string(), "ETH-USD".to_string()],
            1000,
            10,
        );

        assert_eq!(store.len(), 2);
        assert!(store.get("TEST-USD").is_some());
        assert!(store.get("ETH-USD").is_some());
        assert!(store.get("SOL-USD").is_none());
    }
}
