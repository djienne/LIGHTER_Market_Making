//! Quote strategy trait for modular strategy implementations.
//!
//! This module defines the `QuoteStrategy` trait which allows different
//! quote calculation strategies to be plugged in while maintaining
//! a consistent interface.
//!
//! # Design Principles
//!
//! - **Low Latency**: The `update()` method is the hot path and should
//!   complete as fast as possible. Use `#[inline]` and avoid allocations.
//! - **Static Dispatch**: Use concrete types (not `Box<dyn>`) in performance-
//!   critical code to enable inlining and avoid vtable lookups.
//! - **Full Access**: Strategies receive the complete `OrderbookSnapshot`
//!   allowing arbitrary calculations on the order book data.
//!
//! # Example
//!
//! ```ignore
//! use standx_orderbook::strategy::{QuoteStrategy, Quote};
//! use standx_orderbook::types::OrderbookSnapshot;
//!
//! struct MyStrategy {
//!     // ... strategy state
//! }
//!
//! impl QuoteStrategy for MyStrategy {
//!     fn update(&mut self, snapshot: &OrderbookSnapshot) -> Option<Quote> {
//!         // Calculate bid/ask from snapshot
//!         // ...
//!     }
//!     // ... implement other trait methods
//! }
//! ```

use crate::types::OrderbookSnapshot;
use super::quotes::Quote;

/// Trait for quote calculation strategies.
///
/// Strategies receive orderbook snapshots and return bid/ask quotes.
/// Implementations should prioritize low latency - the `update()` method
/// is called on every orderbook message.
///
/// # Hot Path
///
/// The `update()` method is the hot path. For minimum latency:
/// - Mark it `#[inline]` in implementations
/// - Avoid heap allocations
/// - Use simple branching (predictable for CPU branch predictor)
/// - Keep state in contiguous memory (cache-friendly)
pub trait QuoteStrategy {
    /// Process a new orderbook snapshot and optionally return a quote.
    ///
    /// This is the **HOT PATH** - should complete as fast as possible.
    /// Called on every orderbook update.
    ///
    /// Returns `None` if:
    /// - Not enough data yet (warmup period)
    /// - Rate limiting (update_interval_steps not reached)
    /// - Invalid snapshot data
    ///
    /// The returned `Quote` contains:
    /// - `bid_price` / `ask_price`: Absolute prices for limit orders
    /// - `quantity`: Order size in base asset
    /// - `valid_for_trading`: Whether quote should be used for real orders
    fn update(&mut self, snapshot: &OrderbookSnapshot) -> Option<Quote>;

    /// Set the current position for skew calculation.
    ///
    /// Position is in base asset units (e.g., BTC for TEST-USD).
    /// - Positive = long position
    /// - Negative = short position
    ///
    /// Strategies typically use position to skew quotes:
    /// - When long: widen bid, tighten ask (encourage selling)
    /// - When short: tighten bid, widen ask (encourage buying)
    fn set_position(&mut self, position: f64);

    /// Get the current position.
    fn position(&self) -> f64;

    /// Check if the strategy has enough history for actual trading.
    ///
    /// This is more stringent than `is_warmed_up()`. For example,
    /// OBI strategy requires 10 minutes of history for reliable
    /// volatility estimates before placing real orders.
    fn is_valid_for_trading(&self) -> bool;

    /// Check if the strategy is warmed up (has enough data to quote).
    ///
    /// Warmed up means the strategy can produce quotes for display,
    /// but they may not be reliable enough for actual trading.
    /// See `is_valid_for_trading()` for trading readiness.
    fn is_warmed_up(&self) -> bool;

    /// Get current volatility estimate.
    ///
    /// Used for logging and debugging. Units are strategy-specific
    /// (e.g., ticks per second for OBI strategy).
    fn volatility(&self) -> f64;

    /// Get current alpha/signal value.
    ///
    /// Used for logging and debugging. Meaning is strategy-specific
    /// (e.g., imbalance z-score for OBI strategy).
    fn alpha(&self) -> f64;

    /// Get the history duration in seconds.
    ///
    /// How much historical data the strategy has accumulated.
    fn history_duration_secs(&self) -> f64;

    /// Get the required history duration in seconds.
    ///
    /// How much history is needed before `is_valid_for_trading()` returns true.
    fn required_history_secs(&self) -> f64;

    /// Reset the strategy state.
    ///
    /// Clears all accumulated data. Use when:
    /// - Switching symbols
    /// - After extended disconnection
    /// - For testing
    fn reset(&mut self);
}
