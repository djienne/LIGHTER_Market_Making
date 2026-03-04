//! Lock-free equity storage for hot path access.
//!
//! This module provides `SharedEquity` which stores equity and pre-calculates
//! order_qty_dollar and max_position_dollar on write to avoid division in the hot path.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Safety margin for max_position calculation (10% = 0.10).
/// This provides a cushion for price movements, funding rates, and unrealized PnL.
const MAX_POSITION_SAFETY_MARGIN: f64 = 0.10;

/// Lock-free equity storage for hot path access.
///
/// Pre-calculates order_qty_dollar and max_position_dollar on write to avoid division in hot path.
///
/// ## Order Quantity Formula
/// `order_qty_dollar = equity / 5 * 0.9 / order_levels`
///
/// With $500 equity and 2 order levels:
/// - $500 / 5 = $100 (use 20% of capital)
/// - $100 * 0.9 = $90 (10% safety margin)
/// - $90 / 2 = $45 per order (split across levels)
///
/// ## Max Position Formula
/// `max_position_dollar = (equity - order_reserve) * (1.0 - safety_margin)`
/// Where `order_reserve = order_qty * order_levels` (margin blocked by bid orders)
///
/// With $2000 equity, $200/order, 2 levels, and 10% safety margin:
/// - order_reserve = $200 * 2 = $400
/// - max_position = ($2000 - $400) * 0.9 = $1440
///
/// This ensures at max position we still have margin for all bid orders.
pub struct SharedEquity {
    /// Raw equity in USD, stored as f64 bits
    equity_bits: AtomicU64,
    /// Pre-calculated order_qty_dollar per order, stored as f64 bits
    order_qty_dollar_bits: AtomicU64,
    /// Pre-calculated max_position_dollar, stored as f64 bits
    max_position_dollar_bits: AtomicU64,
    /// Number of order levels (for formula)
    order_levels: AtomicU8,
    /// Minimum order size in USD
    min_order_qty_dollar: f64,
    /// Leverage multiplier (1.0–5.0), immutable after construction
    leverage: f64,
    /// Last update timestamp (Unix millis)
    last_update_ms: AtomicU64,
}

impl SharedEquity {
    /// Create a new SharedEquity with the given order levels, minimum order size, and leverage.
    ///
    /// # Arguments
    ///
    /// * `order_levels` - Number of order levels (1 or 2)
    /// * `min_order_qty_dollar` - Minimum order size in USD (prevents dust orders)
    /// * `leverage` - Leverage multiplier (1.0–5.0). Multiplies effective capital.
    pub fn new(order_levels: usize, min_order_qty_dollar: f64, leverage: f64) -> Self {
        Self {
            equity_bits: AtomicU64::new(0.0f64.to_bits()),
            order_qty_dollar_bits: AtomicU64::new(0.0f64.to_bits()),
            max_position_dollar_bits: AtomicU64::new(0.0f64.to_bits()),
            order_levels: AtomicU8::new(order_levels as u8),
            min_order_qty_dollar,
            leverage,
            last_update_ms: AtomicU64::new(0),
        }
    }

    /// Lock-free read of pre-calculated order_qty_dollar.
    ///
    /// Returns 0.0 if equity not yet fetched.
    /// This is designed for the hot path (~1ns latency).
    #[inline]
    pub fn order_qty_dollar(&self) -> f64 {
        f64::from_bits(self.order_qty_dollar_bits.load(Ordering::Acquire))
    }

    /// Lock-free read of raw equity.
    #[inline]
    pub fn equity(&self) -> f64 {
        f64::from_bits(self.equity_bits.load(Ordering::Acquire))
    }

    /// Lock-free read of pre-calculated max_position_dollar (~1ns).
    ///
    /// Returns 0.0 if equity not yet fetched.
    /// This is designed for the hot path.
    #[inline]
    pub fn max_position_dollar(&self) -> f64 {
        f64::from_bits(self.max_position_dollar_bits.load(Ordering::Acquire))
    }

    /// Check if equity has been initialized (> 0).
    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.equity() > 0.0
    }

    /// Update equity and recalculate order_qty_dollar and max_position_dollar.
    ///
    /// Called from WalletTracker background task (cold path, 60s interval).
    /// The formula division happens here, not in the hot path.
    ///
    /// ## Order Quantity Formula
    /// `order_qty_dollar = equity * leverage / 5 * 0.9 / order_levels`
    ///
    /// ## Max Position Formula
    /// `max_position_dollar = (equity * leverage - order_reserve) * (1.0 - safety_margin)`
    /// Where:
    /// - `order_reserve = order_qty * order_levels` (margin blocked by bid orders)
    /// - `safety_margin = 0.10` (10% buffer for price movements, funding, PnL)
    ///
    /// This ensures at max position we still have margin for all bid orders,
    /// preventing order rejections due to insufficient available margin.
    pub fn set_equity(&self, equity: f64) {
        self.equity_bits.store(equity.to_bits(), Ordering::Release);

        // Apply leverage to get effective capital
        let effective = equity * self.leverage;

        // Calculate order_qty: effective / 5 * 0.9 / order_levels
        let levels = self.order_levels.load(Ordering::Acquire) as f64;
        let raw_qty = effective / 5.0 * 0.9 / levels;
        let qty = raw_qty.max(self.min_order_qty_dollar);

        self.order_qty_dollar_bits
            .store(qty.to_bits(), Ordering::Release);

        // Calculate max_position: (effective - order_reserve) * (1.0 - safety_margin)
        // order_reserve = order_qty * levels (margin blocked by all bid orders)
        // Example: $2000 equity, 3x leverage, $540/order, 2 levels -> ($6000 - $1080) * 0.9 = $4428
        let order_reserve = qty * levels;
        let max_pos = (effective - order_reserve) * (1.0 - MAX_POSITION_SAFETY_MARGIN);
        let max_pos = max_pos.max(0.0); // Floor at 0 for edge cases (low equity)
        self.max_position_dollar_bits
            .store(max_pos.to_bits(), Ordering::Release);

        self.last_update_ms.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            Ordering::Release,
        );
    }

    /// Get milliseconds since last update.
    pub fn age_ms(&self) -> u64 {
        let last = self.last_update_ms.load(Ordering::Acquire);
        if last == 0 {
            return u64::MAX;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        now.saturating_sub(last)
    }

    /// Get the minimum order quantity in USD.
    #[inline]
    pub fn min_order_qty_dollar(&self) -> f64 {
        self.min_order_qty_dollar
    }

    /// Get the leverage multiplier.
    #[inline]
    pub fn leverage(&self) -> f64 {
        self.leverage
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_equity_new() {
        let equity = SharedEquity::new(2, 10.0, 1.0);
        assert_eq!(equity.equity(), 0.0);
        assert_eq!(equity.order_qty_dollar(), 0.0);
        assert_eq!(equity.max_position_dollar(), 0.0);
        assert!(!equity.is_initialized());
    }

    #[test]
    fn test_shared_equity_formula() {
        // Test with $500 equity and 2 order levels
        // order_qty: $500 / 5 * 0.9 / 2 = $45
        // order_reserve: $45 * 2 = $90
        // max_position: ($500 - $90) * 0.9 = $410 * 0.9 = $369
        let equity = SharedEquity::new(2, 10.0, 1.0);
        equity.set_equity(500.0);

        assert!(equity.is_initialized());
        assert_eq!(equity.equity(), 500.0);
        assert_eq!(equity.order_qty_dollar(), 45.0);
        assert_eq!(equity.max_position_dollar(), 369.0);
    }

    #[test]
    fn test_shared_equity_formula_single_level() {
        // Test with $500 equity and 1 order level
        // order_qty: $500 / 5 * 0.9 / 1 = $90
        // order_reserve: $90 * 1 = $90
        // max_position: ($500 - $90) * 0.9 = $410 * 0.9 = $369
        let equity = SharedEquity::new(1, 10.0, 1.0);
        equity.set_equity(500.0);

        assert_eq!(equity.order_qty_dollar(), 90.0);
        assert_eq!(equity.max_position_dollar(), 369.0);
    }

    #[test]
    fn test_shared_equity_min_floor() {
        // Test with very low equity (below min_order_qty_dollar floor)
        // order_qty: $10 / 5 * 0.9 / 2 = $0.9, below min -> 10.0
        // order_reserve: $10 * 2 = $20
        // max_position: ($10 - $20) * 0.9 = -$10 * 0.9 = -$9 -> floored to 0
        let equity = SharedEquity::new(2, 10.0, 1.0);
        equity.set_equity(10.0);

        assert_eq!(equity.order_qty_dollar(), 10.0);
        assert_eq!(equity.max_position_dollar(), 0.0); // Floored at 0
    }

    #[test]
    fn test_shared_equity_zero() {
        // Test with zero equity
        // order_qty floors to min_order_qty_dollar = 10.0
        // order_reserve: $10 * 2 = $20
        // max_position: ($0 - $20) * 0.9 = -$18 -> floored to 0
        let equity = SharedEquity::new(2, 10.0, 1.0);
        equity.set_equity(0.0);

        assert!(!equity.is_initialized());
        assert_eq!(equity.order_qty_dollar(), 10.0); // Falls back to min
        assert_eq!(equity.max_position_dollar(), 0.0); // Floored at 0
    }

    #[test]
    fn test_shared_equity_age() {
        let equity = SharedEquity::new(2, 10.0, 1.0);

        // Before any update, age should be MAX
        assert_eq!(equity.age_ms(), u64::MAX);

        // After update, age should be small
        equity.set_equity(100.0);
        assert!(equity.age_ms() < 1000); // Less than 1 second
    }

    #[test]
    fn test_max_position_with_order_reserve() {
        // Test with $2000 equity and 2 order levels
        // order_qty: $2000 / 5 * 0.9 / 2 = $180
        // order_reserve: $180 * 2 = $360
        // max_position: ($2000 - $360) * 0.9 = $1640 * 0.9 = $1476
        let equity = SharedEquity::new(2, 10.0, 1.0);
        equity.set_equity(2000.0);

        assert_eq!(equity.order_qty_dollar(), 180.0);
        assert_eq!(equity.max_position_dollar(), 1476.0);
    }

    #[test]
    fn test_max_position_realistic_equity() {
        // Test with realistic equity ($2227.59 from current run)
        // order_qty: $2227.59 / 5 * 0.9 / 2 = $200.48 (approx)
        // order_reserve: $200.48 * 2 = $400.97
        // max_position: ($2227.59 - $400.97) * 0.9 = $1826.62 * 0.9 = $1643.96
        let equity = SharedEquity::new(2, 10.0, 1.0);
        equity.set_equity(2227.59);

        let order_qty = equity.order_qty_dollar();
        let max_pos = equity.max_position_dollar();

        // Check order_qty is approximately $200.48
        assert!((order_qty - 200.4831).abs() < 0.01);
        // Check max_position is approximately $1643.96
        assert!((max_pos - 1643.96).abs() < 0.1);
    }

    #[test]
    fn test_shared_equity_with_leverage() {
        // Test with $2000 equity, 3x leverage, 2 order levels
        // effective = $2000 * 3 = $6000
        // order_qty: $6000 / 5 * 0.9 / 2 = $540
        // order_reserve: $540 * 2 = $1080
        // max_position: ($6000 - $1080) * 0.9 = $4920 * 0.9 = $4428
        let equity = SharedEquity::new(2, 10.0, 3.0);
        equity.set_equity(2000.0);

        assert_eq!(equity.equity(), 2000.0); // Raw equity unchanged
        assert_eq!(equity.leverage(), 3.0);
        assert_eq!(equity.order_qty_dollar(), 540.0);
        assert_eq!(equity.max_position_dollar(), 4428.0);
    }
}
