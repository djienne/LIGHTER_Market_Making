//! Lock-free atomic storage for Binance alpha and volatility signals.
//!
//! Provides sub-nanosecond read access for the hot path while allowing
//! concurrent updates from the Binance WebSocket poller.
//!
//! ## Performance Characteristics
//!
//! - Read (alpha/volatility): ~1ns (single atomic load)
//! - Write (update): ~1ns (three atomic stores)
//! - Memory: 64 bytes (cache-line aligned to avoid false sharing)
//!
//! ## Usage
//!
//! ```ignore
//! use std::sync::Arc;
//! use standx_orderbook::binance::SharedAlpha;
//!
//! let shared = Arc::new(SharedAlpha::new());
//!
//! // Writer (Binance poller task)
//! shared.update(0.5, 0.001);
//!
//! // Reader (hot path - lock-free)
//! if shared.is_warmed_up() && !shared.is_stale(5000) {
//!     let alpha = shared.alpha();
//!     let vol = shared.volatility();
//! }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Lock-free atomic storage for alpha and volatility from Binance.
///
/// Uses `AtomicU64` with f64-to-bits conversion for lock-free reads.
/// Cache-line aligned to avoid false sharing with other data.
#[repr(align(64))]
pub struct SharedAlpha {
    /// Alpha (imbalance z-score) stored as f64 bits
    alpha_bits: AtomicU64,

    /// Volatility stored as f64 bits
    volatility_bits: AtomicU64,

    /// Last update timestamp in milliseconds (for staleness check)
    last_update_ms: AtomicU64,

    /// Number of samples (for warmup detection)
    sample_count: AtomicU64,
}

impl SharedAlpha {
    /// Create a new SharedAlpha with zero values.
    pub fn new() -> Self {
        Self {
            alpha_bits: AtomicU64::new(0.0_f64.to_bits()),
            volatility_bits: AtomicU64::new(0.0_f64.to_bits()),
            last_update_ms: AtomicU64::new(0),
            sample_count: AtomicU64::new(0),
        }
    }

    /// Get the current alpha value (lock-free, ~1ns).
    #[inline]
    pub fn alpha(&self) -> f64 {
        f64::from_bits(self.alpha_bits.load(Ordering::Relaxed))
    }

    /// Get the current volatility value (lock-free, ~1ns).
    #[inline]
    pub fn volatility(&self) -> f64 {
        f64::from_bits(self.volatility_bits.load(Ordering::Relaxed))
    }

    /// Get the last update timestamp in milliseconds.
    #[inline]
    pub fn last_update_ms(&self) -> u64 {
        self.last_update_ms.load(Ordering::Relaxed)
    }

    /// Get the sample count.
    #[inline]
    pub fn sample_count(&self) -> u64 {
        self.sample_count.load(Ordering::Relaxed)
    }

    /// Check if enough samples have been collected (warmup complete).
    ///
    /// Requires at least 150 samples (~15 seconds at 100ms intervals).
    #[inline]
    pub fn is_warmed_up(&self) -> bool {
        self.sample_count.load(Ordering::Relaxed) >= 150
    }

    /// Check if the alpha is stale (no update within threshold_ms).
    ///
    /// # Arguments
    /// * `threshold_ms` - Maximum allowed age in milliseconds
    #[inline]
    pub fn is_stale(&self, threshold_ms: u64) -> bool {
        let last = self.last_update_ms.load(Ordering::Relaxed);
        let now = current_time_ms();
        now.saturating_sub(last) > threshold_ms
    }

    /// Update alpha and volatility values.
    ///
    /// Called by the Binance poller task on each orderbook update.
    /// This increments the sample count automatically.
    #[inline]
    pub fn update(&self, alpha: f64, volatility: f64) {
        self.alpha_bits.store(alpha.to_bits(), Ordering::Relaxed);
        self.volatility_bits.store(volatility.to_bits(), Ordering::Relaxed);
        self.last_update_ms.store(current_time_ms(), Ordering::Relaxed);
        self.sample_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Reset the shared state (clear all values and sample count).
    ///
    /// Called on Binance WebSocket reconnect to force warmup.
    pub fn reset(&self) {
        self.alpha_bits.store(0.0_f64.to_bits(), Ordering::Relaxed);
        self.volatility_bits.store(0.0_f64.to_bits(), Ordering::Relaxed);
        self.last_update_ms.store(0, Ordering::Relaxed);
        self.sample_count.store(0, Ordering::Relaxed);
    }
}

impl Default for SharedAlpha {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current time in milliseconds since UNIX epoch.
#[inline]
fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before UNIX epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_shared_alpha() {
        let shared = SharedAlpha::new();
        assert_eq!(shared.alpha(), 0.0);
        assert_eq!(shared.volatility(), 0.0);
        assert_eq!(shared.sample_count(), 0);
        assert!(!shared.is_warmed_up());
    }

    #[test]
    fn test_update_and_read() {
        let shared = SharedAlpha::new();
        shared.update(0.5, 0.001);

        assert!((shared.alpha() - 0.5).abs() < 1e-10);
        assert!((shared.volatility() - 0.001).abs() < 1e-10);
        assert_eq!(shared.sample_count(), 1);
    }

    #[test]
    fn test_warmup() {
        let shared = SharedAlpha::new();

        // Not warmed up yet
        assert!(!shared.is_warmed_up());

        // Add 150 samples
        for _ in 0..150 {
            shared.update(0.1, 0.001);
        }

        assert!(shared.is_warmed_up());
        assert_eq!(shared.sample_count(), 150);
    }

    #[test]
    fn test_staleness() {
        let shared = SharedAlpha::new();

        // Should be stale with no updates
        assert!(shared.is_stale(1000));

        // Update and check freshness
        shared.update(0.1, 0.001);
        assert!(!shared.is_stale(1000));
    }

    #[test]
    fn test_reset() {
        let shared = SharedAlpha::new();

        // Add some samples
        for _ in 0..200 {
            shared.update(0.5, 0.002);
        }
        assert!(shared.is_warmed_up());
        assert!(shared.sample_count() >= 200);

        // Reset
        shared.reset();
        assert_eq!(shared.alpha(), 0.0);
        assert_eq!(shared.volatility(), 0.0);
        assert_eq!(shared.sample_count(), 0);
        assert!(!shared.is_warmed_up());
    }

    #[test]
    fn test_negative_alpha() {
        let shared = SharedAlpha::new();
        shared.update(-0.75, 0.0015);

        assert!((shared.alpha() - (-0.75)).abs() < 1e-10);
        assert!((shared.volatility() - 0.0015).abs() < 1e-10);
    }
}
