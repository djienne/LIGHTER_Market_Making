//! Binance orderbook connector module.
//!
//! Provides a WebSocket client for streaming Binance orderbook data with
//! incremental updates and OBI (Order Book Imbalance) calculation.
//!
//! ## Features
//!
//! - **Low-latency WebSocket streaming**: 100ms update interval
//! - **Automatic synchronization**: REST snapshot + WebSocket delta approach
//! - **Lock-free snapshot conversion**: Produces `OrderbookSnapshot` for strategy
//! - **OBI calculation**: Z-score based imbalance signal with volatility
//! - **Auto-reconnect**: Exponential backoff with 24-hour connection limit handling
//!
//! ## Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use standx_orderbook::binance::{BinanceClient, BinanceEvent, BinanceObiCalculator};
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = Arc::new(BinanceClient::new("btcusdt"));
//!     let mut rx = client.clone().run().await;
//!     let mut obi = BinanceObiCalculator::new(300, 0.025); // 5-min window, 2.5% depth
//!
//!     while let Some(event) = rx.recv().await {
//!         match event {
//!             BinanceEvent::Snapshot(snapshot) => {
//!                 if let Some((alpha, vol)) = obi.update(&snapshot) {
//!                     println!("Alpha: {:.4}, Vol: {:.6}", alpha, vol);
//!                 }
//!             }
//!             BinanceEvent::Error(e) => eprintln!("Error: {}", e),
//!             _ => {}
//!         }
//!     }
//! }
//! ```

pub mod client;
pub mod messages;
pub mod orderbook;
pub mod shared_alpha;

pub use client::{
    BinanceClient, BinanceClientConfig, BinanceEvent, BinanceWsStats, BinanceWsStatsSnapshot,
    MarketType, BINANCE_FUTURES_REST_URL, BINANCE_FUTURES_WS_URL, BINANCE_REST_URL, BINANCE_WS_URL,
};
pub use messages::{BinanceDepthSnapshot, BinanceDepthUpdate, ParseError};
pub use orderbook::{BinanceOrderbook, SyncError};
pub use shared_alpha::SharedAlpha;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::strategy::RollingStats;
use crate::types::OrderbookSnapshot;

/// OBI (Order Book Imbalance) calculator for Binance orderbook data.
///
/// Calculates a z-score based imbalance signal and rolling volatility
/// suitable for market making strategies.
///
/// The looking depth parameter specifies how deep into the book to calculate
/// imbalance as a percentage of mid price (e.g., 0.025 = 2.5% from mid).
#[derive(Debug)]
pub struct BinanceObiCalculator {
    /// Rolling statistics for imbalance z-score
    imbalance_stats: RollingStats,

    /// Rolling statistics for mid price changes (volatility)
    mid_change_stats: RollingStats,

    /// Previous mid price for volatility calculation
    prev_mid: Option<f64>,

    /// Depth to look for imbalance calculation (as fraction of mid, e.g., 0.025)
    looking_depth: f64,

    /// Minimum samples before producing signals
    min_samples: usize,
}

impl BinanceObiCalculator {
    /// Create a new OBI calculator.
    ///
    /// # Arguments
    /// * `window_size` - Number of samples for rolling statistics (e.g., 300 for 30s at 100ms)
    /// * `looking_depth` - Depth as fraction of mid price (e.g., 0.025 for 2.5%)
    pub fn new(window_size: usize, looking_depth: f64) -> Self {
        Self {
            imbalance_stats: RollingStats::new(window_size),
            mid_change_stats: RollingStats::new(window_size),
            prev_mid: None,
            looking_depth,
            min_samples: window_size / 2, // Require at least half window before signaling
        }
    }

    /// Update statistics with a new orderbook snapshot.
    ///
    /// Returns `Some((alpha, volatility))` once warmed up, or `None` during warmup.
    ///
    /// * `alpha` - Imbalance z-score (positive = bid-heavy, negative = ask-heavy)
    /// * `volatility` - Rolling standard deviation of mid price returns
    pub fn update(&mut self, snapshot: &OrderbookSnapshot) -> Option<(f64, f64)> {
        let mid = snapshot.mid_price()?;

        // Calculate raw imbalance
        let imbalance = self.calculate_imbalance(snapshot, mid);
        self.imbalance_stats.push(imbalance);

        // Calculate mid price return for volatility
        if let Some(prev) = self.prev_mid {
            if prev > 0.0 {
                let ret = (mid - prev) / prev;
                self.mid_change_stats.push(ret);
            }
        }
        self.prev_mid = Some(mid);

        // Check warmup
        if self.imbalance_stats.len() < self.min_samples {
            return None;
        }

        // Calculate outputs
        let alpha = self.imbalance_stats.zscore(imbalance);
        let volatility = self.mid_change_stats.std();

        Some((alpha, volatility))
    }

    /// Get the current alpha (imbalance z-score) without updating.
    pub fn alpha(&self) -> f64 {
        self.imbalance_stats
            .latest()
            .map(|v| self.imbalance_stats.zscore(v))
            .unwrap_or(0.0)
    }

    /// Get the current volatility without updating.
    pub fn volatility(&self) -> f64 {
        self.mid_change_stats.std()
    }

    /// Check if the calculator is warmed up.
    pub fn is_warmed_up(&self) -> bool {
        self.imbalance_stats.len() >= self.min_samples
    }

    /// Get the number of samples collected.
    pub fn sample_count(&self) -> usize {
        self.imbalance_stats.len()
    }

    /// Clear all statistics (start fresh warmup).
    pub fn clear(&mut self) {
        self.imbalance_stats.clear();
        self.mid_change_stats.clear();
        self.prev_mid = None;
    }

    /// Get the minimum samples required for warmup.
    pub fn min_samples(&self) -> usize {
        self.min_samples
    }

    /// Calculate order book imbalance within the looking depth.
    ///
    /// Imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)
    /// Range: [-1, 1] where positive = bid-heavy, negative = ask-heavy
    fn calculate_imbalance(&self, snapshot: &OrderbookSnapshot, mid: f64) -> f64 {
        let min_price = mid * (1.0 - self.looking_depth);
        let max_price = mid * (1.0 + self.looking_depth);

        let mut bid_volume = 0.0;
        let mut ask_volume = 0.0;

        // Sum bid volume within depth (prices >= min_price)
        for level in snapshot.bid_levels() {
            if level.price >= min_price {
                bid_volume += level.quantity;
            } else {
                break; // Bids are sorted descending, so we can stop early
            }
        }

        // Sum ask volume within depth (prices <= max_price)
        for level in snapshot.ask_levels() {
            if level.price <= max_price {
                ask_volume += level.quantity;
            } else {
                break; // Asks are sorted ascending, so we can stop early
            }
        }

        let total = bid_volume + ask_volume;
        if total > 0.0 {
            (bid_volume - ask_volume) / total
        } else {
            0.0
        }
    }
}

// ============================================================================
// Binance Alpha Poller
// ============================================================================

/// Handle for the Binance alpha poller task.
pub struct BinanceAlphaPollerHandle {
    stop_flag: Arc<AtomicBool>,
    /// Receiver for events (for logging/monitoring)
    _event_rx: Option<mpsc::Receiver<BinanceEvent>>,
}

impl BinanceAlphaPollerHandle {
    /// Stop the poller gracefully.
    pub fn stop(&self) {
        self.stop_flag.store(true, Ordering::Release);
    }

    /// Check if the poller is running.
    pub fn is_running(&self) -> bool {
        !self.stop_flag.load(Ordering::Acquire)
    }
}

/// Start the Binance alpha poller as a background task.
///
/// This connects to Binance Futures WebSocket, calculates OBI alpha and volatility,
/// and updates the SharedAlpha atomically for lock-free reads from the hot path.
///
/// # Arguments
/// * `symbol` - Binance symbol (e.g., "btcusdt")
/// * `window_size` - Number of samples for rolling statistics (e.g., 300 for 30s at 100ms)
/// * `looking_depth` - Depth as fraction of mid price (e.g., 0.025 for 2.5%)
/// * `shared_alpha` - Arc to shared alpha storage for lock-free reads
///
/// # Returns
/// A handle to stop the poller and check status.
///
/// # Example
/// ```ignore
/// use std::sync::Arc;
/// use standx_orderbook::binance::{start_binance_alpha_poller, SharedAlpha};
///
/// let shared_alpha = Arc::new(SharedAlpha::new());
/// let handle = start_binance_alpha_poller(
///     "btcusdt",
///     300,    // 30s window at 100ms updates
///     0.025,  // 2.5% depth
///     Arc::clone(&shared_alpha),
/// );
///
/// // In hot path (lock-free, ~1-5ns):
/// if shared_alpha.is_warmed_up() && !shared_alpha.is_stale(5000) {
///     let alpha = shared_alpha.alpha();
/// }
///
/// // Stop when done
/// handle.stop();
/// ```
pub fn start_binance_alpha_poller(
    symbol: &str,
    window_size: usize,
    looking_depth: f64,
    shared_alpha: Arc<SharedAlpha>,
) -> BinanceAlphaPollerHandle {
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_flag_clone = Arc::clone(&stop_flag);
    let symbol = symbol.to_string();

    // Spawn the background task
    tokio::spawn(async move {
        info!(
            "Binance alpha poller starting for {} (window={}, depth={}%)",
            symbol,
            window_size,
            looking_depth * 100.0
        );

        // Run connection loop with reconnection
        loop {
            if stop_flag_clone.load(Ordering::Acquire) {
                info!("Binance alpha poller stopping (stop requested)");
                break;
            }

            // Create client and OBI calculator
            let client = Arc::new(BinanceClient::new(&symbol));
            let mut obi_calc = BinanceObiCalculator::new(window_size, looking_depth);

            // Reset shared alpha on reconnect (force warmup)
            shared_alpha.reset();

            // Start client
            let mut rx = client.clone().run().await;
            let mut logged_warmup = false;

            // Process events
            while let Some(event) = rx.recv().await {
                if stop_flag_clone.load(Ordering::Acquire) {
                    client.stop();
                    break;
                }

                match event {
                    BinanceEvent::Snapshot(snapshot) => {
                        // Update OBI calculator
                        if let Some((alpha, volatility)) = obi_calc.update(&snapshot) {
                            // Update shared alpha atomically
                            shared_alpha.update(alpha, volatility);

                            // Log warmup milestone once
                            if !logged_warmup && shared_alpha.is_warmed_up() {
                                info!(
                                    "Binance alpha warmed up ({} samples) - alpha={:.3}, vol={:.6}",
                                    shared_alpha.sample_count(),
                                    alpha,
                                    volatility
                                );
                                logged_warmup = true;
                            }
                        }
                    }
                    BinanceEvent::Synced => {
                        info!("Binance alpha poller synced with WebSocket");
                    }
                    BinanceEvent::Disconnected(reason) => {
                        warn!("Binance alpha poller disconnected: {}", reason);
                        // Reset OBI calculator on disconnect (will re-warmup)
                        obi_calc.clear();
                        // Break to reconnect
                        break;
                    }
                    BinanceEvent::Error(err) => {
                        debug!("Binance alpha poller error: {}", err);
                    }
                }
            }

            // Check if we should stop before reconnecting
            if stop_flag_clone.load(Ordering::Acquire) {
                break;
            }

            // Wait before reconnecting
            info!("Binance alpha poller reconnecting in 5 seconds...");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        info!("Binance alpha poller stopped");
    });

    BinanceAlphaPollerHandle {
        stop_flag,
        _event_rx: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PriceLevel, Symbol, MAX_LEVELS};

    fn make_snapshot(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> OrderbookSnapshot {
        let mut snapshot = OrderbookSnapshot::new(Symbol::new("BTCUSDT"));

        for (i, (price, qty)) in bids.iter().enumerate() {
            if i >= MAX_LEVELS {
                break;
            }
            snapshot.bids[i] = PriceLevel::new(*price, *qty);
        }
        snapshot.bid_count = bids.len().min(MAX_LEVELS) as u8;

        for (i, (price, qty)) in asks.iter().enumerate() {
            if i >= MAX_LEVELS {
                break;
            }
            snapshot.asks[i] = PriceLevel::new(*price, *qty);
        }
        snapshot.ask_count = asks.len().min(MAX_LEVELS) as u8;

        snapshot
    }

    #[test]
    fn test_obi_zscore_positive_imbalance() {
        let mut calc = BinanceObiCalculator::new(10, 0.01); // Small window for testing

        // Feed balanced orderbooks first
        for _ in 0..10 {
            let snapshot = make_snapshot(
                &[(100.0, 10.0), (99.0, 10.0)],
                &[(101.0, 10.0), (102.0, 10.0)],
            );
            calc.update(&snapshot);
        }

        // Now feed bid-heavy orderbook
        let bid_heavy = make_snapshot(
            &[(100.0, 100.0), (99.0, 100.0)], // Much more bid volume
            &[(101.0, 10.0), (102.0, 10.0)],
        );

        let result = calc.update(&bid_heavy);
        assert!(result.is_some());

        let (alpha, _vol) = result.unwrap();
        assert!(alpha > 0.0, "Bid-heavy book should produce positive alpha, got {}", alpha);
    }

    #[test]
    fn test_obi_zscore_negative_imbalance() {
        let mut calc = BinanceObiCalculator::new(10, 0.01);

        // Feed balanced orderbooks first
        for _ in 0..10 {
            let snapshot = make_snapshot(
                &[(100.0, 10.0), (99.0, 10.0)],
                &[(101.0, 10.0), (102.0, 10.0)],
            );
            calc.update(&snapshot);
        }

        // Now feed ask-heavy orderbook
        let ask_heavy = make_snapshot(
            &[(100.0, 10.0), (99.0, 10.0)],
            &[(101.0, 100.0), (102.0, 100.0)], // Much more ask volume
        );

        let result = calc.update(&ask_heavy);
        assert!(result.is_some());

        let (alpha, _vol) = result.unwrap();
        assert!(alpha < 0.0, "Ask-heavy book should produce negative alpha, got {}", alpha);
    }

    #[test]
    fn test_obi_warmup_period() {
        let mut calc = BinanceObiCalculator::new(20, 0.01);

        // First update should return None (warmup)
        let snapshot = make_snapshot(
            &[(100.0, 10.0)],
            &[(101.0, 10.0)],
        );

        let result = calc.update(&snapshot);
        assert!(result.is_none(), "Should return None during warmup");
        assert!(!calc.is_warmed_up());

        // Fill window to minimum (half of 20 = 10)
        for _ in 0..9 {
            calc.update(&snapshot);
        }

        // Now should be warmed up
        let result = calc.update(&snapshot);
        assert!(result.is_some(), "Should return Some after warmup");
        assert!(calc.is_warmed_up());
    }

    #[test]
    fn test_obi_volatility_tracking() {
        let mut calc = BinanceObiCalculator::new(10, 0.01);

        // Feed snapshots with changing mid prices
        let mids = [100.0, 100.1, 99.9, 100.2, 99.8, 100.0, 100.1, 99.9, 100.0, 100.05];

        for mid in mids.iter() {
            let bid = mid - 0.5;
            let ask = mid + 0.5;
            let snapshot = make_snapshot(
                &[(bid, 10.0)],
                &[(ask, 10.0)],
            );
            calc.update(&snapshot);
        }

        // Volatility should be non-zero due to price changes
        let vol = calc.volatility();
        assert!(vol > 0.0, "Volatility should be positive with price changes");
    }

    #[test]
    fn test_calculate_imbalance_depth_filter() {
        let calc = BinanceObiCalculator::new(10, 0.01); // 1% depth

        // Mid = 100.5, so depth range is [99.495, 101.505]
        let snapshot = make_snapshot(
            &[(100.0, 10.0), (99.5, 10.0), (99.0, 100.0)], // 99.0 is outside depth
            &[(101.0, 10.0), (101.5, 10.0), (102.0, 100.0)], // 102.0 is outside depth
        );

        let mid = snapshot.mid_price().unwrap();
        let imbalance = calc.calculate_imbalance(&snapshot, mid);

        // Within depth: bids = 10 + 10 = 20, asks = 10 + 10 = 20
        // Imbalance should be close to 0 (balanced)
        assert!(
            imbalance.abs() < 0.1,
            "Should be balanced within depth, got {}",
            imbalance
        );
    }

    #[test]
    fn test_clear_resets_state() {
        let mut calc = BinanceObiCalculator::new(10, 0.01);

        // Warm up
        for _ in 0..10 {
            let snapshot = make_snapshot(&[(100.0, 10.0)], &[(101.0, 10.0)]);
            calc.update(&snapshot);
        }

        assert!(calc.is_warmed_up());
        assert_eq!(calc.sample_count(), 10);

        calc.clear();

        assert!(!calc.is_warmed_up());
        assert_eq!(calc.sample_count(), 0);
    }
}
