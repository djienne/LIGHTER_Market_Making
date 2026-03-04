//! Orderbook sanity checker via REST API.
//!
//! Periodically compares the WebSocket-maintained orderbook against
//! a REST API "ground truth" snapshot and applies corrections if drift is detected.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::SanityCheckConfig;
use crate::trading::client::{DepthBookResponse, StandXClient};
use crate::types::{OrderbookSnapshot, Symbol};
use crate::websocket::current_time_ns;
use super::OrderbookStore;

/// Statistics for sanity checker.
#[derive(Debug, Default)]
pub struct SanityCheckerStats {
    /// Total checks performed
    pub checks: AtomicU64,
    /// Total corrections applied
    pub corrections: AtomicU64,
    /// Total drift detections (including those below threshold)
    pub drift_detections: AtomicU64,
}

impl SanityCheckerStats {
    /// Get a snapshot of current stats.
    pub fn snapshot(&self) -> SanityCheckerStatsSnapshot {
        SanityCheckerStatsSnapshot {
            checks: self.checks.load(Ordering::Relaxed),
            corrections: self.corrections.load(Ordering::Relaxed),
            drift_detections: self.drift_detections.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of sanity checker stats.
#[derive(Debug, Clone)]
pub struct SanityCheckerStatsSnapshot {
    pub checks: u64,
    pub corrections: u64,
    pub drift_detections: u64,
}

/// Configuration for sanity checker.
#[derive(Debug, Clone)]
pub struct SanityCheckerConfig {
    /// Check interval
    pub interval: Duration,
    /// Drift threshold in basis points
    pub drift_threshold_bps: f64,
    /// Max orderbook levels to use for comparison/correction
    pub max_levels: usize,
}

impl From<&SanityCheckConfig> for SanityCheckerConfig {
    fn from(config: &SanityCheckConfig) -> Self {
        Self {
            interval: Duration::from_secs(config.interval_secs),
            drift_threshold_bps: config.drift_threshold_bps,
            max_levels: 20, // Default, can be made configurable
        }
    }
}

/// Orderbook sanity checker that compares WS orderbook against REST API.
pub struct OrderbookSanityChecker {
    client: StandXClient,
    config: SanityCheckerConfig,
    store: Arc<OrderbookStore>,
    symbols: Vec<String>,
    running: Arc<AtomicBool>,
    stats: Arc<SanityCheckerStats>,
}

impl OrderbookSanityChecker {
    /// Create a new sanity checker.
    pub fn new(
        store: Arc<OrderbookStore>,
        symbols: Vec<String>,
        config: SanityCheckerConfig,
    ) -> Self {
        Self {
            client: StandXClient::new(),
            config,
            store,
            symbols,
            running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(SanityCheckerStats::default()),
        }
    }

    /// Get stats handle.
    pub fn stats(&self) -> Arc<SanityCheckerStats> {
        Arc::clone(&self.stats)
    }

    /// Start the sanity checker, returns handle for control.
    pub fn start(self) -> SanityCheckerHandle {
        let running = Arc::clone(&self.running);
        let stats = Arc::clone(&self.stats);
        self.running.store(true, Ordering::Release);

        let handle = tokio::spawn(async move {
            self.run().await;
        });

        SanityCheckerHandle { running, handle, stats }
    }

    /// Main run loop.
    async fn run(self) {
        info!(
            "Orderbook sanity checker started (interval: {}s, threshold: {} bps)",
            self.config.interval.as_secs(),
            self.config.drift_threshold_bps
        );

        while self.running.load(Ordering::Acquire) {
            // Check all symbols
            for symbol in &self.symbols {
                if let Some(ob) = self.store.get(symbol) {
                    self.check_and_correct(symbol, ob).await;
                }
            }

            // Sleep until next check
            tokio::time::sleep(self.config.interval).await;
        }

        info!("Orderbook sanity checker stopped");
    }

    /// Check a single symbol and apply correction if needed.
    async fn check_and_correct(&self, symbol: &str, ob: &super::SymbolOrderbook) {
        self.stats.checks.fetch_add(1, Ordering::Relaxed);

        // 1. Fetch REST snapshot
        let rest_book = match self.client.query_depth_book(symbol).await {
            Ok(b) => b,
            Err(e) => {
                warn!("[{}] Sanity check fetch failed: {}", symbol, e);
                return;
            }
        };

        // 2. Get current WS book (lock-free peek - doesn't swap indices)
        let ws_book = match ob.peek() {
            Some(b) => b,
            None => {
                debug!("[{}] No WS orderbook data yet", symbol);
                return;
            }
        };

        // 3. Detect drift
        let (drift_bid_bps, drift_ask_bps) = self.calculate_drift(&ws_book, &rest_book);
        let max_drift = drift_bid_bps.abs().max(drift_ask_bps.abs());

        if max_drift > 0.01 {
            // Log any non-zero drift for monitoring
            self.stats.drift_detections.fetch_add(1, Ordering::Relaxed);
            debug!(
                "[{}] Drift detected: bid={:.2}bps ask={:.2}bps",
                symbol, drift_bid_bps, drift_ask_bps
            );
        }

        // 4. Apply correction if above threshold
        if max_drift > self.config.drift_threshold_bps {
            warn!(
                "[{}] Orderbook drift {:.2}bps exceeds threshold {:.1}bps, applying correction",
                symbol, max_drift, self.config.drift_threshold_bps
            );

            // Convert REST to OrderbookSnapshot and apply
            let corrected = self.rest_to_snapshot(&rest_book);
            ob.update(corrected);

            self.stats.corrections.fetch_add(1, Ordering::Relaxed);
            info!("[{}] Orderbook corrected from REST API", symbol);
        }
    }

    #[inline]
    fn best_bid_from_levels(&self, levels: &[[String; 2]]) -> f64 {
        let mut best = 0.0;
        for [price_str, _] in levels {
            if let Ok(price) = fast_float::parse::<f64, _>(price_str) {
                if price > best {
                    best = price;
                }
            }
        }
        best
    }

    #[inline]
    fn best_ask_from_levels(&self, levels: &[[String; 2]]) -> f64 {
        let mut best: Option<f64> = None;
        for [price_str, _] in levels {
            if let Ok(price) = fast_float::parse::<f64, _>(price_str) {
                if price > 0.0 {
                    best = Some(best.map_or(price, |current| current.min(price)));
                }
            }
        }
        best.unwrap_or(0.0)
    }

    /// Calculate drift in basis points for bid and ask.
    fn calculate_drift(&self, ws_book: &OrderbookSnapshot, rest_book: &DepthBookResponse) -> (f64, f64) {
        // Parse REST best bid/ask without assuming ordering.
        let rest_bid = self.best_bid_from_levels(&rest_book.bids);
        let rest_ask = self.best_ask_from_levels(&rest_book.asks);

        // Get WS best bid/ask
        let ws_bid = ws_book.best_bid_price().unwrap_or(0.0);
        let ws_ask = ws_book.best_ask_price().unwrap_or(0.0);

        // Log comparison at debug level (reduces log spam in production)
        debug!(
            "[{}] SANITY: REST bid={:.2} ask={:.2} | WS bid={:.2} ask={:.2} | diff: bid={:.2} ask={:.2}",
            rest_book.symbol, rest_bid, rest_ask, ws_bid, ws_ask,
            ws_bid - rest_bid, ws_ask - rest_ask
        );

        // Calculate drift in basis points
        let drift_bid_bps = if rest_bid > 0.0 {
            ((ws_bid - rest_bid) / rest_bid) * 10000.0
        } else {
            0.0
        };

        let drift_ask_bps = if rest_ask > 0.0 {
            ((ws_ask - rest_ask) / rest_ask) * 10000.0
        } else {
            0.0
        };

        (drift_bid_bps, drift_ask_bps)
    }

    /// Convert REST DepthBookResponse to OrderbookSnapshot.
    fn rest_to_snapshot(&self, rest_book: &DepthBookResponse) -> OrderbookSnapshot {
        let now_ns = current_time_ns();
        let mut snapshot = OrderbookSnapshot::new(Symbol::new(&rest_book.symbol));
        snapshot.timestamp_ns = now_ns;
        snapshot.received_at_ns = now_ns;

        // Convert bids: [[price, qty], ...] to [(String, String), ...]
        let bids: Vec<(String, String)> = rest_book.bids.iter()
            .map(|[p, q]| (p.clone(), q.clone()))
            .collect();
        snapshot.set_bids_from_strings(&bids, self.config.max_levels);

        // Convert asks
        let asks: Vec<(String, String)> = rest_book.asks.iter()
            .map(|[p, q]| (p.clone(), q.clone()))
            .collect();
        snapshot.set_asks_from_strings(&asks, self.config.max_levels);

        snapshot
    }
}

/// Handle to control the sanity checker.
pub struct SanityCheckerHandle {
    running: Arc<AtomicBool>,
    handle: JoinHandle<()>,
    stats: Arc<SanityCheckerStats>,
}

impl SanityCheckerHandle {
    /// Stop the sanity checker.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Get stats.
    pub fn stats(&self) -> &Arc<SanityCheckerStats> {
        &self.stats
    }

    /// Wait for the checker to finish.
    pub async fn join(self) {
        let _ = self.handle.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_conversion() {
        let config = SanityCheckConfig {
            enabled: true,
            interval_secs: 30,
            drift_threshold_bps: 1.0,
        };
        let checker_config: SanityCheckerConfig = (&config).into();
        assert_eq!(checker_config.interval.as_secs(), 30);
        assert_eq!(checker_config.drift_threshold_bps, 1.0);
    }
}
