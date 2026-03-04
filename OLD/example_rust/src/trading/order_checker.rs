//! Open orders checker for detecting stale internal state.
//!
//! Polls the exchange for open orders and signals when internal state
//! should be cleared (exchange has no orders but we think we do, or
//! orders are imbalanced - all on same side, or orders are too old).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

use super::auth::AuthManager;

/// Result of polling open orders.
#[derive(Debug, Clone)]
struct PollResult {
    total: usize,
    buys: usize,
    sells: usize,
    /// Oldest order age in seconds (based on local tracking)
    oldest_age_secs: Option<u64>,
    /// Order IDs seen in this poll
    order_ids: Vec<i64>,
}

/// Signal sent when internal order state should be cleared.
#[derive(Debug, Clone)]
pub struct ClearOrdersSignal {
    /// Symbol to clear orders for
    pub symbol: String,
    /// Reason for clearing
    pub reason: String,
    /// Whether to trigger safety pause (e.g., >2 orders detected)
    pub pause_trading: bool,
}

/// Open orders checker configuration.
#[derive(Debug, Clone)]
pub struct OpenOrdersCheckerConfig {
    /// Polling interval
    pub interval: Duration,
    /// Symbol to check
    pub symbol: String,
    /// Number of consecutive zero-order polls before signaling (debounce)
    pub debounce_count: u32,
    /// Maximum order age in seconds before considering stale (0 = disabled)
    /// Based on local tracking of when orders were first seen
    pub max_order_age_secs: u64,
    /// Expected number of order levels per side (1 or 2).
    /// With 1 level: max 2 orders (1 bid + 1 ask)
    /// With 2 levels: max 4 orders (2 bids + 2 asks)
    pub expected_order_levels: usize,
}

impl Default for OpenOrdersCheckerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3),
            symbol: "TEST-USD".to_string(),
            debounce_count: 2, // 2 consecutive polls = 6 seconds
            max_order_age_secs: 120, // 2 minutes
            expected_order_levels: 1, // Default: 1 level = 2 orders max
        }
    }
}

/// Background open orders checker.
///
/// Polls the exchange for open orders and sends a signal when the exchange
/// reports 0 orders. This allows the main loop to immediately clear internal
/// state and place new orders, instead of waiting for timeout.
pub struct OpenOrdersChecker {
    /// Auth manager for API calls
    auth: Arc<Mutex<AuthManager>>,
    /// Configuration
    config: OpenOrdersCheckerConfig,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Channel to send clear signals
    signal_tx: mpsc::Sender<ClearOrdersSignal>,
}

impl OpenOrdersChecker {
    /// Create a new open orders checker.
    pub fn new(
        auth: Arc<Mutex<AuthManager>>,
        config: OpenOrdersCheckerConfig,
        signal_tx: mpsc::Sender<ClearOrdersSignal>,
    ) -> Self {
        Self {
            auth,
            config,
            running: Arc::new(AtomicBool::new(false)),
            signal_tx,
        }
    }

    /// Start the checker as a background task.
    ///
    /// Returns a handle that can be used to stop the checker.
    pub fn start(self) -> OpenOrdersCheckerHandle {
        let running = Arc::clone(&self.running);
        running.store(true, Ordering::Release);

        let handle = tokio::spawn(async move {
            self.run().await;
        });

        OpenOrdersCheckerHandle {
            running,
            task: handle,
        }
    }

    /// Run the polling loop.
    async fn run(self) {
        // Calculate expected max orders based on levels (levels per side * 2 sides)
        let max_orders = self.config.expected_order_levels * 2;

        info!(
            "[{}] Open orders checker started (interval: {:?}, debounce: {}, max_age: {}s, levels: {}, max_orders: {})",
            self.config.symbol, self.config.interval, self.config.debounce_count,
            self.config.max_order_age_secs, self.config.expected_order_levels, max_orders
        );

        let mut consecutive_stale_count = 0u32;
        let mut consecutive_errors = 0u32;
        let mut last_was_stale = false;
        let mut last_stale_reason = String::new();

        // Track when we first saw each order ID (for age detection)
        let mut order_first_seen: HashMap<i64, Instant> = HashMap::new();

        while self.running.load(Ordering::Acquire) {
            match self.poll_open_orders(&mut order_first_seen).await {
                Ok(result) => {
                    consecutive_errors = 0;

                    // Log poll result for debugging
                    debug!(
                        "[{}] Poll result: total={}, buys={}, sells={}, oldest_age={:?}s",
                        self.config.symbol, result.total, result.buys, result.sells,
                        result.oldest_age_secs
                    );

                    // Detect stale state: >max orders (CRITICAL), 0 orders, imbalanced sides, or orders too old
                    // Returns (is_stale, stale_reason, should_pause_trading)
                    let (is_stale, stale_reason, should_pause) = if result.total > max_orders {
                        // CRITICAL: More than expected max orders = duplicate/stuck state
                        (true, format!("too many orders: {} (max {})", result.total, max_orders), true)
                    } else if result.total == 0 {
                        (true, "0 orders".to_string(), false)
                    } else if result.total >= self.config.expected_order_levels && (result.buys == 0 || result.sells == 0) {
                        // Have at least expected_order_levels orders but all on same side
                        (true, format!("imbalanced: {} buys, {} sells", result.buys, result.sells), false)
                    } else if self.config.max_order_age_secs > 0 {
                        // Check for orders that are too old
                        if let Some(oldest_age) = result.oldest_age_secs {
                            if oldest_age > self.config.max_order_age_secs {
                                (true, format!("order age {}s > {}s max", oldest_age, self.config.max_order_age_secs), false)
                            } else {
                                (false, String::new(), false)
                            }
                        } else {
                            (false, String::new(), false)
                        }
                    } else {
                        (false, String::new(), false)
                    };

                    if is_stale {
                        consecutive_stale_count += 1;

                        // Only signal after debounce threshold
                        if consecutive_stale_count >= self.config.debounce_count {
                            // Only signal once per stale period (or when reason changes)
                            if !last_was_stale
                                || consecutive_stale_count == self.config.debounce_count
                                || stale_reason != last_stale_reason
                            {
                                info!(
                                    "[{}] Stale state detected: {} (confirmed {} times) - signaling clear",
                                    self.config.symbol, stale_reason, consecutive_stale_count
                                );

                                let signal = ClearOrdersSignal {
                                    symbol: self.config.symbol.clone(),
                                    reason: format!("{} ({} polls)", stale_reason, consecutive_stale_count),
                                    pause_trading: should_pause,
                                };

                                if let Err(e) = self.signal_tx.send(signal).await {
                                    warn!("[{}] Failed to send clear signal: {}", self.config.symbol, e);
                                }
                            }
                        } else {
                            debug!(
                                "[{}] Stale state: {} (count: {}/{})",
                                self.config.symbol, stale_reason, consecutive_stale_count, self.config.debounce_count
                            );
                        }
                        last_stale_reason = stale_reason;
                    } else {
                        // Normal state: has both buy and sell orders
                        if consecutive_stale_count > 0 {
                            debug!(
                                "[{}] Exchange has {} orders ({} buys, {} sells) - resetting stale count from {}",
                                self.config.symbol, result.total, result.buys, result.sells, consecutive_stale_count
                            );
                        }
                        consecutive_stale_count = 0;
                        last_stale_reason.clear();
                    }

                    last_was_stale = is_stale;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors <= 3 {
                        warn!(
                            "[{}] Open orders check failed (attempt {}): {}",
                            self.config.symbol, consecutive_errors, e
                        );
                    } else if consecutive_errors % 10 == 0 {
                        error!(
                            "[{}] Open orders check failing repeatedly ({} errors): {}",
                            self.config.symbol, consecutive_errors, e
                        );
                    }
                }
            }

            // Sleep until next poll
            tokio::time::sleep(self.config.interval).await;
        }

        info!("[{}] Open orders checker stopped", self.config.symbol);
    }

    /// Poll open orders from API and track order ages.
    async fn poll_open_orders(
        &self,
        order_first_seen: &mut HashMap<i64, Instant>,
    ) -> Result<PollResult, String> {
        let mut auth = self.auth.lock().await;

        let open_orders = auth
            .query_open_orders(Some(&self.config.symbol))
            .await
            .map_err(|e| e.to_string())?;

        let buys = open_orders
            .iter()
            .filter(|o| o.side.eq_ignore_ascii_case("buy"))
            .count();
        let sells = open_orders
            .iter()
            .filter(|o| o.side.eq_ignore_ascii_case("sell"))
            .count();

        // Collect current order IDs
        let current_ids: Vec<i64> = open_orders.iter().map(|o| o.id).collect();

        // Track first-seen time for new orders
        let now = Instant::now();
        for &id in &current_ids {
            order_first_seen.entry(id).or_insert(now);
        }

        // Remove orders no longer present
        order_first_seen.retain(|id, _| current_ids.contains(id));

        // Calculate oldest order age
        let oldest_age_secs = if order_first_seen.is_empty() {
            None
        } else {
            order_first_seen
                .values()
                .map(|first_seen| now.duration_since(*first_seen).as_secs())
                .max()
        };

        Ok(PollResult {
            total: open_orders.len(),
            buys,
            sells,
            oldest_age_secs,
            order_ids: current_ids,
        })
    }
}

/// Handle to control a running open orders checker.
pub struct OpenOrdersCheckerHandle {
    running: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl OpenOrdersCheckerHandle {
    /// Stop the checker.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Wait for the checker to stop.
    pub async fn join(self) {
        self.stop();
        let _ = self.task.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = OpenOrdersCheckerConfig::default();
        assert_eq!(config.interval, Duration::from_secs(3));
        assert_eq!(config.debounce_count, 2);
    }
}
