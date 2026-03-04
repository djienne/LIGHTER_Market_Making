//! Lock-free position storage for low-latency access.
//!
//! Uses atomic operations to allow the main trading loop to read position
//! without blocking, while a background task updates it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use super::auth::AuthManager;
use super::TradingStats;

/// Shared position with atomic storage for lock-free reads.
///
/// Position is stored as f64 bits in an AtomicU64, allowing
/// lock-free reads from the hot path while background updates
/// can write without blocking.
#[derive(Debug)]
pub struct SharedPosition {
    /// Position in base asset units (e.g., BTC), stored as f64 bits
    position_bits: AtomicU64,
    /// Last update timestamp (Unix millis)
    last_update_ms: AtomicU64,
    /// Symbol being tracked
    symbol: String,
}

impl SharedPosition {
    /// Create a new shared position for a symbol.
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            position_bits: AtomicU64::new(0.0f64.to_bits()),
            last_update_ms: AtomicU64::new(0),
            symbol: symbol.into(),
        }
    }

    /// Get the current position (lock-free read).
    #[inline]
    pub fn get(&self) -> f64 {
        f64::from_bits(self.position_bits.load(Ordering::Acquire))
    }

    /// Set the position (atomic store).
    #[inline]
    pub fn set(&self, position: f64) {
        self.position_bits.store(position.to_bits(), Ordering::Release);
        self.last_update_ms.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            Ordering::Release,
        );
    }

    /// Get the symbol being tracked.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Get milliseconds since last update.
    pub fn age_ms(&self) -> u64 {
        let last = self.last_update_ms.load(Ordering::Acquire);
        if last == 0 {
            return u64::MAX; // Never updated
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        now.saturating_sub(last)
    }

    /// Check if position data is stale (older than threshold).
    pub fn is_stale(&self, threshold_ms: u64) -> bool {
        self.age_ms() > threshold_ms
    }
}

/// Position poller configuration.
#[derive(Debug, Clone)]
pub struct PositionPollerConfig {
    /// Polling interval
    pub interval: Duration,
    /// Symbol to poll
    pub symbol: String,
    /// Stale threshold (warn if position is older)
    pub stale_threshold: Duration,
}

impl Default for PositionPollerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(2),
            symbol: "TEST-USD".to_string(),
            stale_threshold: Duration::from_secs(10),
        }
    }
}

/// Background position poller.
///
/// Runs as a tokio task, polling positions at a configurable interval
/// and updating the SharedPosition atomically.
pub struct PositionPoller {
    /// Shared position storage
    position: Arc<SharedPosition>,
    /// Auth manager for API calls
    auth: Arc<Mutex<AuthManager>>,
    /// Configuration
    config: PositionPollerConfig,
    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,
    /// Trading stats for volume tracking (inferred from position changes)
    trading_stats: Arc<TradingStats>,
}

impl PositionPoller {
    /// Create a new position poller.
    pub fn new(
        position: Arc<SharedPosition>,
        auth: Arc<Mutex<AuthManager>>,
        config: PositionPollerConfig,
        trading_stats: Arc<TradingStats>,
    ) -> Self {
        Self {
            position,
            auth,
            config,
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            trading_stats,
        }
    }

    /// Start the position poller as a background task.
    ///
    /// Returns a handle that can be used to stop the poller.
    pub fn start(self) -> PositionPollerHandle {
        let running = Arc::clone(&self.running);
        running.store(true, Ordering::Release);

        let handle = tokio::spawn(async move {
            self.run().await;
        });

        PositionPollerHandle {
            running,
            task: handle,
        }
    }

    /// Run the polling loop.
    async fn run(self) {
        info!(
            "Position poller started for {} (interval: {:?})",
            self.config.symbol, self.config.interval
        );

        let mut last_position = 0.0f64;
        let mut consecutive_errors = 0u32;
        let mut last_log = Instant::now();

        while self.running.load(Ordering::Acquire) {
            // Poll position
            match self.poll_position().await {
                Ok(position) => {
                    consecutive_errors = 0;

                    // Only log if position changed significantly or periodically
                    let position_changed = (position - last_position).abs() > 1e-8;
                    let should_log = position_changed || last_log.elapsed() > Duration::from_secs(60);

                    // Infer traded volume and trade count from position changes (zero hot path impact)
                    if position_changed {
                        let delta = (position - last_position).abs();
                        let mid_price = self.trading_stats.mid_price();
                        self.trading_stats.add_volume(delta, mid_price);
                        self.trading_stats.increment_trade_count();
                        debug!(
                            "[{}] Fill inferred: delta={:.6} (${:.2}), total_vol={:.6} (${:.2}), trades={}",
                            self.config.symbol, delta, delta * mid_price,
                            self.trading_stats.total_volume(), self.trading_stats.total_volume_usd(),
                            self.trading_stats.trade_count()
                        );
                    }

                    if should_log {
                        debug!(
                            "[{}] Position updated: {:.6} (changed: {})",
                            self.config.symbol, position, position_changed
                        );
                        last_log = Instant::now();
                    }

                    last_position = position;
                    self.position.set(position);
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors <= 3 {
                        warn!(
                            "[{}] Position poll failed (attempt {}): {}",
                            self.config.symbol, consecutive_errors, e
                        );
                    } else if consecutive_errors % 10 == 0 {
                        error!(
                            "[{}] Position poll failing repeatedly ({} errors): {}",
                            self.config.symbol, consecutive_errors, e
                        );
                    }
                }
            }

            // Sleep until next poll
            tokio::time::sleep(self.config.interval).await;
        }

        info!("Position poller stopped for {}", self.config.symbol);
    }

    /// Poll position from API.
    async fn poll_position(&self) -> Result<f64, String> {
        let mut auth = self.auth.lock().await;

        // Query positions for our symbol
        let positions = auth
            .query_positions(Some(&self.config.symbol))
            .await
            .map_err(|e| e.to_string())?;

        // Find position for our symbol
        // If symbol not found, assume zero position (new account or no trades yet)
        // If qty parsing fails, return error to preserve previous position value
        let position = match positions.iter().find(|p| p.symbol == self.config.symbol) {
            Some(p) => {
                p.qty.parse::<f64>().map_err(|e| {
                    format!("Failed to parse position qty '{}': {}", p.qty, e)
                })?
            }
            None => 0.0, // No position entry = zero position
        };

        Ok(position)
    }
}

/// Handle to control a running position poller.
pub struct PositionPollerHandle {
    running: Arc<std::sync::atomic::AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl PositionPollerHandle {
    /// Stop the position poller.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Wait for the poller to stop.
    pub async fn join(self) {
        self.stop();
        let _ = self.task.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_position() {
        let pos = SharedPosition::new("TEST-USD");
        assert_eq!(pos.get(), 0.0);
        assert_eq!(pos.symbol(), "TEST-USD");

        pos.set(1.5);
        assert_eq!(pos.get(), 1.5);

        pos.set(-0.25);
        assert_eq!(pos.get(), -0.25);
    }

    #[test]
    fn test_atomic_operations() {
        let pos = Arc::new(SharedPosition::new("TEST-USD"));

        // Simulate concurrent reads
        let pos_clone = Arc::clone(&pos);
        pos.set(123.456);

        assert_eq!(pos_clone.get(), 123.456);
    }
}
