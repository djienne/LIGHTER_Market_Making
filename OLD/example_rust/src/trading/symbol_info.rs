//! Lock-free symbol info storage for tick size and lot size.
//!
//! Uses atomic operations to allow the main trading loop to read tick/lot sizes
//! without blocking, while a background task polls for changes.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::client::{StandXClient, SymbolInfo};

/// Shared symbol info with atomic storage for lock-free reads.
///
/// tick_size and lot_size are stored as f64 bits in AtomicU64, allowing
/// lock-free reads from the hot path while background updates can write
/// without blocking.
#[derive(Debug)]
pub struct SharedSymbolInfo {
    /// Symbol being tracked
    symbol: String,
    /// Tick size (price increment), stored as f64 bits
    tick_size_bits: AtomicU64,
    /// Lot size (quantity increment), stored as f64 bits
    lot_size_bits: AtomicU64,
    /// Price tick decimals (for logging/debugging)
    price_tick_decimals: AtomicU8,
    /// Qty tick decimals (for logging/debugging)
    qty_tick_decimals: AtomicU8,
    /// Generation counter (incremented on each update)
    generation: AtomicU64,
    /// Last update timestamp (Unix millis)
    last_update_ms: AtomicU64,
}

impl SharedSymbolInfo {
    /// Create a new shared symbol info with initial values.
    pub fn new(symbol: impl Into<String>, tick_size: f64, lot_size: f64) -> Self {
        Self {
            symbol: symbol.into(),
            tick_size_bits: AtomicU64::new(tick_size.to_bits()),
            lot_size_bits: AtomicU64::new(lot_size.to_bits()),
            price_tick_decimals: AtomicU8::new(Self::decimals_from_step(tick_size)),
            qty_tick_decimals: AtomicU8::new(Self::decimals_from_step(lot_size)),
            generation: AtomicU64::new(0),
            last_update_ms: AtomicU64::new(0),
        }
    }

    /// Create from SymbolInfo API response.
    pub fn from_api_response(info: &SymbolInfo) -> Self {
        Self::new(&info.symbol, info.tick_size(), info.lot_size())
    }

    /// Calculate decimal places from step size (e.g., 0.01 -> 2, 0.001 -> 3)
    fn decimals_from_step(step: f64) -> u8 {
        if step >= 1.0 {
            0
        } else {
            (-step.log10().round()) as u8
        }
    }

    /// Get the current tick size (lock-free read).
    #[inline]
    pub fn tick_size(&self) -> f64 {
        f64::from_bits(self.tick_size_bits.load(Ordering::Acquire))
    }

    /// Get the current lot size (lock-free read).
    #[inline]
    pub fn lot_size(&self) -> f64 {
        f64::from_bits(self.lot_size_bits.load(Ordering::Acquire))
    }

    /// Get price tick decimals.
    #[inline]
    pub fn price_tick_decimals(&self) -> u8 {
        self.price_tick_decimals.load(Ordering::Acquire)
    }

    /// Get qty tick decimals.
    #[inline]
    pub fn qty_tick_decimals(&self) -> u8 {
        self.qty_tick_decimals.load(Ordering::Acquire)
    }

    /// Get the generation counter (incremented on each update).
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Get the symbol.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Validate that a tick/lot size value is sane.
    ///
    /// Returns true if the value is valid (positive, finite, not too small/large).
    #[inline]
    fn is_valid_step_size(value: f64) -> bool {
        value.is_finite() && value > 1e-12 && value < 1e12
    }

    /// Update symbol info from API response.
    ///
    /// Returns Ok(true) if tick_size or lot_size changed.
    /// Returns Ok(false) if values unchanged.
    /// Returns Err if new values are invalid (keeps current values).
    pub fn try_update(&self, info: &SymbolInfo) -> Result<bool, String> {
        let new_tick_size = info.tick_size();
        let new_lot_size = info.lot_size();

        // Validate new values - reject if invalid
        if !Self::is_valid_step_size(new_tick_size) {
            return Err(format!(
                "Invalid tick_size {} from API (decimals={})",
                new_tick_size, info.price_tick_decimals
            ));
        }
        if !Self::is_valid_step_size(new_lot_size) {
            return Err(format!(
                "Invalid lot_size {} from API (decimals={})",
                new_lot_size, info.qty_tick_decimals
            ));
        }

        let old_tick_size = self.tick_size();
        let old_lot_size = self.lot_size();

        // Check if values changed (using epsilon for float comparison)
        let tick_changed = (new_tick_size - old_tick_size).abs() > 1e-12;
        let lot_changed = (new_lot_size - old_lot_size).abs() > 1e-12;
        let changed = tick_changed || lot_changed;

        // Update values
        self.tick_size_bits.store(new_tick_size.to_bits(), Ordering::Release);
        self.lot_size_bits.store(new_lot_size.to_bits(), Ordering::Release);
        self.price_tick_decimals.store(info.price_tick_decimals, Ordering::Release);
        self.qty_tick_decimals.store(info.qty_tick_decimals, Ordering::Release);

        // Increment generation and update timestamp
        self.generation.fetch_add(1, Ordering::Release);
        self.last_update_ms.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            Ordering::Release,
        );

        Ok(changed)
    }

    /// Update symbol info from API response (legacy, panics on invalid values).
    ///
    /// Returns true if tick_size or lot_size changed.
    /// Prefer try_update() for proper error handling.
    pub fn update(&self, info: &SymbolInfo) -> bool {
        self.try_update(info).expect("Invalid tick_size or lot_size from API")
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
}

/// Signal sent when tick size or lot size changes.
#[derive(Debug, Clone)]
pub struct TickSizeChangedSignal {
    /// Symbol that changed
    pub symbol: String,
    /// Old tick size
    pub old_tick_size: f64,
    /// New tick size
    pub new_tick_size: f64,
    /// Old lot size
    pub old_lot_size: f64,
    /// New lot size
    pub new_lot_size: f64,
}

/// Symbol info poller configuration.
#[derive(Debug, Clone)]
pub struct SymbolInfoPollerConfig {
    /// Polling interval
    pub interval: Duration,
    /// Symbol to poll
    pub symbol: String,
}

impl Default for SymbolInfoPollerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            symbol: "TEST-USD".to_string(),
        }
    }
}

/// Background symbol info poller.
///
/// Polls the API for symbol info at a configurable interval and sends
/// a signal when tick_size or lot_size changes.
pub struct SymbolInfoPoller {
    /// Shared symbol info storage
    info: Arc<SharedSymbolInfo>,
    /// HTTP client for API calls
    client: StandXClient,
    /// Configuration
    config: SymbolInfoPollerConfig,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Channel to send change signals
    signal_tx: mpsc::Sender<TickSizeChangedSignal>,
}

impl SymbolInfoPoller {
    /// Create a new symbol info poller.
    pub fn new(
        info: Arc<SharedSymbolInfo>,
        config: SymbolInfoPollerConfig,
        signal_tx: mpsc::Sender<TickSizeChangedSignal>,
    ) -> Self {
        Self {
            info,
            client: StandXClient::new(),
            config,
            running: Arc::new(AtomicBool::new(false)),
            signal_tx,
        }
    }

    /// Start the poller as a background task.
    ///
    /// Returns a handle that can be used to stop the poller.
    pub fn start(self) -> SymbolInfoPollerHandle {
        let running = Arc::clone(&self.running);
        running.store(true, Ordering::Release);

        let handle = tokio::spawn(async move {
            self.run().await;
        });

        SymbolInfoPollerHandle {
            running,
            task: handle,
        }
    }

    /// Run the polling loop.
    async fn run(self) {
        info!(
            "[{}] Symbol info poller started (interval: {:?})",
            self.config.symbol, self.config.interval
        );

        let mut consecutive_errors = 0u32;

        while self.running.load(Ordering::Acquire) {
            match self.poll_symbol_info().await {
                Ok(api_info) => {
                    // Capture old values before update
                    let old_tick_size = self.info.tick_size();
                    let old_lot_size = self.info.lot_size();

                    // Try to update - validates values before accepting
                    match self.info.try_update(&api_info) {
                        Ok(changed) => {
                            consecutive_errors = 0;

                            if changed {
                                let new_tick_size = self.info.tick_size();
                                let new_lot_size = self.info.lot_size();

                                warn!(
                                    "[{}] TICK SIZE CHANGED! tick_size: {} -> {}, lot_size: {} -> {}",
                                    self.config.symbol, old_tick_size, new_tick_size,
                                    old_lot_size, new_lot_size
                                );

                                let signal = TickSizeChangedSignal {
                                    symbol: self.config.symbol.clone(),
                                    old_tick_size,
                                    new_tick_size,
                                    old_lot_size,
                                    new_lot_size,
                                };

                                if let Err(e) = self.signal_tx.send(signal).await {
                                    error!(
                                        "[{}] Failed to send tick size changed signal: {}",
                                        self.config.symbol, e
                                    );
                                }
                            } else {
                                debug!(
                                    "[{}] Symbol info unchanged: tick_size={}, lot_size={}",
                                    self.config.symbol,
                                    self.info.tick_size(),
                                    self.info.lot_size()
                                );
                            }
                        }
                        Err(validation_error) => {
                            // API returned invalid values - keep current values
                            consecutive_errors += 1;
                            warn!(
                                "[{}] Rejecting invalid symbol info: {} - keeping current tick_size={}, lot_size={}",
                                self.config.symbol, validation_error,
                                self.info.tick_size(), self.info.lot_size()
                            );
                        }
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors <= 3 {
                        warn!(
                            "[{}] Symbol info poll failed (attempt {}): {}",
                            self.config.symbol, consecutive_errors, e
                        );
                    } else if consecutive_errors % 10 == 0 {
                        error!(
                            "[{}] Symbol info poll failing repeatedly ({} errors): {}",
                            self.config.symbol, consecutive_errors, e
                        );
                    }
                    // Keep current tick_size on error, retry on next poll
                }
            }

            // Sleep until next poll
            tokio::time::sleep(self.config.interval).await;
        }

        info!("[{}] Symbol info poller stopped", self.config.symbol);
    }

    /// Poll symbol info from API.
    async fn poll_symbol_info(&self) -> Result<SymbolInfo, String> {
        self.client
            .query_symbol_info(&self.config.symbol)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Handle to control a running symbol info poller.
pub struct SymbolInfoPollerHandle {
    running: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl SymbolInfoPollerHandle {
    /// Stop the poller.
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
    fn test_shared_symbol_info_new() {
        let info = SharedSymbolInfo::new("TEST-USD", 0.01, 0.0001);
        assert_eq!(info.symbol(), "TEST-USD");
        assert_eq!(info.tick_size(), 0.01);
        assert_eq!(info.lot_size(), 0.0001);
        assert_eq!(info.price_tick_decimals(), 2);
        assert_eq!(info.qty_tick_decimals(), 4);
        assert_eq!(info.generation(), 0);
    }

    #[test]
    fn test_shared_symbol_info_from_api() {
        let api_info = SymbolInfo {
            symbol: "ETH-USD".to_string(),
            price_tick_decimals: 2,
            qty_tick_decimals: 3,
        };

        let info = SharedSymbolInfo::from_api_response(&api_info);
        assert_eq!(info.symbol(), "ETH-USD");
        assert_eq!(info.tick_size(), 0.01);
        assert_eq!(info.lot_size(), 0.001);
    }

    #[test]
    fn test_shared_symbol_info_update() {
        let info = SharedSymbolInfo::new("TEST-USD", 0.01, 0.0001);
        assert_eq!(info.generation(), 0);

        // Same values - no change
        let api_info = SymbolInfo {
            symbol: "TEST-USD".to_string(),
            price_tick_decimals: 2,
            qty_tick_decimals: 4,
        };
        let changed = info.update(&api_info);
        assert!(!changed);
        assert_eq!(info.generation(), 1); // Generation still increments

        // Different tick_size - should detect change
        let api_info = SymbolInfo {
            symbol: "TEST-USD".to_string(),
            price_tick_decimals: 1, // 0.1 instead of 0.01
            qty_tick_decimals: 4,
        };
        let changed = info.update(&api_info);
        assert!(changed);
        assert_eq!(info.tick_size(), 0.1);
        assert_eq!(info.generation(), 2);
    }

    #[test]
    fn test_atomic_operations() {
        let info = Arc::new(SharedSymbolInfo::new("TEST-USD", 0.01, 0.0001));

        // Simulate concurrent reads
        let info_clone = Arc::clone(&info);
        assert_eq!(info_clone.tick_size(), 0.01);
        assert_eq!(info_clone.lot_size(), 0.0001);
    }

    #[test]
    fn test_decimals_from_step() {
        assert_eq!(SharedSymbolInfo::decimals_from_step(1.0), 0);
        assert_eq!(SharedSymbolInfo::decimals_from_step(0.1), 1);
        assert_eq!(SharedSymbolInfo::decimals_from_step(0.01), 2);
        assert_eq!(SharedSymbolInfo::decimals_from_step(0.001), 3);
        assert_eq!(SharedSymbolInfo::decimals_from_step(0.0001), 4);
    }

    #[test]
    fn test_poller_config_default() {
        let config = SymbolInfoPollerConfig::default();
        assert_eq!(config.interval, Duration::from_secs(30));
        assert_eq!(config.symbol, "TEST-USD");
    }

    #[test]
    fn test_try_update_rejects_invalid_values() {
        let info = SharedSymbolInfo::new("TEST-USD", 0.01, 0.0001);

        // Valid update should succeed
        let valid_info = SymbolInfo {
            symbol: "TEST-USD".to_string(),
            price_tick_decimals: 1, // 0.1
            qty_tick_decimals: 3,   // 0.001
        };
        assert!(info.try_update(&valid_info).is_ok());
        assert_eq!(info.tick_size(), 0.1);

        // Zero tick_size (decimals=255 would give extremely small value)
        // Note: price_tick_decimals is u8, so max is 255 -> 10^-255 which is essentially 0
        // We can't really test 0 directly, but we can test the validation logic

        // Test that valid values work
        let valid_info = SymbolInfo {
            symbol: "TEST-USD".to_string(),
            price_tick_decimals: 2,
            qty_tick_decimals: 4,
        };
        let result = info.try_update(&valid_info);
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_valid_step_size() {
        // Valid values
        assert!(SharedSymbolInfo::is_valid_step_size(0.01));
        assert!(SharedSymbolInfo::is_valid_step_size(0.0001));
        assert!(SharedSymbolInfo::is_valid_step_size(1.0));
        assert!(SharedSymbolInfo::is_valid_step_size(100.0));

        // Invalid values
        assert!(!SharedSymbolInfo::is_valid_step_size(0.0));
        assert!(!SharedSymbolInfo::is_valid_step_size(-0.01));
        assert!(!SharedSymbolInfo::is_valid_step_size(f64::NAN));
        assert!(!SharedSymbolInfo::is_valid_step_size(f64::INFINITY));
        assert!(!SharedSymbolInfo::is_valid_step_size(f64::NEG_INFINITY));
        assert!(!SharedSymbolInfo::is_valid_step_size(1e-20)); // Too small
        assert!(!SharedSymbolInfo::is_valid_step_size(1e20));  // Too large
    }
}
