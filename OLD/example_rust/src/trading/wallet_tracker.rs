//! Wallet balance tracker with PnL calculation and CSV logging.
//!
//! This module provides a background task that polls account balance and
//! writes wallet history to a CSV file with PnL percentage calculation.
//! The reference equity and total volume persist across restarts by reading from existing CSV.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::auth::AuthManager;
use super::equity::SharedEquity;
use super::position::SharedPosition;
use super::TradingStats;

/// Configuration for wallet tracking.
#[derive(Debug, Clone)]
pub struct WalletTrackerConfig {
    /// Polling interval
    pub interval: Duration,
    /// Path to the CSV file
    pub csv_path: String,
}

/// A single wallet record for CSV.
#[derive(Debug)]
struct WalletRecord {
    /// ISO 8601 timestamp
    timestamp: String,
    /// Total equity (balance + unrealized PnL)
    equity: f64,
    /// Cross margin balance
    cross_balance: f64,
    /// Unrealized PnL
    unrealized_pnl: f64,
    /// PnL % from reference
    pnl_percent: f64,
    /// Position value in USD (positive if long, negative if short)
    position_usd: f64,
    /// Cumulative traded volume in base asset (e.g., BTC)
    total_volume: f64,
    /// Cumulative traded volume in USD
    total_volume_usd: f64,
    /// Cumulative trade count
    trade_count: u64,
}

/// Wallet tracker that polls balance and writes to CSV.
pub struct WalletTracker {
    auth: Arc<Mutex<AuthManager>>,
    config: WalletTrackerConfig,
    running: Arc<AtomicBool>,
    reference_equity: Option<f64>,
    /// Position tracker (for position_usd calculation)
    position: Arc<SharedPosition>,
    /// Trading stats (for volume tracking and mid price)
    trading_stats: Arc<TradingStats>,
    /// Shared equity for hot path order sizing
    shared_equity: Arc<SharedEquity>,
}

impl WalletTracker {
    /// Create a new wallet tracker.
    pub fn new(
        auth: Arc<Mutex<AuthManager>>,
        config: WalletTrackerConfig,
        position: Arc<SharedPosition>,
        trading_stats: Arc<TradingStats>,
        shared_equity: Arc<SharedEquity>,
    ) -> Self {
        Self {
            auth,
            config,
            running: Arc::new(AtomicBool::new(false)),
            reference_equity: None,
            position,
            trading_stats,
            shared_equity,
        }
    }

    /// Start the tracker, returns handle for control.
    pub fn start(self) -> WalletTrackerHandle {
        let running = Arc::clone(&self.running);
        self.running.store(true, Ordering::Release);

        let handle = tokio::spawn(async move {
            self.run().await;
        });

        WalletTrackerHandle { running, handle }
    }

    /// Main run loop.
    async fn run(mut self) {
        info!(
            "Wallet tracker started (interval: {}s, csv: {})",
            self.config.interval.as_secs(),
            self.config.csv_path
        );

        // Load reference equity from existing CSV (persists across restarts)
        self.load_reference_from_csv().await;

        // Write CSV header if file doesn't exist
        self.ensure_csv_header().await;

        while self.running.load(Ordering::Acquire) {
            // Fetch balance and record
            match self.fetch_and_record().await {
                Ok(_) => {}
                Err(e) => {
                    warn!("Wallet tracker error: {}", e);
                }
            }

            // Sleep until next poll
            tokio::time::sleep(self.config.interval).await;
        }

        info!("Wallet tracker stopped");
    }

    /// Fetch balance and write to CSV.
    async fn fetch_and_record(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Fetch balance
        let balance = {
            let mut auth = self.auth.lock().await;
            auth.query_balance().await?
        };

        // Parse values
        let equity = balance
            .equity
            .as_ref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let cross_balance = balance
            .cross_balance
            .as_ref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let upnl = balance
            .upnl
            .as_ref()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        // Set reference on first fetch (fallback if CSV didn't have it)
        if self.reference_equity.is_none() && equity > 0.0 {
            self.reference_equity = Some(equity);
            info!("Wallet tracker: reference equity set to ${:.2}", equity);
        }

        // Update shared equity for hot path order sizing
        if equity > 0.0 {
            let was_initialized = self.shared_equity.is_initialized();
            self.shared_equity.set_equity(equity);
            let order_qty = self.shared_equity.order_qty_dollar();
            if !was_initialized {
                let leverage = self.shared_equity.leverage();
                if leverage > 1.0 {
                    info!(
                        "Order sizing: equity=${:.2} x{:.0} leverage -> ${:.2}/order",
                        equity, leverage, order_qty
                    );
                } else {
                    info!(
                        "Order sizing: equity=${:.2} -> ${:.2}/order",
                        equity, order_qty
                    );
                }
            }
        }

        // Calculate PnL %
        let pnl_percent = match self.reference_equity {
            Some(ref_eq) if ref_eq > 0.0 => ((equity - ref_eq) / ref_eq) * 100.0,
            _ => 0.0,
        };

        // Get position and mid price for position_usd calculation
        let position = self.position.get();
        let mid_price = self.trading_stats.mid_price();
        let position_usd = position * mid_price;

        // Get cumulative traded volume and trade count
        let total_volume = self.trading_stats.total_volume();
        let total_volume_usd = self.trading_stats.total_volume_usd();
        let trade_count = self.trading_stats.trade_count();

        // Create record
        let record = WalletRecord {
            timestamp: chrono::Utc::now().to_rfc3339(),
            equity,
            cross_balance,
            unrealized_pnl: upnl,
            pnl_percent,
            position_usd,
            total_volume,
            total_volume_usd,
            trade_count,
        };

        // Write to CSV
        self.append_csv(&record).await?;

        debug!(
            "Wallet: equity=${:.2} pnl={:+.2}% pos_usd=${:.2} vol={:.4} (${:.2}) trades={}",
            equity, pnl_percent, position_usd, total_volume, total_volume_usd, trade_count
        );

        Ok(())
    }

    /// Ensure CSV file has header.
    async fn ensure_csv_header(&self) {
        let path = Path::new(&self.config.csv_path);
        if !path.exists() {
            if let Ok(mut file) = tokio::fs::File::create(path).await {
                let header = "timestamp,equity,cross_balance,unrealized_pnl,pnl_percent,position_usd,total_volume,total_volume_usd,trade_count\n";
                let _ = file.write_all(header.as_bytes()).await;
            }
        }
    }

    /// Append a record to the CSV file.
    async fn append_csv(&self, record: &WalletRecord) -> std::io::Result<()> {
        let line = format!(
            "{},{:.8},{:.8},{:.8},{:.4},{:.2},{:.8},{:.2},{}\n",
            record.timestamp,
            record.equity,
            record.cross_balance,
            record.unrealized_pnl,
            record.pnl_percent,
            record.position_usd,
            record.total_volume,
            record.total_volume_usd,
            record.trade_count,
        );

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.csv_path)
            .await?;

        file.write_all(line.as_bytes()).await?;
        Ok(())
    }

    /// Load reference equity from first CSV row and total_volume from last row (persist across restarts).
    async fn load_reference_from_csv(&mut self) {
        let path = Path::new(&self.config.csv_path);
        if !path.exists() {
            debug!("No existing CSV file, reference will be set on first fetch");
            return;
        }

        // Read file and parse first data row (skip header)
        match tokio::fs::read_to_string(path).await {
            Ok(contents) => {
                let lines: Vec<&str> = contents.lines().collect();
                if lines.len() < 2 {
                    debug!("CSV file has no data rows");
                    return;
                }

                // Load reference equity from first data row (index 1, after header)
                let first_row = lines[1];
                let first_fields: Vec<&str> = first_row.split(',').collect();
                // CSV format: timestamp,equity,cross_balance,unrealized_pnl,pnl_percent,position_usd,total_volume,total_volume_usd
                if first_fields.len() >= 2 {
                    if let Ok(equity) = first_fields[1].parse::<f64>() {
                        if equity > 0.0 {
                            self.reference_equity = Some(equity);
                            info!("Loaded reference equity from CSV: ${:.2}", equity);
                        }
                    }
                }

                // Load volumes and trade count from last data row (for persistence)
                let last_row = lines.last().unwrap();
                let last_fields: Vec<&str> = last_row.split(',').collect();
                // CSV format: ...,total_volume (idx 6), total_volume_usd (idx 7), trade_count (idx 8)
                let mut volume = 0.0;
                let mut volume_usd = 0.0;
                let mut trade_count: u64 = 0;

                if last_fields.len() >= 7 {
                    if let Ok(v) = last_fields[6].parse::<f64>() {
                        volume = v;
                    }
                }
                if last_fields.len() >= 8 {
                    if let Ok(v) = last_fields[7].parse::<f64>() {
                        volume_usd = v;
                    }
                }
                if last_fields.len() >= 9 {
                    if let Ok(v) = last_fields[8].parse::<u64>() {
                        trade_count = v;
                    }
                }

                if volume > 0.0 || volume_usd > 0.0 || trade_count > 0 {
                    info!("Loaded volumes from CSV: {:.4} BTC (${:.2}), {} trades", volume, volume_usd, trade_count);
                    self.trading_stats.set_initial_volumes(volume, volume_usd);
                    self.trading_stats.set_initial_trade_count(trade_count);
                } else {
                    debug!("CSV doesn't have volume/trade columns (old format), starting at 0");
                }
            }
            Err(e) => {
                warn!("Failed to read CSV for reference: {}", e);
            }
        }
    }
}

/// Handle to control the wallet tracker.
pub struct WalletTrackerHandle {
    running: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl WalletTrackerHandle {
    /// Stop the wallet tracker.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Wait for the tracker to finish.
    pub async fn join(self) {
        let _ = self.handle.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wallet_tracker_config() {
        let config = WalletTrackerConfig {
            interval: Duration::from_secs(60),
            csv_path: "test.csv".to_string(),
        };
        assert_eq!(config.interval.as_secs(), 60);
        assert_eq!(config.csv_path, "test.csv");
    }
}
