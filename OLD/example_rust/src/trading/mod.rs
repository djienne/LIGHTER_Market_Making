//! Trading module for StandX.
//!
//! This module provides trading capabilities including:
//! - [`AuthManager`]: JWT authentication for API access
//! - [`StandXClient`]: HTTP client for REST API
//! - [`OrderWsClient`]: WebSocket client for low-latency order operations
//! - [`OrderManager`]: Order submission and management
//!
//! # Usage
//!
//! ```ignore
//! use standx_orderbook::trading::{AuthManager, Credentials, OrderWsClient, NewOrderRequest};
//! use std::sync::Arc;
//! use tokio::sync::Mutex;
//!
//! // Load credentials from .env file
//! dotenvy::dotenv().ok();
//!
//! // Create auth manager from environment
//! let mut auth = AuthManager::from_env()?;
//! auth.authenticate().await?;
//!
//! // Create WebSocket order client
//! let auth = Arc::new(Mutex::new(auth));
//! let mut order_client = OrderWsClient::new(Arc::clone(&auth));
//! let mut rx = order_client.connect().await?;
//!
//! // Place a limit order
//! let order = NewOrderRequest::limit_buy("TEST-USD", 90000.0, 0.001);
//! let cl_ord_id = order_client.place_order(&order).await?;
//! ```

pub mod auth;
pub mod client;
pub mod equity;
pub mod order_checker;
pub mod order_manager;
pub mod order_ws;
pub mod orders;
pub mod position;
pub mod symbol_info;
pub mod wallet_tracker;

pub use auth::{AuthManager, AuthToken, Credentials, AuthError, Authenticated};
pub use client::{
    StandXClient, ClientError, Position, Balance, DepthBookResponse,
    NewOrderRequest, OrderResponse, OpenOrder, CancelOrdersRequest,
};
pub use order_manager::{
    OrderManager as QuoteOrderManager, OrderManagerConfig, OrderDecision,
    LiveOrder, OrderState, Side, OrderManagerStats,
};
pub use order_ws::{OrderWsClient, OrderWsError, OrderEvent};
pub use orders::{
    OrderManager, OrderRequest, Order, OrderError,
    OrderSide, OrderType, OrderStatus, TimeInForce,
};
pub use position::{SharedPosition, PositionPoller, PositionPollerConfig, PositionPollerHandle};
pub use wallet_tracker::{WalletTracker, WalletTrackerConfig, WalletTrackerHandle};
pub use order_checker::{OpenOrdersChecker, OpenOrdersCheckerConfig, OpenOrdersCheckerHandle, ClearOrdersSignal};
pub use symbol_info::{SharedSymbolInfo, SymbolInfoPoller, SymbolInfoPollerConfig, SymbolInfoPollerHandle, TickSizeChangedSignal};
pub use client::SymbolInfo;
pub use equity::SharedEquity;

use std::sync::atomic::{AtomicU64, Ordering};

/// Shared trading statistics for tracking volume and prices.
///
/// Uses lock-free atomics for zero-contention reads/writes.
/// Designed to be updated in async event handlers (not hot path).
pub struct TradingStats {
    /// Cumulative traded volume in base asset (stored as f64 bits)
    total_volume_bits: AtomicU64,
    /// Cumulative traded volume in USD (stored as f64 bits)
    total_volume_usd_bits: AtomicU64,
    /// Current mid price for position USD calculation (stored as f64 bits)
    mid_price_bits: AtomicU64,
    /// Cumulative trade count
    trade_count: AtomicU64,
}

impl TradingStats {
    /// Create new trading stats with optional initial values (for persistence).
    pub fn new(initial_volume: f64, initial_volume_usd: f64, initial_trade_count: u64) -> Self {
        Self {
            total_volume_bits: AtomicU64::new(initial_volume.to_bits()),
            total_volume_usd_bits: AtomicU64::new(initial_volume_usd.to_bits()),
            mid_price_bits: AtomicU64::new(0.0_f64.to_bits()),
            trade_count: AtomicU64::new(initial_trade_count),
        }
    }

    /// Add to cumulative traded volume (both base asset and USD).
    /// Called from async fill event handler - not in hot path.
    #[inline]
    pub fn add_volume(&self, qty: f64, price: f64) {
        // Note: This is not strictly atomic addition, but for statistics
        // the occasional race is acceptable. True atomic f64 add would require
        // a CAS loop which is overkill for cumulative stats.
        let qty_abs = qty.abs();

        // Add base asset volume
        let current = f64::from_bits(self.total_volume_bits.load(Ordering::Relaxed));
        let new = current + qty_abs;
        self.total_volume_bits.store(new.to_bits(), Ordering::Relaxed);

        // Add USD volume
        let usd_volume = qty_abs * price;
        let current_usd = f64::from_bits(self.total_volume_usd_bits.load(Ordering::Relaxed));
        let new_usd = current_usd + usd_volume;
        self.total_volume_usd_bits.store(new_usd.to_bits(), Ordering::Relaxed);
    }

    /// Update the current mid price.
    /// Called from main loop on orderbook updates - single atomic store (~1ns).
    #[inline]
    pub fn set_mid_price(&self, price: f64) {
        self.mid_price_bits.store(price.to_bits(), Ordering::Relaxed);
    }

    /// Get total traded volume in base asset.
    pub fn total_volume(&self) -> f64 {
        f64::from_bits(self.total_volume_bits.load(Ordering::Relaxed))
    }

    /// Get total traded volume in USD.
    pub fn total_volume_usd(&self) -> f64 {
        f64::from_bits(self.total_volume_usd_bits.load(Ordering::Relaxed))
    }

    /// Get current mid price.
    pub fn mid_price(&self) -> f64 {
        f64::from_bits(self.mid_price_bits.load(Ordering::Relaxed))
    }

    /// Set initial volumes from persisted CSV data.
    /// Used on startup to restore cumulative volumes.
    pub fn set_initial_volumes(&self, volume: f64, volume_usd: f64) {
        self.total_volume_bits.store(volume.to_bits(), Ordering::Relaxed);
        self.total_volume_usd_bits.store(volume_usd.to_bits(), Ordering::Relaxed);
    }

    /// Increment trade count by 1.
    /// Called when a position change is detected (fill inferred).
    #[inline]
    pub fn increment_trade_count(&self) {
        self.trade_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total trade count.
    pub fn trade_count(&self) -> u64 {
        self.trade_count.load(Ordering::Relaxed)
    }

    /// Set initial trade count from persisted CSV data.
    /// Used on startup to restore cumulative trade count.
    pub fn set_initial_trade_count(&self, count: u64) {
        self.trade_count.store(count, Ordering::Relaxed);
    }
}

impl Default for TradingStats {
    fn default() -> Self {
        Self::new(0.0, 0.0, 0)
    }
}

/// Trading client combining authentication and order management.
pub struct TradingClient {
    /// Authentication manager
    pub auth: AuthManager,
    /// Whether trading is enabled
    enabled: bool,
}

impl TradingClient {
    /// Create a new trading client (disabled by default).
    pub fn new() -> Self {
        Self {
            auth: AuthManager::new(),
            enabled: false,
        }
    }

    /// Create a trading client with credentials.
    pub fn with_credentials(credentials: Credentials) -> Self {
        Self {
            auth: AuthManager::with_credentials(credentials),
            enabled: false,
        }
    }

    /// Enable trading.
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disable trading.
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Check if trading is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Check if ready to trade (enabled and authenticated).
    pub fn is_ready(&self) -> bool {
        self.enabled && self.auth.is_authenticated()
    }
}

impl Default for TradingClient {
    fn default() -> Self {
        Self::new()
    }
}

impl Authenticated for TradingClient {
    fn auth(&self) -> &AuthManager {
        &self.auth
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trading_client() {
        let mut client = TradingClient::new();
        assert!(!client.is_enabled());
        assert!(!client.is_ready());

        client.enable();
        assert!(client.is_enabled());
        // Still not ready without authentication
        assert!(!client.is_ready());
    }
}
