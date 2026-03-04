//! Order management module for StandX trading.
//!
//! This module provides order submission and management via
//! the StandX WebSocket API. Currently a stub for future implementation.
//!
//! # Order Types
//!
//! - `limit`: Execute at specified price or better
//! - `market`: Execute immediately at best available price
//!
//! # Time in Force
//!
//! - `gtc`: Good Til Canceled
//! - `ioc`: Immediate Or Cancel
//! - `alo`: Add Liquidity Only (maker only)

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

use super::auth::AuthManager;

/// Order errors.
#[derive(Error, Debug)]
pub enum OrderError {
    #[error("Not authenticated")]
    NotAuthenticated,

    #[error("Invalid order: {0}")]
    InvalidOrder(String),

    #[error("Order rejected: {0}")]
    Rejected(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Order not found: {0}")]
    NotFound(String),
}

/// Order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    /// Get the string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        }
    }
}

/// Order type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
}

impl OrderType {
    /// Get the string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
        }
    }
}

/// Time in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    /// Good Til Canceled
    GTC,
    /// Immediate Or Cancel
    IOC,
    /// Add Liquidity Only (maker only)
    ALO,
}

impl TimeInForce {
    /// Get the string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeInForce::GTC => "gtc",
            TimeInForce::IOC => "ioc",
            TimeInForce::ALO => "alo",
        }
    }
}

/// Order status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Open,
    Filled,
    Canceled,
    Rejected,
    Untriggered,
}

impl OrderStatus {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "open" => Some(OrderStatus::Open),
            "filled" => Some(OrderStatus::Filled),
            "canceled" => Some(OrderStatus::Canceled),
            "rejected" => Some(OrderStatus::Rejected),
            "untriggered" => Some(OrderStatus::Untriggered),
            _ => None,
        }
    }
}

/// Order request parameters.
#[derive(Debug, Clone)]
pub struct OrderRequest {
    /// Trading symbol
    pub symbol: String,
    /// Order side
    pub side: OrderSide,
    /// Order type
    pub order_type: OrderType,
    /// Quantity
    pub quantity: f64,
    /// Price (required for limit orders)
    pub price: Option<f64>,
    /// Time in force
    pub time_in_force: TimeInForce,
    /// Reduce only flag
    pub reduce_only: bool,
    /// Client order ID (auto-generated if None)
    pub client_order_id: Option<String>,
    /// Leverage (must match position)
    pub leverage: Option<u32>,
}

impl OrderRequest {
    /// Create a limit buy order.
    pub fn limit_buy(symbol: impl Into<String>, price: f64, quantity: f64) -> Self {
        Self {
            symbol: symbol.into(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            quantity,
            price: Some(price),
            time_in_force: TimeInForce::GTC,
            reduce_only: false,
            client_order_id: None,
            leverage: None,
        }
    }

    /// Create a limit sell order.
    pub fn limit_sell(symbol: impl Into<String>, price: f64, quantity: f64) -> Self {
        Self {
            symbol: symbol.into(),
            side: OrderSide::Sell,
            order_type: OrderType::Limit,
            quantity,
            price: Some(price),
            time_in_force: TimeInForce::GTC,
            reduce_only: false,
            client_order_id: None,
            leverage: None,
        }
    }

    /// Create a market buy order.
    pub fn market_buy(symbol: impl Into<String>, quantity: f64) -> Self {
        Self {
            symbol: symbol.into(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            quantity,
            price: None,
            time_in_force: TimeInForce::IOC,
            reduce_only: false,
            client_order_id: None,
            leverage: None,
        }
    }

    /// Create a market sell order.
    pub fn market_sell(symbol: impl Into<String>, quantity: f64) -> Self {
        Self {
            symbol: symbol.into(),
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            quantity,
            price: None,
            time_in_force: TimeInForce::IOC,
            reduce_only: false,
            client_order_id: None,
            leverage: None,
        }
    }

    /// Set as reduce only.
    pub fn reduce_only(mut self) -> Self {
        self.reduce_only = true;
        self
    }

    /// Set time in force.
    pub fn with_tif(mut self, tif: TimeInForce) -> Self {
        self.time_in_force = tif;
        self
    }

    /// Set client order ID.
    pub fn with_client_id(mut self, id: impl Into<String>) -> Self {
        self.client_order_id = Some(id.into());
        self
    }

    /// Set leverage.
    pub fn with_leverage(mut self, leverage: u32) -> Self {
        self.leverage = Some(leverage);
        self
    }

    /// Validate the order request.
    pub fn validate(&self) -> Result<(), OrderError> {
        if self.quantity <= 0.0 {
            return Err(OrderError::InvalidOrder("Quantity must be positive".into()));
        }

        if self.order_type == OrderType::Limit && self.price.is_none() {
            return Err(OrderError::InvalidOrder("Limit orders require a price".into()));
        }

        if let Some(price) = self.price {
            if price <= 0.0 {
                return Err(OrderError::InvalidOrder("Price must be positive".into()));
            }
        }

        Ok(())
    }
}

/// Order response from the exchange.
#[derive(Debug, Clone)]
pub struct Order {
    /// Exchange order ID
    pub order_id: u64,
    /// Client order ID
    pub client_order_id: String,
    /// Trading symbol
    pub symbol: String,
    /// Order side
    pub side: OrderSide,
    /// Order type
    pub order_type: OrderType,
    /// Order status
    pub status: OrderStatus,
    /// Order price
    pub price: f64,
    /// Order quantity
    pub quantity: f64,
    /// Filled quantity
    pub filled_quantity: f64,
    /// Average fill price
    pub avg_fill_price: f64,
    /// Leverage
    pub leverage: u32,
    /// Time in force
    pub time_in_force: TimeInForce,
    /// Reduce only flag
    pub reduce_only: bool,
    /// Created timestamp (milliseconds)
    pub created_at_ms: u64,
    /// Updated timestamp (milliseconds)
    pub updated_at_ms: u64,
}

/// Order manager for submitting and tracking orders.
///
/// Requires authentication via AuthManager.
pub struct OrderManager {
    /// Authentication manager
    auth: Arc<AuthManager>,
    /// Number of orders submitted
    orders_submitted: AtomicU64,
    /// Number of orders filled
    orders_filled: AtomicU64,
    /// Number of orders canceled
    orders_canceled: AtomicU64,
}

impl OrderManager {
    /// Create a new order manager.
    pub fn new(auth: Arc<AuthManager>) -> Self {
        Self {
            auth,
            orders_submitted: AtomicU64::new(0),
            orders_filled: AtomicU64::new(0),
            orders_canceled: AtomicU64::new(0),
        }
    }

    /// Submit a new order.
    ///
    /// # Stub Implementation
    ///
    /// This is a stub that will need to be implemented with actual
    /// WebSocket order submission.
    pub async fn submit(&self, request: OrderRequest) -> Result<String, OrderError> {
        if !self.auth.is_authenticated() {
            return Err(OrderError::NotAuthenticated);
        }

        request.validate()?;

        // TODO: Implement actual order submission via WebSocket
        // 1. Generate client_order_id if not provided
        // 2. Sign the request
        // 3. Send via ws-api/v1 with method "order:new"

        self.orders_submitted.fetch_add(1, Ordering::Relaxed);

        Err(OrderError::NetworkError("Not implemented".into()))
    }

    /// Cancel an order by ID.
    pub async fn cancel(&self, _order_id: u64) -> Result<(), OrderError> {
        if !self.auth.is_authenticated() {
            return Err(OrderError::NotAuthenticated);
        }

        // TODO: Implement actual order cancellation
        // 1. Sign the request
        // 2. Send via ws-api/v1 with method "order:cancel"

        Err(OrderError::NetworkError("Not implemented".into()))
    }

    /// Cancel an order by client order ID.
    pub async fn cancel_by_client_id(&self, _client_order_id: &str) -> Result<(), OrderError> {
        if !self.auth.is_authenticated() {
            return Err(OrderError::NotAuthenticated);
        }

        // TODO: Implement actual order cancellation
        Err(OrderError::NetworkError("Not implemented".into()))
    }

    /// Get order statistics.
    pub fn stats(&self) -> OrderManagerStats {
        OrderManagerStats {
            orders_submitted: self.orders_submitted.load(Ordering::Relaxed),
            orders_filled: self.orders_filled.load(Ordering::Relaxed),
            orders_canceled: self.orders_canceled.load(Ordering::Relaxed),
        }
    }
}

/// Order manager statistics.
#[derive(Debug, Clone)]
pub struct OrderManagerStats {
    pub orders_submitted: u64,
    pub orders_filled: u64,
    pub orders_canceled: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_request_validation() {
        let valid = OrderRequest::limit_buy("TEST-USD", 50000.0, 0.1);
        assert!(valid.validate().is_ok());

        let no_qty = OrderRequest::limit_buy("TEST-USD", 50000.0, 0.0);
        assert!(no_qty.validate().is_err());

        let market_no_price = OrderRequest::market_buy("TEST-USD", 0.1);
        assert!(market_no_price.validate().is_ok());
    }

    #[test]
    fn test_order_sides() {
        assert_eq!(OrderSide::Buy.as_str(), "buy");
        assert_eq!(OrderSide::Sell.as_str(), "sell");
    }

    #[test]
    fn test_time_in_force() {
        assert_eq!(TimeInForce::GTC.as_str(), "gtc");
        assert_eq!(TimeInForce::IOC.as_str(), "ioc");
        assert_eq!(TimeInForce::ALO.as_str(), "alo");
    }
}
