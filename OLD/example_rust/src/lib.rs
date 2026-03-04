//! # StandX Market Maker
//!
//! High-performance market making system for the StandX perpetual futures exchange.
//!
//! ## Features
//!
//! - **Lock-free data structures**: Triple buffer for current state,
//!   ring buffer for history (no mutexes on hot path)
//! - **OBI Strategy**: Order Book Imbalance based quote generation with
//!   volatility scaling and position skew adjustment
//! - **Low-latency trading**: WebSocket order API (5-50ms) with HTTP fallback
//! - **Position management**: Background polling with atomic updates (lock-free reads)
//! - **Auto-reconnection**: WebSocket clients with exponential backoff
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Market Data (WebSocket)                   │
//! │  Orderbook ─► Snapshot ─► OBI Strategy ─► Quote Generation  │
//! └─────────────────────────────────────────────────────────────┘
//!                              ▲
//!                              │ lock-free read
//!               ┌──────────────┴──────────────┐
//!               │   SharedPosition (Atomic)   │
//!               └──────────────▲──────────────┘
//!                              │ background update
//! ┌─────────────────────────────────────────────────────────────┐
//! │               Position Poller (HTTP, 2s interval)           │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//! ┌─────────────────────────────────────────────────────────────┐
//! │                 Order Execution (WebSocket)                  │
//! │    NewOrder ─► OrderWsClient ─► Exchange ─► Confirmation    │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Modules
//!
//! - [`types`]: Core data types (PriceLevel, OrderbookSnapshot, Symbol)
//! - [`config`]: Configuration loading and validation
//! - [`orderbook`]: Lock-free orderbook storage
//! - [`websocket`]: WebSocket client for market data
//! - [`trading`]: Authentication, order management, position polling
//! - [`logging`]: Centralized logging with debug flag control
//! - [`strategy`]: OBI market making strategy and quote generation
//!
//! ## Quick Start
//!
//! ```ignore
//! use std::sync::Arc;
//! use standx_orderbook::{Config, OrderbookStore, WsClient, WsClientBuilder, WsEvent};
//!
//! #[tokio::main]
//! async fn main() {
//!     // Load configuration
//!     let config = Config::from_file("config.json").unwrap();
//!
//!     // Create orderbook store
//!     let store = Arc::new(OrderbookStore::new(
//!         &config.symbols,
//!         config.history_buffer_size,
//!         config.history_minutes,
//!     ));
//!
//!     // Create WebSocket client
//!     let client = WsClientBuilder::new()
//!         .config(config.websocket.clone())
//!         .symbols(config.symbols.clone())
//!         .build();
//!
//!     // Run client and process messages
//!     let mut rx = client.run().await;
//!     while let Some(event) = rx.recv().await {
//!         match event {
//!             WsEvent::Message(msg, received_at) => {
//!                 // Process orderbook updates
//!             }
//!             _ => {}
//!         }
//!     }
//! }
//! ```

pub mod binance;
pub mod config;
pub mod logging;
pub mod orderbook;
pub mod strategy;
pub mod trading;
pub mod types;
pub mod websocket;

// Re-export commonly used types
pub use config::{Config, ConfigError, WebSocketConfig, StrategyConfig, PositionConfig, OrderConfig, WalletConfig, SanityCheckConfig, SymbolInfoConfig};
pub use logging::{init as init_logging, is_enabled as logging_enabled, logger};
pub use orderbook::{
    CurrentOrderbook, OrderbookHistory, OrderbookManager, OrderbookStore,
    SymbolOrderbook, OrderbookStats,
    OrderbookSanityChecker, SanityCheckerConfig, SanityCheckerHandle, SanityCheckerStats,
};
pub use strategy::{QuoteStrategy, ObiStrategy, Quote, QuoteFormatter, RollingStats, RollingWindow};
pub use trading::{
    AuthManager, SharedPosition, PositionPoller, PositionPollerConfig, PositionPollerHandle,
    QuoteOrderManager, OrderManagerConfig, OrderDecision, LiveOrder, OrderState, Side, OrderManagerStats,
    WalletTracker, WalletTrackerConfig, WalletTrackerHandle,
    OpenOrdersChecker, OpenOrdersCheckerConfig, OpenOrdersCheckerHandle, ClearOrdersSignal,
    SharedSymbolInfo, SymbolInfoPoller, SymbolInfoPollerConfig, SymbolInfoPollerHandle, TickSizeChangedSignal,
};
pub use types::{OrderbookSnapshot, PriceLevel, Symbol, MAX_LEVELS};
pub use binance::{
    BinanceClient, BinanceClientConfig, BinanceEvent, BinanceObiCalculator,
    BinanceOrderbook, BinanceWsStats, BinanceWsStatsSnapshot,
    SharedAlpha, BinanceAlphaPollerHandle, start_binance_alpha_poller,
};
pub use websocket::{
    WsClient, WsClientBuilder, WsEvent, WsStats, WsStatsSnapshot,
    StandXMessage, DepthBookData, MessageError,
    current_time_ns,
};

/// Library version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default WebSocket URL for market data.
pub const WS_STREAM_URL: &str = "wss://perps.standx.com/ws-stream/v1";

/// Default WebSocket URL for orders.
pub const WS_API_URL: &str = "wss://perps.standx.com/ws-api/v1";
