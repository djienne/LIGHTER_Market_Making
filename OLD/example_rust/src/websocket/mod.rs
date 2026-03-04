//! WebSocket client module for StandX market data.
//!
//! This module provides:
//! - [`WsClient`]: WebSocket client with automatic reconnection
//! - [`StandXMessage`]: Parsed message types
//! - Message parsing utilities
//! - [`ReconnectConfig`]: Shared reconnection configuration

mod client;
mod messages;
pub mod reconnect;

pub use client::{WsClient, WsClientBuilder, WsEvent, WsStats, WsStatsSnapshot};
pub use messages::{
    StandXMessage, DepthBookData, PriceData, TradeData,
    MessageError, subscribe_message, unsubscribe_message,
    parse_timestamp, current_time_ns,
};
pub use reconnect::{ReconnectConfig, ReconnectState};
