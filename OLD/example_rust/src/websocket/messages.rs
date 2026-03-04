//! StandX WebSocket message parsing.
//!
//! This module handles deserialization of StandX market data messages
//! including orderbook depth, trades, and prices.

use serde::Deserialize;
use chrono::DateTime;
use thiserror::Error;

use crate::types::{OrderbookSnapshot, Symbol};

/// Message parsing errors.
#[derive(Error, Debug)]
pub enum MessageError {
    #[error("JSON parse error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Invalid price format: {0}")]
    InvalidPrice(String),

    #[error("Invalid quantity format: {0}")]
    InvalidQuantity(String),

    #[error("Invalid timestamp format: {0}")]
    InvalidTimestamp(String),

    #[error("Unknown channel: {0}")]
    UnknownChannel(String),

    #[error("Missing required field: {0}")]
    MissingField(String),
}

/// Raw WebSocket message from StandX.
#[derive(Debug, Deserialize)]
pub struct RawMessage {
    /// Sequence number
    #[allow(dead_code)]
    pub seq: Option<u64>,
    /// Channel name
    pub channel: Option<String>,
    /// Message data (varies by channel)
    pub data: Option<serde_json::Value>,
    /// Error code (if error message)
    pub code: Option<i32>,
    /// Error message
    pub message: Option<String>,
}

/// Depth book (orderbook) data from the depth_book channel.
#[derive(Debug, Deserialize)]
pub struct DepthBookData {
    pub symbol: String,
    pub asks: Vec<(String, String)>,
    pub bids: Vec<(String, String)>,
    pub sequence: Option<u64>,
    /// Timestamp - can be integer (ms) or string (ISO 8601)
    #[serde(default)]
    pub time: Option<serde_json::Value>,
    /// Last traded price (sometimes included)
    #[serde(default)]
    pub last_price: Option<String>,
    /// Mark price (sometimes included)
    #[serde(default)]
    pub mark_price: Option<String>,
}

/// Price data from the price channel.
#[derive(Debug, Deserialize)]
pub struct PriceData {
    pub symbol: String,
    pub index_price: Option<String>,
    pub last_price: Option<String>,
    pub mark_price: Option<String>,
    pub mid_price: Option<String>,
    pub spread: Option<(String, String)>,
    pub time: Option<String>,
}

/// Trade data from the public_trade channel.
#[derive(Debug, Deserialize)]
pub struct TradeData {
    pub symbol: String,
    pub price: String,
    pub qty: String,
    pub quote_qty: Option<String>,
    pub is_buyer_taker: bool,
    pub time: Option<String>,
}

/// Parsed StandX message.
#[derive(Debug)]
pub enum StandXMessage {
    /// Orderbook depth update
    DepthBook(DepthBookData),
    /// Price update
    Price(PriceData),
    /// Public trade
    Trade(TradeData),
    /// Authentication response
    Auth { code: i32, message: String },
    /// Error message
    Error { code: i32, message: String },
    /// Unknown or unhandled message
    Unknown(serde_json::Value),
}

impl StandXMessage {
    /// Parse a raw JSON message from the WebSocket.
    pub fn parse(data: &[u8]) -> Result<Self, MessageError> {
        let raw: RawMessage = serde_json::from_slice(data)?;

        // Parse based on channel first
        match raw.channel.as_deref() {
            Some("depth_book") => {
                let data = raw.data.ok_or(MessageError::MissingField("data".into()))?;
                let depth: DepthBookData = serde_json::from_value(data)?;
                Ok(StandXMessage::DepthBook(depth))
            }
            Some("price") => {
                let data = raw.data.ok_or(MessageError::MissingField("data".into()))?;
                let price: PriceData = serde_json::from_value(data)?;
                Ok(StandXMessage::Price(price))
            }
            Some("public_trade") => {
                let data = raw.data.ok_or(MessageError::MissingField("data".into()))?;
                let trade: TradeData = serde_json::from_value(data)?;
                Ok(StandXMessage::Trade(trade))
            }
            Some("auth") => {
                let data = raw.data.ok_or(MessageError::MissingField("data".into()))?;
                let code = data.get("code")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i32;
                let message = data.get("msg")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                Ok(StandXMessage::Auth { code, message })
            }
            Some(channel) => {
                // Handle other channels or return as unknown
                if let Some(data) = raw.data {
                    Ok(StandXMessage::Unknown(data))
                } else {
                    Err(MessageError::UnknownChannel(channel.to_string()))
                }
            }
            None => {
                // No channel specified, might be an error or response
                if let Some(code) = raw.code {
                    let message = raw.message.unwrap_or_default();
                    if code != 0 {
                        Ok(StandXMessage::Error { code, message })
                    } else {
                        // Success response without channel
                        if let Some(data) = raw.data {
                            Ok(StandXMessage::Unknown(data))
                        } else {
                            let value = serde_json::from_slice(data)?;
                            Ok(StandXMessage::Unknown(value))
                        }
                    }
                } else if let Some(data) = raw.data {
                    Ok(StandXMessage::Unknown(data))
                } else {
                    // Try to return the whole raw message
                    let value = serde_json::from_slice(data)?;
                    Ok(StandXMessage::Unknown(value))
                }
            }
        }
    }

    /// Parse from a string.
    pub fn parse_str(s: &str) -> Result<Self, MessageError> {
        Self::parse(s.as_bytes())
    }
}

/// Convert depth book data to an OrderbookSnapshot.
impl DepthBookData {
    /// Convert to an OrderbookSnapshot.
    ///
    /// # Arguments
    ///
    /// * `max_levels` - Maximum number of levels to include
    /// * `received_at_ns` - Local receive timestamp in nanoseconds
    ///
    /// # Performance
    ///
    /// This method is optimized for low latency:
    /// - Parses strings directly into fixed arrays (no intermediate Vec allocation)
    /// - Uses fast_float for ~3x faster float parsing
    /// - Assumes exchange data is pre-sorted (skips sorting)
    #[inline]
    pub fn to_snapshot(&self, max_levels: usize, received_at_ns: i64) -> Result<OrderbookSnapshot, MessageError> {
        let mut snapshot = OrderbookSnapshot::new(Symbol::new(&self.symbol));

        // Parse directly into fixed arrays (no intermediate Vec allocation)
        snapshot.set_bids_from_strings(&self.bids, max_levels);
        snapshot.set_asks_from_strings(&self.asks, max_levels);

        // Set sequence
        snapshot.sequence = self.sequence.unwrap_or(0);

        // Parse timestamp (can be integer ms or string ISO 8601)
        if let Some(ref time_val) = self.time {
            snapshot.timestamp_ns = parse_timestamp_value(time_val)?;
        }

        snapshot.received_at_ns = received_at_ns;

        Ok(snapshot)
    }
}

/// Parse timestamp from JSON value (integer ms or string ISO 8601).
pub fn parse_timestamp_value(val: &serde_json::Value) -> Result<i64, MessageError> {
    match val {
        serde_json::Value::Number(n) => {
            // Integer timestamp in milliseconds
            let ms = n.as_i64().unwrap_or(0);
            Ok(ms * 1_000_000) // Convert to nanoseconds
        }
        serde_json::Value::String(s) => {
            parse_timestamp(s)
        }
        _ => Ok(0),
    }
}

/// Parse ISO 8601 timestamp to nanoseconds since Unix epoch.
///
/// Optimized to avoid string allocation - uses stack buffer for 'Z' suffix.
#[inline]
pub fn parse_timestamp(s: &str) -> Result<i64, MessageError> {
    // Try parsing as ISO 8601 (handles both with and without 'Z')
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_nanos_opt().unwrap_or(0));
    }

    // If no 'Z' suffix, try appending it without heap allocation
    if !s.ends_with('Z') && s.len() < 64 {
        // Use stack buffer to avoid heap allocation
        let mut buf = [0u8; 64];
        let bytes = s.as_bytes();
        buf[..bytes.len()].copy_from_slice(bytes);
        buf[bytes.len()] = b'Z';

        if let Ok(s_with_z) = std::str::from_utf8(&buf[..bytes.len() + 1]) {
            if let Ok(dt) = DateTime::parse_from_rfc3339(s_with_z) {
                return Ok(dt.timestamp_nanos_opt().unwrap_or(0));
            }
        }
    }

    // Fallback - only allocate on error path (cold)
    Err(MessageError::InvalidTimestamp(s.to_string()))
}

/// Get current time in nanoseconds since Unix epoch.
pub fn current_time_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Create a subscription message for a channel.
pub fn subscribe_message(channel: &str, symbol: &str) -> String {
    serde_json::json!({
        "subscribe": {
            "channel": channel,
            "symbol": symbol
        }
    }).to_string()
}

/// Create an unsubscription message for a channel.
pub fn unsubscribe_message(channel: &str, symbol: &str) -> String {
    serde_json::json!({
        "unsubscribe": {
            "channel": channel,
            "symbol": symbol
        }
    }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_depth_book() {
        let json = r#"{
            "seq": 1,
            "channel": "depth_book",
            "data": {
                "symbol": "TEST-USD",
                "asks": [["101.00", "1.0"], ["102.00", "2.0"]],
                "bids": [["100.00", "1.0"], ["99.00", "2.0"]],
                "sequence": 12345,
                "time": "2025-08-11T03:44:40.922233Z"
            }
        }"#;

        let msg = StandXMessage::parse_str(json).unwrap();
        match msg {
            StandXMessage::DepthBook(data) => {
                assert_eq!(data.symbol, "TEST-USD");
                assert_eq!(data.asks.len(), 2);
                assert_eq!(data.bids.len(), 2);
                assert_eq!(data.sequence, Some(12345));

                let snapshot = data.to_snapshot(20, 0).unwrap();
                assert_eq!(snapshot.best_bid_price(), Some(100.0));
                assert_eq!(snapshot.best_ask_price(), Some(101.0));
            }
            _ => panic!("Expected DepthBook message"),
        }
    }

    #[test]
    fn test_parse_trade() {
        let json = r#"{
            "seq": 1,
            "channel": "public_trade",
            "data": {
                "symbol": "TEST-USD",
                "price": "121720.18",
                "qty": "0.01",
                "quote_qty": "1217.2018",
                "is_buyer_taker": true,
                "time": "2025-08-11T03:48:47.086505Z"
            }
        }"#;

        let msg = StandXMessage::parse_str(json).unwrap();
        match msg {
            StandXMessage::Trade(data) => {
                assert_eq!(data.symbol, "TEST-USD");
                assert_eq!(data.price, "121720.18");
                assert!(data.is_buyer_taker);
            }
            _ => panic!("Expected Trade message"),
        }
    }

    #[test]
    fn test_parse_error() {
        let json = r#"{
            "code": 400,
            "message": "Bad request"
        }"#;

        let msg = StandXMessage::parse_str(json).unwrap();
        match msg {
            StandXMessage::Error { code, message } => {
                assert_eq!(code, 400);
                assert_eq!(message, "Bad request");
            }
            _ => panic!("Expected Error message"),
        }
    }

    #[test]
    fn test_subscribe_message() {
        let msg = subscribe_message("depth_book", "TEST-USD");
        assert!(msg.contains("depth_book"));
        assert!(msg.contains("TEST-USD"));
    }
}
