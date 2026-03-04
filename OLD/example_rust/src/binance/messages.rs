//! Binance WebSocket and REST API message parsing.
//!
//! This module handles deserialization of Binance depth (orderbook) messages
//! for both the REST snapshot endpoint and WebSocket incremental updates.

use serde::Deserialize;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;

/// Errors that can occur during message parsing.
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Failed to parse JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid message type: expected text")]
    InvalidMessageType,

    #[error("Failed to parse price: {0}")]
    InvalidPrice(String),

    #[error("Failed to parse quantity: {0}")]
    InvalidQuantity(String),
}

/// Binance REST API depth snapshot response.
///
/// Fetched from: `GET /api/v3/depth?symbol=BTCUSDT&limit=1000`
///
/// Example response:
/// ```json
/// {
///   "lastUpdateId": 1027024,
///   "bids": [["4.00000000", "431.00000000"]],
///   "asks": [["4.00000200", "12.00000000"]]
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceDepthSnapshot {
    /// Last update ID for sequence synchronization
    pub last_update_id: u64,

    /// Bid levels as [price, quantity] string pairs
    pub bids: Vec<[String; 2]>,

    /// Ask levels as [price, quantity] string pairs
    pub asks: Vec<[String; 2]>,
}

/// Binance WebSocket depth update event.
///
/// Works for both Spot and Futures streams.
///
/// Spot example (`wss://stream.binance.com:9443/ws/btcusdt@depth@100ms`):
/// ```json
/// {
///   "e": "depthUpdate",
///   "E": 123456789,
///   "s": "BTCUSDT",
///   "U": 157,
///   "u": 160,
///   "b": [["0.0024", "10"]],
///   "a": [["0.0026", "100"]]
/// }
/// ```
///
/// Futures example (`wss://fstream.binance.com/ws/btcusdt@depth@100ms`):
/// ```json
/// {
///   "e": "depthUpdate",
///   "E": 123456789,
///   "T": 123456788,
///   "s": "BTCUSDT",
///   "U": 157,
///   "u": 160,
///   "pu": 156,
///   "b": [["0.0024", "10"]],
///   "a": [["0.0026", "100"]]
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceDepthUpdate {
    /// Event type (should be "depthUpdate")
    #[serde(rename = "e")]
    pub event_type: String,

    /// Event time (milliseconds since epoch)
    #[serde(rename = "E")]
    pub event_time: u64,

    /// Symbol
    #[serde(rename = "s")]
    pub symbol: String,

    /// First update ID in event
    #[serde(rename = "U")]
    pub first_update_id: u64,

    /// Final update ID in event
    #[serde(rename = "u")]
    pub final_update_id: u64,

    /// Previous final update ID (Futures only)
    /// Used for continuity checking in Futures streams
    #[serde(rename = "pu")]
    pub prev_final_update_id: Option<u64>,

    /// Bid delta levels as [price, quantity] string pairs
    /// Quantity of 0 means remove the level
    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,

    /// Ask delta levels as [price, quantity] string pairs
    /// Quantity of 0 means remove the level
    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
}

impl BinanceDepthUpdate {
    /// Check if this is a Futures update (has `pu` field).
    pub fn is_futures(&self) -> bool {
        self.prev_final_update_id.is_some()
    }
}

/// Parse a WebSocket message into a depth update.
///
/// Returns `None` for ping/pong/close frames (handled by tungstenite).
pub fn parse_depth_update(msg: &Message) -> Result<Option<BinanceDepthUpdate>, ParseError> {
    match msg {
        Message::Text(text) => {
            let update: BinanceDepthUpdate = serde_json::from_str(text)?;
            Ok(Some(update))
        }
        Message::Binary(data) => {
            let update: BinanceDepthUpdate = serde_json::from_slice(data)?;
            Ok(Some(update))
        }
        // Ping/Pong/Close are handled by tokio-tungstenite
        Message::Ping(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_) => Ok(None),
    }
}

/// Parse a REST API response into a depth snapshot.
pub fn parse_snapshot(json: &str) -> Result<BinanceDepthSnapshot, ParseError> {
    let snapshot: BinanceDepthSnapshot = serde_json::from_str(json)?;
    Ok(snapshot)
}

/// Parse a price/quantity string pair into f64 values.
///
/// Uses fast-float for high-performance parsing.
#[inline]
pub fn parse_level(level: &[String; 2]) -> Result<(f64, f64), ParseError> {
    let price: f64 = fast_float::parse(&level[0])
        .map_err(|_| ParseError::InvalidPrice(level[0].clone()))?;
    let qty: f64 = fast_float::parse(&level[1])
        .map_err(|_| ParseError::InvalidQuantity(level[1].clone()))?;
    Ok((price, qty))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_depth_snapshot() {
        let json = r#"{
            "lastUpdateId": 1027024,
            "bids": [
                ["4.00000000", "431.00000000"],
                ["3.99000000", "100.00000000"]
            ],
            "asks": [
                ["4.00000200", "12.00000000"],
                ["5.00000000", "50.00000000"]
            ]
        }"#;

        let snapshot = parse_snapshot(json).unwrap();
        assert_eq!(snapshot.last_update_id, 1027024);
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);
        assert_eq!(snapshot.bids[0][0], "4.00000000");
        assert_eq!(snapshot.bids[0][1], "431.00000000");
    }

    #[test]
    fn test_parse_depth_update() {
        let json = r#"{
            "e": "depthUpdate",
            "E": 123456789,
            "s": "BTCUSDT",
            "U": 157,
            "u": 160,
            "b": [["0.0024", "10"]],
            "a": [["0.0026", "100"]]
        }"#;

        let msg = Message::Text(json.to_string());
        let update = parse_depth_update(&msg).unwrap().unwrap();

        assert_eq!(update.event_type, "depthUpdate");
        assert_eq!(update.event_time, 123456789);
        assert_eq!(update.symbol, "BTCUSDT");
        assert_eq!(update.first_update_id, 157);
        assert_eq!(update.final_update_id, 160);
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.asks.len(), 1);
    }

    #[test]
    fn test_parse_level() {
        let level = ["100.50".to_string(), "1.25".to_string()];
        let (price, qty) = parse_level(&level).unwrap();
        assert!((price - 100.50).abs() < 1e-10);
        assert!((qty - 1.25).abs() < 1e-10);
    }

    #[test]
    fn test_parse_ping_returns_none() {
        let msg = Message::Ping(vec![]);
        assert!(parse_depth_update(&msg).unwrap().is_none());
    }
}
