//! WebSocket client with automatic reconnection.
//!
//! This module provides a robust WebSocket client for connecting to
//! StandX market data streams with automatic reconnection and
//! exponential backoff using the shared reconnection infrastructure.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, Error as WsError},
};
use tracing::{debug, error, info, warn};

use crate::config::WebSocketConfig;
use super::messages::{StandXMessage, subscribe_message, current_time_ns};
use super::reconnect::{ReconnectConfig, ReconnectState};

/// WebSocket client events.
#[derive(Debug)]
pub enum WsEvent {
    /// Connected to the server
    Connected,
    /// Disconnected from the server
    Disconnected(String),
    /// Received a message
    Message(StandXMessage, i64),  // Message and receive timestamp (ns)
    /// Parse error
    ParseError(String),
    /// Connection error
    Error(String),
}

/// WebSocket client statistics.
#[derive(Debug, Default)]
pub struct WsStats {
    /// Total messages received
    pub messages_received: AtomicU64,
    /// Total reconnection attempts
    pub reconnect_count: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
    /// Last message timestamp
    pub last_message_ns: AtomicU64,
}

impl WsStats {
    /// Get a snapshot of the current statistics.
    pub fn snapshot(&self) -> WsStatsSnapshot {
        WsStatsSnapshot {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            reconnect_count: self.reconnect_count.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            last_message_ns: self.last_message_ns.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of WebSocket statistics.
#[derive(Debug, Clone)]
pub struct WsStatsSnapshot {
    pub messages_received: u64,
    pub reconnect_count: u64,
    pub bytes_received: u64,
    pub last_message_ns: u64,
}

/// WebSocket client for StandX market data.
pub struct WsClient {
    /// WebSocket URL
    url: String,
    /// Reconnection configuration
    reconnect_config: ReconnectConfig,
    /// Symbols to subscribe to
    symbols: Vec<String>,
    /// Whether the client is running
    running: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<WsStats>,
}

impl WsClient {
    /// Create a new WebSocket client from WebSocketConfig.
    pub fn new(config: WebSocketConfig, symbols: Vec<String>) -> Self {
        Self {
            url: config.url.clone(),
            reconnect_config: config.to_reconnect_config(),
            symbols,
            running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(WsStats::default()),
        }
    }

    /// Create a new WebSocket client with explicit ReconnectConfig.
    pub fn with_reconnect_config(url: String, reconnect_config: ReconnectConfig, symbols: Vec<String>) -> Self {
        Self {
            url,
            reconnect_config,
            symbols,
            running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(WsStats::default()),
        }
    }

    /// Get the client statistics.
    pub fn stats(&self) -> Arc<WsStats> {
        Arc::clone(&self.stats)
    }

    /// Check if the client is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Stop the client.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Run the WebSocket client with automatic reconnection.
    ///
    /// Returns a receiver for WebSocket events.
    pub async fn run(self: Arc<Self>) -> mpsc::Receiver<WsEvent> {
        let (tx, rx) = mpsc::channel(10000);
        let client = Arc::clone(&self);

        self.running.store(true, Ordering::Release);

        tokio::spawn(async move {
            client.connection_loop(tx).await;
        });

        rx
    }

    /// Main connection loop with exponential backoff reconnection.
    /// Uses the shared ReconnectState for consistent backoff behavior.
    async fn connection_loop(&self, tx: mpsc::Sender<WsEvent>) {
        let mut reconnect_state = ReconnectState::new(&self.reconnect_config);

        while self.running.load(Ordering::Acquire) {
            info!("Connecting to {}", self.url);

            match self.connect_and_run(&tx).await {
                Ok(_) => {
                    info!("Connection closed gracefully");
                    reconnect_state.reset(&self.reconnect_config);
                }
                Err(e) => {
                    error!("Connection error: {}", e);
                    let _ = tx.send(WsEvent::Error(e.to_string())).await;
                }
            }

            if !self.running.load(Ordering::Acquire) {
                break;
            }

            // Reconnection with exponential backoff using shared state
            // For orderbook WS, max_retries is None so this always returns Some
            match reconnect_state.next_delay(&self.reconnect_config) {
                Some(delay) => {
                    self.stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.send(WsEvent::Disconnected(format!("Reconnecting in {}s", delay))).await;

                    warn!("Reconnecting in {}s (attempt #{})",
                        delay,
                        reconnect_state.reconnect_count()
                    );

                    sleep(Duration::from_secs(delay)).await;
                }
                None => {
                    // This shouldn't happen for orderbook WS (unlimited retries)
                    // but handle it gracefully
                    error!("Max retries exceeded, stopping");
                    break;
                }
            }
        }

        info!("WebSocket client stopped");
    }

    /// Connect and run the message loop.
    async fn connect_and_run(&self, tx: &mpsc::Sender<WsEvent>) -> Result<(), WsError> {
        // Connect with timeout
        let connect_timeout = Duration::from_secs(self.reconnect_config.connect_timeout_secs);
        let (ws_stream, _) = timeout(connect_timeout, connect_async(&self.url))
            .await
            .map_err(|_| WsError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Connection timeout"
            )))??;

        info!("Connected to {}", self.url);
        let _ = tx.send(WsEvent::Connected).await;

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to channels
        for symbol in &self.symbols {
            let sub_msg = subscribe_message("depth_book", symbol);
            debug!("Subscribing to depth_book for {}", symbol);
            write.send(Message::Text(sub_msg)).await?;
        }

        // Message receive loop
        let stale_timeout = Duration::from_secs(self.reconnect_config.stale_timeout_secs);
        let mut last_message = Instant::now();

        loop {
            if !self.running.load(Ordering::Acquire) {
                break;
            }

            // Check for stale connection
            if last_message.elapsed() > stale_timeout {
                warn!("Connection stale (no message for {:?}), reconnecting", stale_timeout);
                break;
            }

            // Read with timeout
            let read_timeout = Duration::from_secs(30);
            match timeout(read_timeout, read.next()).await {
                Ok(Some(Ok(msg))) => {
                    last_message = Instant::now();
                    self.handle_message(msg, tx).await;
                }
                Ok(Some(Err(e))) => {
                    error!("WebSocket error: {}", e);
                    return Err(e);
                }
                Ok(None) => {
                    info!("WebSocket stream ended");
                    break;
                }
                Err(_) => {
                    // Timeout, continue loop
                    continue;
                }
            }
        }

        Ok(())
    }

    /// Handle a received WebSocket message.
    async fn handle_message(&self, msg: Message, tx: &mpsc::Sender<WsEvent>) {
        let received_at = current_time_ns();

        match msg {
            Message::Text(text) => {
                self.stats.bytes_received.fetch_add(text.len() as u64, Ordering::Relaxed);
                self.stats.messages_received.fetch_add(1, Ordering::Relaxed);
                self.stats.last_message_ns.store(received_at as u64, Ordering::Relaxed);

                match StandXMessage::parse_str(&text) {
                    Ok(parsed) => {
                        let _ = tx.send(WsEvent::Message(parsed, received_at)).await;
                    }
                    Err(e) => {
                        debug!("Failed to parse message: {}", e);
                        let _ = tx.send(WsEvent::ParseError(e.to_string())).await;
                    }
                }
            }
            Message::Binary(data) => {
                self.stats.bytes_received.fetch_add(data.len() as u64, Ordering::Relaxed);
                self.stats.messages_received.fetch_add(1, Ordering::Relaxed);
                self.stats.last_message_ns.store(received_at as u64, Ordering::Relaxed);

                match StandXMessage::parse(&data) {
                    Ok(parsed) => {
                        let _ = tx.send(WsEvent::Message(parsed, received_at)).await;
                    }
                    Err(e) => {
                        debug!("Failed to parse binary message: {}", e);
                        let _ = tx.send(WsEvent::ParseError(e.to_string())).await;
                    }
                }
            }
            Message::Ping(_) => {
                debug!("Received ping");
            }
            Message::Pong(_) => {
                debug!("Received pong");
            }
            Message::Close(frame) => {
                info!("Received close frame: {:?}", frame);
            }
            Message::Frame(_) => {
                // Raw frame, typically not used
            }
        }
    }
}

/// Builder for WsClient.
pub struct WsClientBuilder {
    url: String,
    reconnect_config: ReconnectConfig,
    symbols: Vec<String>,
}

impl WsClientBuilder {
    /// Create a new builder with default configuration.
    pub fn new() -> Self {
        let default_config = WebSocketConfig::default();
        Self {
            url: default_config.url.clone(),
            reconnect_config: default_config.to_reconnect_config(),
            symbols: vec!["TEST-USD".to_string()],
        }
    }

    /// Set the WebSocket URL.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// Set the symbols to subscribe to.
    pub fn symbols(mut self, symbols: Vec<String>) -> Self {
        self.symbols = symbols;
        self
    }

    /// Set the reconnect delay.
    pub fn reconnect_delay(mut self, secs: u64) -> Self {
        self.reconnect_config.initial_delay_secs = secs;
        self
    }

    /// Set the maximum reconnect delay.
    pub fn max_reconnect_delay(mut self, secs: u64) -> Self {
        self.reconnect_config.max_delay_secs = secs;
        self
    }

    /// Set the connection timeout.
    pub fn connect_timeout(mut self, secs: u64) -> Self {
        self.reconnect_config.connect_timeout_secs = secs;
        self
    }

    /// Set the stale connection timeout.
    pub fn stale_timeout(mut self, secs: u64) -> Self {
        self.reconnect_config.stale_timeout_secs = secs;
        self
    }

    /// Set the full WebSocket configuration (extracts reconnect config).
    pub fn config(mut self, config: WebSocketConfig) -> Self {
        self.url = config.url.clone();
        self.reconnect_config = config.to_reconnect_config();
        self
    }

    /// Set the reconnection configuration directly.
    pub fn reconnect_config(mut self, config: ReconnectConfig) -> Self {
        self.reconnect_config = config;
        self
    }

    /// Build the WebSocket client.
    pub fn build(self) -> Arc<WsClient> {
        Arc::new(WsClient::with_reconnect_config(self.url, self.reconnect_config, self.symbols))
    }
}

impl Default for WsClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let client = WsClientBuilder::new()
            .url("wss://test.example.com/ws")
            .symbols(vec!["TEST-USD".to_string(), "ETH-USD".to_string()])
            .reconnect_delay(10)
            .build();

        assert!(!client.is_running());
        assert_eq!(client.symbols.len(), 2);
    }

    #[test]
    fn test_stats() {
        let stats = WsStats::default();
        stats.messages_received.fetch_add(10, Ordering::Relaxed);
        stats.bytes_received.fetch_add(1000, Ordering::Relaxed);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.messages_received, 10);
        assert_eq!(snapshot.bytes_received, 1000);
    }
}
