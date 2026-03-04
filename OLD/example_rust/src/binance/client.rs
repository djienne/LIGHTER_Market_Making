//! Binance WebSocket client with auto-reconnect.
//!
//! Streams orderbook depth updates from Binance and maintains a synchronized
//! local orderbook state using the REST snapshot + WebSocket delta approach.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::types::OrderbookSnapshot;
use crate::websocket::reconnect::{ReconnectConfig, ReconnectState};

use super::messages::{parse_depth_update, parse_snapshot};
use super::orderbook::{BinanceOrderbook, SyncError};

/// WebSocket endpoint for Binance Spot depth stream.
pub const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws";

/// REST API endpoint for Binance Spot depth snapshot.
pub const BINANCE_REST_URL: &str = "https://api.binance.com/api/v3/depth";

/// WebSocket endpoint for Binance Futures depth stream.
pub const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/ws";

/// REST API endpoint for Binance Futures depth snapshot.
pub const BINANCE_FUTURES_REST_URL: &str = "https://fapi.binance.com/fapi/v1/depth";

/// Binance market type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MarketType {
    /// Spot market (api.binance.com)
    Spot,
    /// USD-M Perpetual Futures (fapi.binance.com) - DEFAULT
    #[default]
    Futures,
}

/// Binance 24-hour connection limit (proactively reconnect before this).
const MAX_CONNECTION_DURATION: Duration = Duration::from_secs(23 * 60 * 60); // 23 hours

/// Events emitted by the Binance client.
#[derive(Debug, Clone)]
pub enum BinanceEvent {
    /// New orderbook snapshot available
    Snapshot(OrderbookSnapshot),

    /// Successfully synced with WebSocket stream
    Synced,

    /// Disconnected from WebSocket
    Disconnected(String),

    /// Error occurred
    Error(String),
}

/// Statistics for the Binance WebSocket connection.
#[derive(Debug, Default)]
pub struct BinanceWsStats {
    /// Total messages received
    pub messages_received: AtomicU64,

    /// Total reconnection attempts
    pub reconnects: AtomicU64,

    /// Total bytes received
    pub bytes_received: AtomicU64,

    /// Total snapshots fetched from REST
    pub snapshots_fetched: AtomicU64,

    /// Sequence gaps detected
    pub sequence_gaps: AtomicU64,
}

impl BinanceWsStats {
    /// Create a new stats instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a snapshot of current stats.
    pub fn snapshot(&self) -> BinanceWsStatsSnapshot {
        BinanceWsStatsSnapshot {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            snapshots_fetched: self.snapshots_fetched.load(Ordering::Relaxed),
            sequence_gaps: self.sequence_gaps.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of Binance WebSocket stats.
#[derive(Debug, Clone)]
pub struct BinanceWsStatsSnapshot {
    pub messages_received: u64,
    pub reconnects: u64,
    pub bytes_received: u64,
    pub snapshots_fetched: u64,
    pub sequence_gaps: u64,
}

/// Configuration for the Binance client.
#[derive(Debug, Clone)]
pub struct BinanceClientConfig {
    /// Symbol to subscribe (e.g., "btcusdt")
    pub symbol: String,

    /// Depth stream update interval (e.g., "100ms", "1000ms")
    pub update_interval: String,

    /// Number of levels to fetch in REST snapshot
    pub snapshot_depth: u32,

    /// Market type (Spot or Futures)
    pub market_type: MarketType,

    /// WebSocket URL (default: fstream.binance.com for Futures)
    pub ws_url: String,

    /// REST API URL (default: fapi.binance.com for Futures)
    pub rest_url: String,

    /// Reconnection configuration
    pub reconnect: ReconnectConfig,
}

impl Default for BinanceClientConfig {
    fn default() -> Self {
        Self {
            symbol: "btcusdt".to_string(),
            update_interval: "100ms".to_string(),
            snapshot_depth: 1000,
            market_type: MarketType::Futures,
            ws_url: BINANCE_FUTURES_WS_URL.to_string(),
            rest_url: BINANCE_FUTURES_REST_URL.to_string(),
            reconnect: ReconnectConfig::for_orderbook(),
        }
    }
}

impl BinanceClientConfig {
    /// Create a config for BTCUSDT with 100ms updates (Futures by default).
    pub fn btcusdt() -> Self {
        Self::default()
    }

    /// Create config for Futures market (default).
    pub fn futures(symbol: &str) -> Self {
        Self::default().with_symbol(symbol)
    }

    /// Create config for Spot market.
    pub fn spot(symbol: &str) -> Self {
        Self {
            market_type: MarketType::Spot,
            ws_url: BINANCE_WS_URL.to_string(),
            rest_url: BINANCE_REST_URL.to_string(),
            ..Self::default()
        }
        .with_symbol(symbol)
    }

    /// Set the symbol.
    pub fn with_symbol(mut self, symbol: &str) -> Self {
        self.symbol = symbol.to_lowercase();
        self
    }

    /// Set the market type.
    pub fn with_market_type(mut self, market_type: MarketType) -> Self {
        self.market_type = market_type;
        match market_type {
            MarketType::Spot => {
                self.ws_url = BINANCE_WS_URL.to_string();
                self.rest_url = BINANCE_REST_URL.to_string();
            }
            MarketType::Futures => {
                self.ws_url = BINANCE_FUTURES_WS_URL.to_string();
                self.rest_url = BINANCE_FUTURES_REST_URL.to_string();
            }
        }
        self
    }

    /// Set the update interval.
    pub fn with_update_interval(mut self, interval: &str) -> Self {
        self.update_interval = interval.to_string();
        self
    }

    /// Set the snapshot depth.
    pub fn with_snapshot_depth(mut self, depth: u32) -> Self {
        self.snapshot_depth = depth;
        self
    }

    /// Build the WebSocket stream URL.
    pub fn ws_stream_url(&self) -> String {
        format!(
            "{}/{}@depth@{}",
            self.ws_url, self.symbol, self.update_interval
        )
    }

    /// Build the REST snapshot URL.
    pub fn rest_snapshot_url(&self) -> String {
        format!(
            "{}?symbol={}&limit={}",
            self.rest_url,
            self.symbol.to_uppercase(),
            self.snapshot_depth
        )
    }
}

/// Binance WebSocket client with auto-reconnect.
pub struct BinanceClient {
    config: BinanceClientConfig,
    running: AtomicBool,
    needs_resync: AtomicBool,
    stats: BinanceWsStats,
    http_client: reqwest::Client,
}

impl BinanceClient {
    /// Create a new Binance client with default configuration.
    pub fn new(symbol: &str) -> Self {
        Self::with_config(BinanceClientConfig::default().with_symbol(symbol))
    }

    /// Create a new Binance client with custom configuration.
    pub fn with_config(config: BinanceClientConfig) -> Self {
        Self {
            config,
            running: AtomicBool::new(false),
            needs_resync: AtomicBool::new(false),
            stats: BinanceWsStats::new(),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    /// Start the client and return a receiver for events.
    ///
    /// The client runs in a background task and emits events for:
    /// - New orderbook snapshots
    /// - Sync status changes
    /// - Errors and disconnections
    pub async fn run(self: Arc<Self>) -> mpsc::Receiver<BinanceEvent> {
        let (tx, rx) = mpsc::channel(256);

        self.running.store(true, Ordering::Release);

        let client = self.clone();
        tokio::spawn(async move {
            client.connection_loop(tx).await;
        });

        rx
    }

    /// Stop the client gracefully.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Check if the client is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Get statistics.
    pub fn stats(&self) -> &BinanceWsStats {
        &self.stats
    }

    /// Fetch a depth snapshot from the REST API.
    pub async fn fetch_snapshot(
        &self,
    ) -> Result<super::messages::BinanceDepthSnapshot, Box<dyn std::error::Error + Send + Sync>>
    {
        let url = self.config.rest_snapshot_url();
        debug!(url = %url, "Fetching depth snapshot");

        let response = self.http_client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(format!("HTTP error: {}", response.status()).into());
        }

        let text = response.text().await?;
        let snapshot = parse_snapshot(&text)?;

        self.stats.snapshots_fetched.fetch_add(1, Ordering::Relaxed);
        info!(
            last_update_id = snapshot.last_update_id,
            bids = snapshot.bids.len(),
            asks = snapshot.asks.len(),
            "Fetched depth snapshot"
        );

        Ok(snapshot)
    }

    /// Main connection loop with auto-reconnect.
    async fn connection_loop(&self, tx: mpsc::Sender<BinanceEvent>) {
        let mut reconnect_state = ReconnectState::new(&self.config.reconnect);

        while self.running.load(Ordering::Acquire) {
            let connection_start = Instant::now();

            match self.connect_and_stream(&tx, connection_start).await {
                Ok(_) => {
                    // Clean exit (stop() called or 24h limit)
                    reconnect_state.reset(&self.config.reconnect);
                }
                Err(e) => {
                    error!(error = %e, "Connection error");
                    let _ = tx.send(BinanceEvent::Error(e.to_string())).await;
                }
            }

            // Check if we need immediate resync (sequence gap) vs normal reconnect
            if self.needs_resync.swap(false, Ordering::AcqRel) {
                info!("Immediate resync due to sequence gap");
                continue;
            }

            if !self.running.load(Ordering::Acquire) {
                break;
            }

            // Apply reconnection backoff
            if let Some(delay) = reconnect_state.next_delay(&self.config.reconnect) {
                let _ = tx
                    .send(BinanceEvent::Disconnected(format!(
                        "Reconnecting in {}s",
                        delay
                    )))
                    .await;
                self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
                sleep(Duration::from_secs(delay)).await;
            } else {
                error!("Max reconnection attempts exceeded");
                let _ = tx
                    .send(BinanceEvent::Error(
                        "Max reconnection attempts exceeded".to_string(),
                    ))
                    .await;
                break;
            }
        }

        info!("Binance client stopped");
    }

    /// Connect to WebSocket, fetch snapshot, and stream updates.
    async fn connect_and_stream(
        &self,
        tx: &mpsc::Sender<BinanceEvent>,
        connection_start: Instant,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ws_url = self.config.ws_stream_url();
        info!(url = %ws_url, "Connecting to Binance WebSocket");

        // Connect with timeout
        let connect_timeout = Duration::from_secs(self.config.reconnect.connect_timeout_secs);
        let (ws_stream, _response) = timeout(connect_timeout, connect_async(&ws_url)).await??;

        let (mut write, mut read) = ws_stream.split();

        info!("WebSocket connected, fetching snapshot");

        // Create local orderbook state
        let mut orderbook = BinanceOrderbook::new(&self.config.symbol.to_uppercase());

        // Fetch REST snapshot
        let snapshot = self.fetch_snapshot().await?;
        orderbook.init_from_snapshot(snapshot)?;

        // Apply any buffered updates that arrived during snapshot fetch
        // (In practice, the snapshot fetch is fast enough that this is rarely needed)

        info!(
            snapshot_id = orderbook.snapshot_update_id(),
            "Initialized orderbook from snapshot"
        );

        let stale_timeout = Duration::from_secs(self.config.reconnect.stale_timeout_secs);
        let mut last_message_time = Instant::now();

        // Main message loop
        while self.running.load(Ordering::Acquire) {
            // Check for 24-hour connection limit (proactive reconnect)
            if connection_start.elapsed() > MAX_CONNECTION_DURATION {
                info!("Approaching 24-hour connection limit, proactively reconnecting");
                break;
            }

            // Check for stale connection
            if last_message_time.elapsed() > stale_timeout {
                warn!(
                    elapsed_secs = last_message_time.elapsed().as_secs(),
                    "Connection stale, reconnecting"
                );
                break;
            }

            // Read next message with timeout (to check stale/running periodically)
            let read_timeout = Duration::from_secs(5);
            let msg_result = timeout(read_timeout, read.next()).await;

            match msg_result {
                Ok(Some(Ok(msg))) => {
                    last_message_time = Instant::now();

                    // Track bytes
                    let bytes = match &msg {
                        Message::Text(t) => t.len() as u64,
                        Message::Binary(b) => b.len() as u64,
                        _ => 0,
                    };
                    self.stats.bytes_received.fetch_add(bytes, Ordering::Relaxed);

                    // Handle pong responses (ping is automatic)
                    if let Message::Ping(data) = &msg {
                        let _ = write.send(Message::Pong(data.clone())).await;
                        continue;
                    }

                    // Parse depth update
                    let update = match parse_depth_update(&msg)? {
                        Some(u) => u,
                        None => continue, // Ping/Pong/Close handled
                    };

                    self.stats.messages_received.fetch_add(1, Ordering::Relaxed);

                    // Apply update to local orderbook
                    match orderbook.apply_update(&update) {
                        Ok(true) => {
                            // Update applied successfully
                            if orderbook.is_synced() {
                                let snapshot = orderbook.to_snapshot();
                                let _ = tx.send(BinanceEvent::Snapshot(snapshot)).await;
                            }
                        }
                        Ok(false) => {
                            // Stale update, skipped
                            debug!(
                                update_id = update.final_update_id,
                                "Skipped stale update"
                            );
                        }
                        Err(SyncError::SequenceGap { expected, got }) => {
                            warn!(
                                expected = expected,
                                got = got,
                                "Sequence gap detected, triggering resync"
                            );
                            self.stats.sequence_gaps.fetch_add(1, Ordering::Relaxed);
                            self.needs_resync.store(true, Ordering::Release);
                            break;
                        }
                        Err(SyncError::SequenceBreak { expected, got }) => {
                            warn!(
                                expected = expected,
                                got = got,
                                "Sequence break detected, triggering resync"
                            );
                            self.stats.sequence_gaps.fetch_add(1, Ordering::Relaxed);
                            self.needs_resync.store(true, Ordering::Release);
                            break;
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to apply update");
                            break;
                        }
                    }

                }
                Ok(Some(Err(e))) => {
                    error!(error = %e, "WebSocket error");
                    return Err(e.into());
                }
                Ok(None) => {
                    info!("WebSocket stream closed");
                    break;
                }
                Err(_) => {
                    // Timeout - continue loop to check stale/running flags
                    continue;
                }
            }
        }

        // Graceful close
        let _ = write.send(Message::Close(None)).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_futures() {
        let config = BinanceClientConfig::default();
        assert_eq!(config.market_type, MarketType::Futures);
    }

    #[test]
    fn test_futures_config_urls() {
        let config = BinanceClientConfig::futures("btcusdt");
        assert!(config.ws_stream_url().contains("fstream.binance.com"));
        assert!(config.rest_snapshot_url().contains("fapi.binance.com"));
        assert_eq!(config.market_type, MarketType::Futures);
    }

    #[test]
    fn test_spot_config_urls() {
        let config = BinanceClientConfig::spot("btcusdt");
        assert!(config.ws_stream_url().contains("stream.binance.com"));
        assert!(config.rest_snapshot_url().contains("api.binance.com"));
        assert_eq!(config.market_type, MarketType::Spot);
    }

    #[test]
    fn test_config_urls() {
        // Default is now Futures
        let config = BinanceClientConfig::btcusdt();

        assert_eq!(
            config.ws_stream_url(),
            "wss://fstream.binance.com/ws/btcusdt@depth@100ms"
        );
        assert_eq!(
            config.rest_snapshot_url(),
            "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"
        );
    }

    #[test]
    fn test_config_custom_symbol() {
        let config = BinanceClientConfig::default()
            .with_symbol("ETHUSDT")
            .with_update_interval("1000ms")
            .with_snapshot_depth(500);

        assert!(config.ws_stream_url().contains("ethusdt@depth@1000ms"));
        assert!(config.rest_snapshot_url().contains("symbol=ETHUSDT"));
        assert!(config.rest_snapshot_url().contains("limit=500"));
    }

    #[test]
    fn test_with_market_type() {
        // Start with default (Futures), switch to Spot
        let config = BinanceClientConfig::default().with_market_type(MarketType::Spot);
        assert_eq!(config.market_type, MarketType::Spot);
        assert!(config.ws_stream_url().contains("stream.binance.com"));

        // Start with Spot, switch to Futures
        let config = BinanceClientConfig::spot("btcusdt").with_market_type(MarketType::Futures);
        assert_eq!(config.market_type, MarketType::Futures);
        assert!(config.ws_stream_url().contains("fstream.binance.com"));
    }

    #[test]
    fn test_stats_default() {
        let stats = BinanceWsStats::new();
        let snapshot = stats.snapshot();

        assert_eq!(snapshot.messages_received, 0);
        assert_eq!(snapshot.reconnects, 0);
        assert_eq!(snapshot.bytes_received, 0);
    }
}
