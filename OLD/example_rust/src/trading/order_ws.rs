//! WebSocket client for order operations (low latency).
//!
//! This module provides a WebSocket client for placing and canceling orders
//! with minimal latency. Uses the StandX ws-api/v1 endpoint.
//!
//! Features automatic reconnection with exponential backoff (max 10 retries).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{timeout, sleep, Instant};
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::auth::AuthManager;
use super::client::NewOrderRequest;
use crate::websocket::reconnect::{ReconnectConfig, ReconnectState};

/// Order WebSocket errors.
#[derive(Debug, thiserror::Error)]
pub enum OrderWsError {
    #[error("Not connected")]
    NotConnected,

    #[error("Not authenticated")]
    NotAuthenticated,

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Send error: {0}")]
    SendError(String),

    #[error("Auth error: {0}")]
    AuthError(#[from] super::auth::AuthError),

    #[error("Authentication timeout")]
    AuthTimeout,

    #[error("Authentication rejected: {0}")]
    AuthRejected(String),
}

/// Order events received from WebSocket.
#[derive(Debug, Clone)]
pub enum OrderEvent {
    /// Connected to WebSocket server.
    Connected,
    /// Authenticated on WebSocket.
    Authenticated,
    /// Order accepted by exchange.
    OrderAccepted { cl_ord_id: String, order_id: i64 },
    /// Order rejected by exchange.
    OrderRejected { cl_ord_id: String, reason: String },
    /// Order filled.
    OrderFilled { order_id: i64, fill_qty: String, fill_price: String },
    /// Order canceled.
    OrderCanceled { order_id: i64, cl_ord_id: Option<String> },
    /// Cancel request failed.
    CancelFailed { order_id: i64, reason: String },
    /// WebSocket disconnected (will attempt reconnect).
    Disconnected(String),
    /// Reconnecting after disconnect.
    Reconnecting { attempt: u32, delay_secs: u64 },
    /// Max reconnection attempts exceeded - fatal error.
    MaxRetriesExceeded,
    /// General error.
    Error(String),
}

/// WebSocket message for order API.
#[derive(Debug, Serialize)]
struct WsOrderMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    header: Option<WsHeader>,
    /// Params as JSON string (not nested object) for order methods
    params: WsParams,
}

/// Params wrapper that serializes as a JSON string.
#[derive(Debug)]
struct WsParams(String);

impl Serialize for WsParams {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

#[derive(Debug, Serialize)]
struct WsHeader {
    #[serde(rename = "x-request-id")]
    request_id: String,
    #[serde(rename = "x-request-timestamp")]
    timestamp: String,
    #[serde(rename = "x-request-signature")]
    signature: String,
}

/// Response from WebSocket.
#[derive(Debug, Deserialize)]
struct WsResponse {
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    code: Option<i32>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    data: Option<serde_json::Value>,
    /// Result field used by StandX responses (alternative to data)
    #[serde(default)]
    result: Option<serde_json::Value>,
    /// Request ID to correlate responses
    #[serde(default)]
    request_id: Option<String>,
}

type WsWriter = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsReader = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// WebSocket client for order operations with automatic reconnection.
pub struct OrderWsClient {
    /// WebSocket URL
    url: String,
    /// Auth manager for JWT and signing
    auth: Arc<Mutex<AuthManager>>,
    /// WebSocket write half (protected by mutex for Arc<Self> access)
    writer: Arc<Mutex<Option<WsWriter>>>,
    /// Whether connected
    connected: Arc<AtomicBool>,
    /// Whether authenticated on WebSocket
    ws_authenticated: Arc<AtomicBool>,
    /// Whether the client is running (for connection loop)
    running: Arc<AtomicBool>,
    /// Session ID for this connection
    session_id: String,
    /// Message counter for request IDs
    message_counter: AtomicU64,
    /// Reconnection configuration
    reconnect_config: ReconnectConfig,
    /// Pending cancel cl_ord_ids (to distinguish cancel confirmations from order acceptances)
    pending_cancels: Arc<Mutex<std::collections::HashSet<String>>>,
}

impl OrderWsClient {
    /// Create a new order WebSocket client with default config.
    pub fn new(auth: Arc<Mutex<AuthManager>>) -> Self {
        Self {
            url: "wss://perps.standx.com/ws-api/v1".to_string(),
            auth,
            writer: Arc::new(Mutex::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            ws_authenticated: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicBool::new(false)),
            session_id: format!("session_{}", chrono::Utc::now().timestamp_millis()),
            message_counter: AtomicU64::new(0),
            reconnect_config: ReconnectConfig::for_orders(), // Max 10 retries
            pending_cancels: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Create with custom URL and reconnection config.
    pub fn with_config(
        auth: Arc<Mutex<AuthManager>>,
        url: impl Into<String>,
        reconnect_config: ReconnectConfig,
    ) -> Self {
        Self {
            url: url.into(),
            auth,
            writer: Arc::new(Mutex::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            ws_authenticated: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicBool::new(false)),
            session_id: format!("session_{}", chrono::Utc::now().timestamp_millis()),
            message_counter: AtomicU64::new(0),
            reconnect_config,
            pending_cancels: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Create with custom URL (uses default reconnect config).
    pub fn with_url(auth: Arc<Mutex<AuthManager>>, url: impl Into<String>) -> Self {
        let mut client = Self::new(auth);
        client.url = url.into();
        client
    }

    /// Set custom reconnection configuration.
    pub fn with_reconnect_config(mut self, config: ReconnectConfig) -> Self {
        self.reconnect_config = config;
        self
    }

    /// Check if connected.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Check if WebSocket is authenticated.
    pub fn is_ws_authenticated(&self) -> bool {
        self.ws_authenticated.load(Ordering::Acquire)
    }

    /// Check if the client is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Stop the client (will stop reconnection attempts).
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
        self.ws_authenticated.store(false, Ordering::Release);
    }

    /// Generate a unique request ID.
    fn next_request_id(&self) -> String {
        let count = self.message_counter.fetch_add(1, Ordering::Relaxed);
        format!("req_{}_{}", self.session_id, count)
    }

    /// Run the WebSocket client with automatic reconnection.
    ///
    /// Returns a receiver for order events. Spawns a background task
    /// that handles reconnection with exponential backoff.
    pub async fn run(self: Arc<Self>) -> mpsc::Receiver<OrderEvent> {
        let (tx, rx) = mpsc::channel(1000);
        let client = Arc::clone(&self);

        self.running.store(true, Ordering::Release);

        tokio::spawn(async move {
            client.connection_loop(tx).await;
        });

        rx
    }

    /// Main connection loop with exponential backoff reconnection.
    async fn connection_loop(&self, tx: mpsc::Sender<OrderEvent>) {
        let mut reconnect_state = ReconnectState::new(&self.reconnect_config);

        while self.running.load(Ordering::Acquire) {
            info!("Connecting to order WebSocket: {}", self.url);

            match self.connect_and_run(&tx).await {
                Ok(_) => {
                    info!("Order WebSocket connection closed gracefully");
                    reconnect_state.reset(&self.reconnect_config);
                }
                Err(e) => {
                    error!("Order WebSocket connection error: {}", e);
                    let _ = tx.send(OrderEvent::Error(e.to_string())).await;
                }
            }

            // Mark as disconnected
            self.connected.store(false, Ordering::Release);
            self.ws_authenticated.store(false, Ordering::Release);

            if !self.running.load(Ordering::Acquire) {
                break;
            }

            // Send disconnected event
            let _ = tx.send(OrderEvent::Disconnected("Connection lost".to_string())).await;

            // Calculate next delay (returns None if max retries exceeded)
            match reconnect_state.next_delay(&self.reconnect_config) {
                Some(delay) => {
                    let attempt = reconnect_state.consecutive_failures();
                    warn!(
                        "Order WebSocket reconnecting in {}s (attempt {}/{})",
                        delay,
                        attempt,
                        self.reconnect_config.max_retries.unwrap_or(u32::MAX)
                    );
                    let _ = tx.send(OrderEvent::Reconnecting { attempt, delay_secs: delay }).await;
                    sleep(Duration::from_secs(delay)).await;
                }
                None => {
                    error!(
                        "Order WebSocket max retries ({}) exceeded, giving up",
                        self.reconnect_config.max_retries.unwrap_or(0)
                    );
                    let _ = tx.send(OrderEvent::MaxRetriesExceeded).await;
                    self.running.store(false, Ordering::Release);
                    break;
                }
            }
        }

        info!("Order WebSocket client stopped");
    }

    /// Connect, authenticate, and run the message loop.
    async fn connect_and_run(&self, tx: &mpsc::Sender<OrderEvent>) -> Result<(), OrderWsError> {
        // Connect with timeout
        let connect_timeout = Duration::from_secs(self.reconnect_config.connect_timeout_secs);
        let (ws_stream, _) = timeout(connect_timeout, connect_async(&self.url))
            .await
            .map_err(|_| OrderWsError::ConnectionError("Connection timeout".to_string()))?
            .map_err(|e| OrderWsError::ConnectionError(e.to_string()))?;

        info!("Order WebSocket connected");
        self.connected.store(true, Ordering::Release);

        let (write, mut read) = ws_stream.split();

        // Store writer for use by place_order/cancel_order
        {
            let mut writer_guard = self.writer.lock().await;
            *writer_guard = Some(write);
        }

        // Send connected event
        let _ = tx.send(OrderEvent::Connected).await;

        // Authenticate (pass read half to validate response)
        self.ws_authenticate(&mut read).await?;

        // Send authenticated event
        let _ = tx.send(OrderEvent::Authenticated).await;

        // Message receive loop
        let stale_timeout = Duration::from_secs(self.reconnect_config.stale_timeout_secs);
        let mut last_message = Instant::now();

        loop {
            if !self.running.load(Ordering::Acquire) {
                break;
            }

            // Check for stale connection
            if last_message.elapsed() > stale_timeout {
                warn!("Order WebSocket connection stale (no message for {:?}), reconnecting", stale_timeout);
                break;
            }

            // Read with timeout
            let read_timeout = Duration::from_secs(30);
            match timeout(read_timeout, read.next()).await {
                Ok(Some(Ok(msg))) => {
                    last_message = Instant::now();
                    match msg {
                        Message::Text(text) => {
                            debug!("WS Received: {}", text);
                            if let Ok(response) = serde_json::from_str::<WsResponse>(&text) {
                                Self::handle_response(response, tx, &self.pending_cancels).await;
                            }
                        }
                        Message::Ping(_) => {
                            debug!("Received ping (pong handled automatically)");
                        }
                        Message::Pong(_) => {
                            debug!("Received pong");
                        }
                        Message::Close(frame) => {
                            info!("WebSocket close frame received: {:?}", frame);
                            break;
                        }
                        _ => {}
                    }
                }
                Ok(Some(Err(e))) => {
                    error!("WebSocket read error: {}", e);
                    return Err(OrderWsError::ConnectionError(e.to_string()));
                }
                Ok(None) => {
                    info!("WebSocket stream ended");
                    break;
                }
                Err(_) => {
                    // Timeout, continue loop (stale check handles actual staleness)
                    continue;
                }
            }
        }

        // Clear writer on disconnect
        {
            let mut writer_guard = self.writer.lock().await;
            *writer_guard = None;
        }

        self.connected.store(false, Ordering::Release);
        self.ws_authenticated.store(false, Ordering::Release);

        Ok(())
    }

    /// Authenticate on the WebSocket connection.
    ///
    /// Waits for the actual auth:login response and validates the code.
    /// Returns error if auth is rejected or times out.
    async fn ws_authenticate(&self, read: &mut WsReader) -> Result<(), OrderWsError> {
        // Get JWT token from auth manager
        let token = {
            let mut auth = self.auth.lock().await;
            // Ensure we have a valid token
            if !auth.is_authenticated() {
                auth.authenticate().await?;
            }
            auth.jwt().map(|s| s.to_string())
        };

        let token = token.ok_or(OrderWsError::NotAuthenticated)?;

        // Send auth:login message - params must be JSON string
        let auth_params = serde_json::to_string(&json!({ "token": token }))
            .map_err(|e| OrderWsError::SendError(e.to_string()))?;
        let msg = WsOrderMessage {
            session_id: Some(self.session_id.clone()),
            request_id: Some(self.next_request_id()),
            method: "auth:login".to_string(),
            header: None,
            params: WsParams(auth_params),
        };

        let request_id = msg.request_id.clone();
        self.send_message_internal(&msg).await?;

        // Wait for auth response with timeout (5 seconds)
        // Response format: { "code": 0, "message": "success", "request_id": "..." }
        let auth_timeout = Duration::from_secs(5);
        let start = Instant::now();

        while start.elapsed() < auth_timeout {
            let read_timeout = Duration::from_millis(500);
            match timeout(read_timeout, read.next()).await {
                Ok(Some(Ok(Message::Text(text)))) => {
                    debug!("Auth response: {}", text);
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                        // Check if this is an auth response (has code field and matches our request_id)
                        let resp_request_id = json.get("request_id").and_then(|r| r.as_str());
                        let has_code = json.get("code").is_some();

                        // Match by request_id if present, or accept any response with code during auth
                        if has_code && (resp_request_id == request_id.as_deref() || resp_request_id.is_none()) {
                            let code = json.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
                            if code == 0 {
                                self.ws_authenticated.store(true, Ordering::Release);
                                info!("Order WebSocket authenticated successfully");
                                return Ok(());
                            } else {
                                let msg = json.get("message")
                                    .or_else(|| json.get("msg"))
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("Unknown auth error")
                                    .to_string();
                                error!("Order WebSocket auth rejected: code={}, msg={}", code, msg);
                                return Err(OrderWsError::AuthRejected(msg));
                            }
                        }
                        // Not an auth response, continue waiting
                    }
                }
                Ok(Some(Ok(Message::Ping(_)))) | Ok(Some(Ok(Message::Pong(_)))) => {
                    // Ignore ping/pong during auth
                    continue;
                }
                Ok(Some(Ok(Message::Close(frame)))) => {
                    return Err(OrderWsError::ConnectionError(
                        format!("Connection closed during auth: {:?}", frame)
                    ));
                }
                Ok(Some(Err(e))) => {
                    return Err(OrderWsError::ConnectionError(e.to_string()));
                }
                Ok(None) => {
                    return Err(OrderWsError::ConnectionError("Stream ended during auth".to_string()));
                }
                Err(_) => {
                    // Read timeout, continue loop (overall timeout handles actual timeout)
                    continue;
                }
                _ => continue,
            }
        }

        error!("Order WebSocket auth timeout after {}s", auth_timeout.as_secs());
        Err(OrderWsError::AuthTimeout)
    }

    /// Internal message send (locks writer).
    async fn send_message_internal(&self, msg: &WsOrderMessage) -> Result<(), OrderWsError> {
        let mut writer_guard = self.writer.lock().await;
        let writer = writer_guard.as_mut()
            .ok_or(OrderWsError::NotConnected)?;

        let json = serde_json::to_string(msg)
            .map_err(|e| OrderWsError::SendError(e.to_string()))?;

        debug!("Sending: {}", json);

        writer.send(Message::Text(json)).await
            .map_err(|e| OrderWsError::SendError(e.to_string()))?;

        Ok(())
    }

    /// Place a new order via WebSocket.
    ///
    /// Returns the client order ID.
    pub async fn place_order(&self, mut request: NewOrderRequest) -> Result<String, OrderWsError> {
        if !self.is_connected() {
            return Err(OrderWsError::NotConnected);
        }
        if !self.is_ws_authenticated() {
            return Err(OrderWsError::NotAuthenticated);
        }

        // Generate client order ID if not provided
        let cl_ord_id = request.cl_ord_id.take()
            .unwrap_or_else(|| format!("ord_{}", chrono::Utc::now().timestamp_millis()));

        // Set cl_ord_id back
        request.cl_ord_id = Some(cl_ord_id.clone());

        // Sign the request
        let params_json = serde_json::to_string(&request)
            .map_err(|e| OrderWsError::SendError(e.to_string()))?;

        let (request_id, timestamp_ms, signature) = {
            let auth = self.auth.lock().await;
            auth.sign_request(&params_json)?
        };

        // Build WebSocket message
        let msg = WsOrderMessage {
            session_id: Some(self.session_id.clone()),
            request_id: Some(request_id.clone()),
            method: "order:new".to_string(),
            header: Some(WsHeader {
                request_id,
                timestamp: timestamp_ms.to_string(),
                signature,
            }),
            params: WsParams(params_json),
        };

        self.send_message_internal(&msg).await?;

        Ok(cl_ord_id)
    }

    /// Cancel an order by order ID.
    pub async fn cancel_order(&self, order_id: i64) -> Result<(), OrderWsError> {
        self.send_cancel(json!({ "order_id": order_id })).await
    }

    /// Cancel an order by client order ID.
    pub async fn cancel_order_by_client_id(&self, cl_ord_id: &str) -> Result<(), OrderWsError> {
        // Track this cl_ord_id as pending cancel to distinguish from order acceptance
        {
            let mut pending = self.pending_cancels.lock().await;
            pending.insert(cl_ord_id.to_string());
        }
        self.send_cancel(json!({ "cl_ord_id": cl_ord_id })).await
    }

    /// Get reference to pending cancels set (for response handling).
    pub fn pending_cancels(&self) -> Arc<Mutex<std::collections::HashSet<String>>> {
        Arc::clone(&self.pending_cancels)
    }

    /// Internal helper to send a cancel request.
    async fn send_cancel(&self, params: serde_json::Value) -> Result<(), OrderWsError> {
        if !self.is_connected() {
            return Err(OrderWsError::NotConnected);
        }
        if !self.is_ws_authenticated() {
            return Err(OrderWsError::NotAuthenticated);
        }

        let params_json = serde_json::to_string(&params)
            .map_err(|e| OrderWsError::SendError(e.to_string()))?;

        let (request_id, timestamp_ms, signature) = {
            let auth = self.auth.lock().await;
            auth.sign_request(&params_json)?
        };

        let msg = WsOrderMessage {
            session_id: Some(self.session_id.clone()),
            request_id: Some(request_id.clone()),
            method: "order:cancel".to_string(),
            header: Some(WsHeader {
                request_id,
                timestamp: timestamp_ms.to_string(),
                signature,
            }),
            params: WsParams(params_json),
        };

        self.send_message_internal(&msg).await
    }

    /// Disconnect the WebSocket gracefully.
    pub async fn disconnect(&self) {
        self.running.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
        self.ws_authenticated.store(false, Ordering::Release);

        let mut writer_guard = self.writer.lock().await;
        if let Some(mut writer) = writer_guard.take() {
            let _ = writer.close().await;
        }

        info!("Order WebSocket disconnected");
    }

    // ========== Field Extraction Helpers ==========
    // These handle multiple field name variants used by StandX API.

    /// Extract client order ID from JSON data.
    /// Handles: cl_ord_id, clOrdId, clientOrderId, client_order_id
    #[inline]
    fn extract_cl_ord_id(data: &serde_json::Value) -> Option<String> {
        data.get("cl_ord_id")
            .or_else(|| data.get("clOrdId"))
            .or_else(|| data.get("clientOrderId"))
            .or_else(|| data.get("client_order_id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Extract exchange order ID from JSON data.
    /// Handles: id, order_id
    #[inline]
    fn extract_order_id(data: &serde_json::Value) -> Option<i64> {
        data.get("id")
            .or_else(|| data.get("order_id"))
            .and_then(|v| v.as_i64())
    }

    /// Extract fill quantity from JSON data.
    /// Handles: fill_qty, fillQty
    #[inline]
    fn extract_fill_qty(data: &serde_json::Value) -> String {
        data.get("fill_qty")
            .or_else(|| data.get("fillQty"))
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string()
    }

    /// Extract fill price from JSON data.
    /// Handles: fill_price, fillPrice
    #[inline]
    fn extract_fill_price(data: &serde_json::Value) -> String {
        data.get("fill_price")
            .or_else(|| data.get("fillPrice"))
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string()
    }

    /// Check if data has a client order ID field.
    #[inline]
    fn has_cl_ord_id(data: &serde_json::Value) -> bool {
        data.get("cl_ord_id").is_some()
            || data.get("clOrdId").is_some()
            || data.get("clientOrderId").is_some()
            || data.get("client_order_id").is_some()
    }

    // ========== Response Handlers ==========

    /// Handle a response from the server.
    #[inline]
    async fn handle_response(
        response: WsResponse,
        event_tx: &mpsc::Sender<OrderEvent>,
        pending_cancels: &Arc<Mutex<std::collections::HashSet<String>>>,
    ) {
        let method = response.method.as_deref().unwrap_or("");
        let code = response.code.unwrap_or(0);

        // Use result field if data is not present (StandX API uses "result")
        let data = response.data.or(response.result);

        // Handle responses with explicit method field
        match method {
            "order:new" => {
                debug!("Routing: order:new response (code={})", code);
                if let Some(data) = data {
                    Self::handle_order_response(data, code, &response.message, event_tx).await;
                }
                return;
            }
            "order:cancel" => {
                debug!("Routing: order:cancel response (code={})", code);
                if let Some(data) = data {
                    // Remove from pending cancels
                    if let Some(cl_ord_id) = Self::extract_cl_ord_id(&data) {
                        let mut pending = pending_cancels.lock().await;
                        pending.remove(&cl_ord_id);
                    }
                    Self::handle_cancel_response(data, code, &response.message, event_tx).await;
                }
                return;
            }
            "order:fill" | "order:filled" => {
                debug!("Routing: order:fill response");
                if let Some(data) = data {
                    let order_id = Self::extract_order_id(&data).unwrap_or(0);
                    let fill_qty = Self::extract_fill_qty(&data);
                    let fill_price = Self::extract_fill_price(&data);
                    let _ = event_tx.send(OrderEvent::OrderFilled { order_id, fill_qty, fill_price }).await;
                }
                return;
            }
            _ => {}
        }

        // Handle responses WITHOUT method field (StandX API style)
        if let Some(data) = data {
            let cl_ord_id = Self::extract_cl_ord_id(&data);

            // Check if this cl_ord_id is in our pending cancels list
            let is_pending_cancel = if let Some(ref id) = cl_ord_id {
                let pending = pending_cancels.lock().await;
                pending.contains(id)
            } else {
                false
            };

            if is_pending_cancel {
                // This is a cancel confirmation - remove from pending and fire event
                if let Some(ref id) = cl_ord_id {
                    let mut pending = pending_cancels.lock().await;
                    pending.remove(id);
                }
                debug!("Routing: cancel confirmation from pending_cancels (code={})", code);
                Self::handle_cancel_response(data, code, &response.message, event_tx).await;
                return;
            }

            // Not a pending cancel - must be an order acceptance
            if cl_ord_id.is_some() {
                debug!("Routing: order acceptance (not in pending_cancels) (code={})", code);
                Self::handle_order_response(data, code, &response.message, event_tx).await;
                return;
            }

            // Fallback: has order_id without cl_ord_id → cancel response
            let order_id = Self::extract_order_id(&data);
            if order_id.is_some() {
                debug!("Routing: inferred cancel response from order_id only (code={})", code);
                Self::handle_cancel_response(data, code, &response.message, event_tx).await;
                return;
            }
        }

        // Handle errors
        if code != 0 {
            debug!("Routing: error response (code={})", code);
            let msg = response.message.unwrap_or_else(|| "Unknown error".to_string());
            let _ = event_tx.send(OrderEvent::Error(msg)).await;
        }
    }

    /// Handle order new/accepted response.
    #[inline]
    async fn handle_order_response(
        data: serde_json::Value,
        code: i32,
        message: &Option<String>,
        event_tx: &mpsc::Sender<OrderEvent>,
    ) {
        let cl_ord_id = Self::extract_cl_ord_id(&data).unwrap_or_default();
        let order_id = Self::extract_order_id(&data).unwrap_or(0);

        if code == 0 {
            info!("Order accepted: cl_ord_id={}, order_id={}", cl_ord_id, order_id);
            let _ = event_tx.send(OrderEvent::OrderAccepted { cl_ord_id, order_id }).await;
        } else {
            let reason = message.clone().unwrap_or_default();
            warn!("Order rejected: cl_ord_id={}, reason={}", cl_ord_id, reason);
            let _ = event_tx.send(OrderEvent::OrderRejected { cl_ord_id, reason }).await;
        }
    }

    /// Handle order cancel response.
    #[inline]
    async fn handle_cancel_response(
        data: serde_json::Value,
        code: i32,
        message: &Option<String>,
        event_tx: &mpsc::Sender<OrderEvent>,
    ) {
        let order_id = Self::extract_order_id(&data).unwrap_or(0);
        let cl_ord_id = Self::extract_cl_ord_id(&data);

        if code == 0 {
            let _ = event_tx.send(OrderEvent::OrderCanceled { order_id, cl_ord_id }).await;
        } else {
            let reason = message.clone().unwrap_or_else(|| "Cancel failed".to_string());
            warn!("Cancel failed for order {}: {}", order_id, reason);
            let _ = event_tx.send(OrderEvent::CancelFailed { order_id, reason }).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_generation() {
        let auth = Arc::new(Mutex::new(AuthManager::new()));
        let client = OrderWsClient::new(auth);

        let id1 = client.next_request_id();
        let id2 = client.next_request_id();

        assert_ne!(id1, id2);
        assert!(id1.starts_with("req_session_"));
    }

    #[test]
    fn test_reconnect_config() {
        let auth = Arc::new(Mutex::new(AuthManager::new()));
        let client = OrderWsClient::new(auth);

        // Default config should have max 10 retries
        assert_eq!(client.reconnect_config.max_retries, Some(10));
    }
}
