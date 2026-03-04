//! HTTP client for StandX REST API.

use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// HTTP client errors.
#[derive(Error, Debug)]
pub enum ClientError {
    #[error("HTTP request failed: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("API error: {0}")]
    ApiError(String),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),
}

/// Response from prepare-signin endpoint.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrepareSigninResponse {
    pub signed_data: String,
}

/// Response from login endpoint.
#[derive(Debug, Deserialize)]
pub struct LoginResponse {
    pub token: String,
    pub address: String,
    pub chain: String,
}

/// Position from query_positions endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct Position {
    pub id: i64,
    pub symbol: String,
    pub status: String,
    pub qty: String,
    #[serde(alias = "entryPrice", alias = "entry_price")]
    pub entry_price: Option<String>,
    #[serde(alias = "markPrice", alias = "mark_price")]
    pub mark_price: Option<String>,
    pub leverage: Option<String>,
    #[serde(alias = "marginMode", alias = "margin_mode")]
    pub margin_mode: Option<String>,
    #[serde(alias = "realizedPnl", alias = "realized_pnl")]
    pub realized_pnl: Option<String>,
    pub upnl: Option<String>,
}

/// Balance from query_balance endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct Balance {
    #[serde(alias = "crossBalance", alias = "cross_balance")]
    pub cross_balance: Option<String>,
    #[serde(alias = "crossAvailable", alias = "cross_available")]
    pub cross_available: Option<String>,
    pub equity: Option<String>,
    pub upnl: Option<String>,
    // Alternative snake_case fields
    #[serde(alias = "isolatedBalance", alias = "isolated_balance")]
    pub isolated_balance: Option<String>,
}

/// Depth book from query_depth_book endpoint (public, no auth).
#[derive(Debug, Clone, Deserialize)]
pub struct DepthBookResponse {
    pub symbol: String,
    /// Asks (sell orders): [[price, qty], ...]
    pub asks: Vec<[String; 2]>,
    /// Bids (buy orders): [[price, qty], ...]
    pub bids: Vec<[String; 2]>,
}

/// Symbol info from query_symbol_info endpoint (public, no auth).
#[derive(Debug, Clone, Deserialize)]
pub struct SymbolInfo {
    pub symbol: String,
    /// Number of decimal places for price (e.g., 2 means tick_size = 0.01)
    pub price_tick_decimals: u8,
    /// Number of decimal places for quantity (e.g., 3 means lot_size = 0.001)
    pub qty_tick_decimals: u8,
}

/// Position config from query_position_config endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct PositionConfig {
    pub symbol: String,
    /// Leverage as string from API (e.g., "1", "10")
    #[serde(deserialize_with = "deserialize_string_to_i32")]
    pub leverage: i32,
    pub margin_mode: String,
}

/// Deserialize a string like "1" or "10" to i32.
fn deserialize_string_to_i32<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    s.parse::<i32>().map_err(|e| D::Error::custom(format!("invalid leverage: {}", e)))
}

/// Request for changing leverage.
#[derive(Debug, Clone, Serialize)]
pub struct ChangeLeverageRequest {
    pub symbol: String,
    pub leverage: i32,
}

impl SymbolInfo {
    /// Convert price_tick_decimals to tick_size (e.g., 2 -> 0.01)
    #[inline]
    pub fn tick_size(&self) -> f64 {
        10.0_f64.powi(-(self.price_tick_decimals as i32))
    }

    /// Convert qty_tick_decimals to lot_size (e.g., 3 -> 0.001)
    #[inline]
    pub fn lot_size(&self) -> f64 {
        10.0_f64.powi(-(self.qty_tick_decimals as i32))
    }
}

/// Request for placing a new order.
#[derive(Debug, Clone, Serialize)]
pub struct NewOrderRequest {
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub qty: String,
    pub price: String,
    pub time_in_force: String,
    pub reduce_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cl_ord_id: Option<String>,
}

/// Calculate decimal precision from step size (e.g., 0.01 -> 2, 0.001 -> 3).
#[inline]
fn precision_from_step(step: f64) -> usize {
    if step >= 1.0 {
        0
    } else {
        (-step.log10().floor()) as usize
    }
}

/// Format a number with dynamic precision based on step size.
#[inline]
fn format_with_precision(value: f64, step: f64) -> String {
    let precision = precision_from_step(step);
    format!("{:.prec$}", value, prec = precision)
}

impl NewOrderRequest {
    /// Create a new limit order request with explicit precision.
    pub fn limit_with_precision(
        symbol: impl Into<String>,
        side: &str,
        price: f64,
        qty: f64,
        tick_size: f64,
        lot_size: f64,
        cl_ord_id: Option<String>,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            side: side.to_string(),
            order_type: "limit".to_string(),
            qty: format_with_precision(qty, lot_size),
            price: format_with_precision(price, tick_size),
            time_in_force: "gtc".to_string(),
            reduce_only: false,
            cl_ord_id,
        }
    }

    /// Create a new limit order request (uses default 2 decimal price, 4 decimal qty).
    pub fn limit(
        symbol: impl Into<String>,
        side: &str,
        price: f64,
        qty: f64,
        cl_ord_id: Option<String>,
    ) -> Self {
        Self::limit_with_precision(symbol, side, price, qty, 0.01, 0.0001, cl_ord_id)
    }

    /// Create a limit buy order (GTC by default).
    pub fn limit_buy(symbol: impl Into<String>, price: f64, qty: f64) -> Self {
        Self::limit(symbol, "buy", price, qty, None)
    }

    /// Create a limit sell order (GTC by default).
    pub fn limit_sell(symbol: impl Into<String>, price: f64, qty: f64) -> Self {
        Self::limit(symbol, "sell", price, qty, None)
    }

    /// Create a post-only (ALO) limit buy order with explicit precision.
    pub fn post_only_buy_with_precision(
        symbol: impl Into<String>,
        price: f64,
        qty: f64,
        tick_size: f64,
        lot_size: f64,
    ) -> Self {
        Self::limit_with_precision(symbol, "buy", price, qty, tick_size, lot_size, None).post_only()
    }

    /// Create a post-only (ALO) limit sell order with explicit precision.
    pub fn post_only_sell_with_precision(
        symbol: impl Into<String>,
        price: f64,
        qty: f64,
        tick_size: f64,
        lot_size: f64,
    ) -> Self {
        Self::limit_with_precision(symbol, "sell", price, qty, tick_size, lot_size, None).post_only()
    }

    /// Create a post-only (ALO) limit buy order (default precision).
    ///
    /// Post-only orders are rejected if they would immediately match.
    /// This ensures maker-only execution (0.01% fee vs 0.04% taker).
    pub fn post_only_buy(symbol: impl Into<String>, price: f64, qty: f64) -> Self {
        Self::limit(symbol, "buy", price, qty, None).post_only()
    }

    /// Create a post-only (ALO) limit sell order (default precision).
    ///
    /// Post-only orders are rejected if they would immediately match.
    /// This ensures maker-only execution (0.01% fee vs 0.04% taker).
    pub fn post_only_sell(symbol: impl Into<String>, price: f64, qty: f64) -> Self {
        Self::limit(symbol, "sell", price, qty, None).post_only()
    }

    /// Set client order ID.
    pub fn with_client_id(mut self, cl_ord_id: impl Into<String>) -> Self {
        self.cl_ord_id = Some(cl_ord_id.into());
        self
    }

    /// Set time in force.
    pub fn with_tif(mut self, tif: &str) -> Self {
        self.time_in_force = tif.to_string();
        self
    }

    /// Set post-only (ALO - Add Liquidity Only).
    ///
    /// Post-only orders are rejected if they would immediately match.
    /// This ensures maker-only execution (0.01% fee vs 0.04% taker).
    pub fn post_only(mut self) -> Self {
        self.time_in_force = "alo".to_string();
        self
    }

    /// Set reduce only flag.
    pub fn reduce_only(mut self) -> Self {
        self.reduce_only = true;
        self
    }
}

/// Response from order endpoints.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderResponse {
    pub code: i32,
    pub message: String,
    #[serde(default)]
    pub request_id: Option<String>,
}

impl OrderResponse {
    /// Check if the response indicates success.
    pub fn is_success(&self) -> bool {
        self.code == 0
    }
}

/// Open order from query_open_orders endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct OpenOrder {
    pub id: i64,
    #[serde(alias = "clOrdId", alias = "cl_ord_id")]
    pub cl_ord_id: Option<String>,
    pub symbol: String,
    pub side: String,
    #[serde(alias = "orderType", alias = "order_type")]
    pub order_type: String,
    pub price: String,
    pub qty: String,
    #[serde(alias = "fillQty", alias = "fill_qty")]
    pub fill_qty: Option<String>,
    pub status: String,
    pub leverage: Option<String>,
    #[serde(alias = "timeInForce", alias = "time_in_force")]
    pub time_in_force: Option<String>,
    #[serde(alias = "reduceOnly", alias = "reduce_only")]
    pub reduce_only: Option<bool>,
}

/// Request for canceling multiple orders.
#[derive(Debug, Clone, Serialize)]
pub struct CancelOrdersRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id_list: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cl_ord_id_list: Option<Vec<String>>,
}

/// Paginated response wrapper from API.
#[derive(Debug, Clone, Deserialize)]
struct PaginatedResponse<T> {
    #[allow(dead_code)]
    code: i32,
    #[allow(dead_code)]
    message: String,
    #[allow(dead_code)]
    page_size: Option<i32>,
    result: Vec<T>,
}

/// Request body for prepare-signin.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PrepareSigninRequest {
    address: String,
    request_id: String,
}

/// Request body for login.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginRequest {
    signature: String,
    signed_data: String,
    expires_seconds: u64,
}

/// HTTP client for StandX API.
pub struct StandXClient {
    http: Client,
    auth_base_url: String,
    perps_base_url: String,
}

impl StandXClient {
    /// Build the HTTP client with appropriate timeouts.
    fn build_http_client() -> Client {
        Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client")
    }

    /// Create a new StandX client with default URLs.
    pub fn new() -> Self {
        Self {
            http: Self::build_http_client(),
            auth_base_url: "https://api.standx.com".to_string(),
            perps_base_url: "https://perps.standx.com".to_string(),
        }
    }

    /// Create a new StandX client with custom URLs.
    pub fn with_urls(auth_base_url: impl Into<String>, perps_base_url: impl Into<String>) -> Self {
        Self {
            http: Self::build_http_client(),
            auth_base_url: auth_base_url.into(),
            perps_base_url: perps_base_url.into(),
        }
    }

    /// Request prepare-signin data.
    pub async fn prepare_signin(
        &self,
        address: &str,
        request_id: &str,
        chain: &str,
    ) -> Result<PrepareSigninResponse, ClientError> {
        let url = format!("{}/v1/offchain/prepare-signin?chain={}", self.auth_base_url, chain);

        let body = PrepareSigninRequest {
            address: address.to_string(),
            request_id: request_id.to_string(),
        };

        let response = self.http
            .post(&url)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        response.json().await.map_err(|e| ClientError::InvalidResponse(e.to_string()))
    }

    /// Submit login with signature.
    pub async fn login(
        &self,
        signature: &str,
        signed_data: &str,
        chain: &str,
        expires_seconds: u64,
    ) -> Result<LoginResponse, ClientError> {
        let url = format!("{}/v1/offchain/login?chain={}", self.auth_base_url, chain);

        let body = LoginRequest {
            signature: signature.to_string(),
            signed_data: signed_data.to_string(),
            expires_seconds,
        };

        let response = self.http
            .post(&url)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        response.json().await.map_err(|e| ClientError::InvalidResponse(e.to_string()))
    }

    /// Query positions.
    pub async fn query_positions(
        &self,
        token: &str,
        symbol: Option<&str>,
    ) -> Result<Vec<Position>, ClientError> {
        let mut url = format!("{}/api/query_positions", self.perps_base_url);
        if let Some(sym) = symbol {
            url.push_str(&format!("?symbol={}", sym));
        }

        let response = self.http
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        let text = response.text().await.map_err(|e| ClientError::InvalidResponse(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| {
            // Include raw response for debugging
            ClientError::InvalidResponse(format!("{}\nRaw response: {}", e, &text[..text.len().min(500)]))
        })
    }

    /// Query account balance.
    pub async fn query_balance(&self, token: &str) -> Result<Balance, ClientError> {
        let url = format!("{}/api/query_balance", self.perps_base_url);

        let response = self.http
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        let text = response.text().await.map_err(|e| ClientError::InvalidResponse(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| {
            // Include raw response for debugging
            ClientError::InvalidResponse(format!("{}\nRaw response: {}", e, &text[..text.len().min(500)]))
        })
    }

    /// Query open orders.
    pub async fn query_open_orders(
        &self,
        token: &str,
        symbol: Option<&str>,
    ) -> Result<Vec<OpenOrder>, ClientError> {
        let mut url = format!("{}/api/query_open_orders", self.perps_base_url);
        if let Some(sym) = symbol {
            url.push_str(&format!("?symbol={}", sym));
        }

        let response = self.http
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        let text = response.text().await.map_err(|e| ClientError::InvalidResponse(e.to_string()))?;

        // API returns wrapped response: {code, message, page_size, result: [...]}
        let wrapper: PaginatedResponse<OpenOrder> = serde_json::from_str(&text).map_err(|e| {
            ClientError::InvalidResponse(format!("{}\nRaw response: {}", e, &text[..text.len().min(500)]))
        })?;
        Ok(wrapper.result)
    }

    /// Cancel orders (batch) - requires signature.
    ///
    /// Returns Ok on success. The API returns `[]` for successful cancellation.
    pub async fn cancel_orders(
        &self,
        token: &str,
        request_id: &str,
        timestamp_ms: u64,
        signature: &str,
        body: &CancelOrdersRequest,
    ) -> Result<(), ClientError> {
        let url = format!("{}/api/cancel_orders", self.perps_base_url);

        let response = self.http
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("x-request-sign-version", "v1")
            .header("x-request-id", request_id)
            .header("x-request-timestamp", timestamp_ms.to_string())
            .header("x-request-signature", signature)
            .json(body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        // API returns [] on success
        Ok(())
    }

    /// Place a new order via HTTP - requires signature.
    pub async fn new_order(
        &self,
        token: &str,
        request_id: &str,
        timestamp_ms: u64,
        signature: &str,
        body: &NewOrderRequest,
    ) -> Result<OrderResponse, ClientError> {
        let url = format!("{}/api/new_order", self.perps_base_url);

        let response = self.http
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("x-request-sign-version", "v1")
            .header("x-request-id", request_id)
            .header("x-request-timestamp", timestamp_ms.to_string())
            .header("x-request-signature", signature)
            .json(body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        let text = response.text().await.map_err(|e| ClientError::InvalidResponse(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| {
            ClientError::InvalidResponse(format!("{}\nRaw response: {}", e, &text[..text.len().min(500)]))
        })
    }

    /// Query orderbook depth (public endpoint, no auth required).
    pub async fn query_depth_book(&self, symbol: &str) -> Result<DepthBookResponse, ClientError> {
        let url = format!("{}/api/query_depth_book?symbol={}", self.perps_base_url, symbol);

        let response = self.http
            .get(&url)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        let text = response.text().await.map_err(|e| ClientError::InvalidResponse(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| {
            ClientError::InvalidResponse(format!("{}\nRaw response: {}", e, &text[..text.len().min(500)]))
        })
    }

    /// Query symbol info (public endpoint, no auth required).
    ///
    /// Returns tick size and lot size decimals for the given symbol.
    /// Note: The API returns an array of all symbols, so we find the matching one.
    pub async fn query_symbol_info(&self, symbol: &str) -> Result<SymbolInfo, ClientError> {
        let url = format!("{}/api/query_symbol_info?symbol={}", self.perps_base_url, symbol);

        let response = self.http
            .get(&url)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        let text = response.text().await.map_err(|e| ClientError::InvalidResponse(e.to_string()))?;

        // API returns an array of symbols, find the matching one
        let symbols: Vec<SymbolInfo> = serde_json::from_str(&text).map_err(|e| {
            ClientError::InvalidResponse(format!("{}\nRaw response: {}", e, &text[..text.len().min(500)]))
        })?;

        symbols.into_iter()
            .find(|s| s.symbol == symbol)
            .ok_or_else(|| ClientError::InvalidResponse(format!("Symbol '{}' not found in API response", symbol)))
    }

    /// Query position config (leverage, margin mode).
    pub async fn query_position_config(
        &self,
        token: &str,
        symbol: &str,
    ) -> Result<PositionConfig, ClientError> {
        let url = format!("{}/api/query_position_config?symbol={}", self.perps_base_url, symbol);

        let response = self.http
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        let text = response.text().await.map_err(|e| ClientError::InvalidResponse(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| {
            ClientError::InvalidResponse(format!("{}\nRaw response: {}", e, &text[..text.len().min(500)]))
        })
    }

    /// Change leverage - requires signature.
    pub async fn change_leverage(
        &self,
        token: &str,
        request_id: &str,
        timestamp_ms: u64,
        signature: &str,
        body: &ChangeLeverageRequest,
    ) -> Result<OrderResponse, ClientError> {
        let url = format!("{}/api/change_leverage", self.perps_base_url);

        let response = self.http
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("x-request-sign-version", "v1")
            .header("x-request-id", request_id)
            .header("x-request-timestamp", timestamp_ms.to_string())
            .header("x-request-signature", signature)
            .json(body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::ApiError(format!("{}: {}", status, text)));
        }

        let text = response.text().await.map_err(|e| ClientError::InvalidResponse(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| {
            ClientError::InvalidResponse(format!("{}\nRaw response: {}", e, &text[..text.len().min(500)]))
        })
    }
}

impl Default for StandXClient {
    fn default() -> Self {
        Self::new()
    }
}
