//! Authentication module for StandX trading.
//!
//! This module provides JWT token management for authenticated API access.
//!
//! # Authentication Flow (BSC Chain)
//!
//! 1. Generate ed25519 keypair -> base58(public_key) = requestId
//! 2. POST /v1/offchain/prepare-signin?chain=bsc with {address, requestId}
//! 3. Decode JWT response, extract "message" field
//! 4. Sign message with wallet private key (Ethereum personal_sign)
//! 5. POST /v1/offchain/login?chain=bsc with {signature, signedData, expiresSeconds}
//! 6. Use returned JWT token for authenticated requests

use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use k256::ecdsa::SigningKey as EcdsaSigningKey;
use sha3::{Keccak256, Digest};
use rand::rngs::OsRng;

use super::client::{StandXClient, ClientError, Position, Balance};

/// Authentication errors.
#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Not authenticated")]
    NotAuthenticated,

    #[error("Token expired")]
    TokenExpired,

    #[error("Invalid credentials: {0}")]
    InvalidCredentials(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Signature error: {0}")]
    SignatureError(String),

    #[error("Client error: {0}")]
    ClientError(#[from] ClientError),
}

/// Authentication credentials.
#[derive(Clone)]
pub struct Credentials {
    /// Wallet address (ETH format: 0x...)
    pub address: String,
    /// Private key for signing (hex encoded, with or without 0x prefix)
    pub private_key: String,
    /// Blockchain chain (bsc or solana)
    pub chain: String,
}

impl Credentials {
    /// Create new credentials.
    pub fn new(address: impl Into<String>, private_key: impl Into<String>, chain: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            private_key: private_key.into(),
            chain: chain.into(),
        }
    }

    /// Load credentials from environment variables.
    ///
    /// Expects WALLET_AD and PRIVATE_KEY environment variables.
    pub fn from_env() -> Result<Self, AuthError> {
        let address = std::env::var("WALLET_AD")
            .map_err(|_| AuthError::InvalidCredentials("WALLET_AD not set".to_string()))?;
        let private_key = std::env::var("PRIVATE_KEY")
            .map_err(|_| AuthError::InvalidCredentials("PRIVATE_KEY not set".to_string()))?;

        Ok(Self {
            address,
            private_key,
            chain: "bsc".to_string(), // Default to BSC
        })
    }
}

/// JWT token with expiration tracking.
#[derive(Clone)]
pub struct AuthToken {
    /// JWT token string
    pub token: String,
    /// Expiration timestamp (Unix seconds)
    pub expires_at: u64,
    /// User address
    pub address: String,
}

impl AuthToken {
    /// Check if the token is expired.
    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        now >= self.expires_at
    }

    /// Get remaining validity in seconds.
    pub fn remaining_secs(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.expires_at.saturating_sub(now)
    }

    /// Get the JWT token string.
    pub fn jwt(&self) -> &str {
        &self.token
    }
}

/// Authentication manager for StandX API.
///
/// Handles JWT token acquisition and refresh.
pub struct AuthManager {
    /// HTTP client for API calls
    client: StandXClient,
    /// Credentials for authentication
    credentials: Option<Credentials>,
    /// Current auth token
    token: Option<AuthToken>,
    /// ed25519 keypair for request signing
    ed25519_key: Option<Ed25519SigningKey>,
    /// Request ID (base58 encoded ed25519 public key)
    request_id: Option<String>,
    /// Number of successful authentications
    auth_count: AtomicU64,
}

impl AuthManager {
    /// Create a new auth manager without credentials.
    pub fn new() -> Self {
        Self {
            client: StandXClient::new(),
            credentials: None,
            token: None,
            ed25519_key: None,
            request_id: None,
            auth_count: AtomicU64::new(0),
        }
    }

    /// Create a new auth manager with credentials.
    pub fn with_credentials(credentials: Credentials) -> Self {
        Self {
            client: StandXClient::new(),
            credentials: Some(credentials),
            token: None,
            ed25519_key: None,
            request_id: None,
            auth_count: AtomicU64::new(0),
        }
    }

    /// Create auth manager from environment variables.
    pub fn from_env() -> Result<Self, AuthError> {
        let credentials = Credentials::from_env()?;
        Ok(Self::with_credentials(credentials))
    }

    /// Set credentials.
    pub fn set_credentials(&mut self, credentials: Credentials) {
        self.credentials = Some(credentials);
        self.token = None; // Invalidate existing token
    }

    /// Check if credentials are configured.
    pub fn has_credentials(&self) -> bool {
        self.credentials.is_some()
    }

    /// Check if currently authenticated.
    pub fn is_authenticated(&self) -> bool {
        self.token.as_ref().map(|t| !t.is_expired()).unwrap_or(false)
    }

    /// Get the current token if valid.
    pub fn token(&self) -> Option<&AuthToken> {
        self.token.as_ref().filter(|t| !t.is_expired())
    }

    /// Get the JWT token string if authenticated.
    pub fn jwt(&self) -> Option<&str> {
        self.token().map(|t| t.token.as_str())
    }

    /// Get reference to the HTTP client.
    pub fn client(&self) -> &StandXClient {
        &self.client
    }

    /// Generate ed25519 keypair and request ID.
    fn generate_keypair(&mut self) {
        let signing_key = Ed25519SigningKey::generate(&mut OsRng);
        let public_key = signing_key.verifying_key();
        let request_id = bs58::encode(public_key.as_bytes()).into_string();

        self.ed25519_key = Some(signing_key);
        self.request_id = Some(request_id);
    }

    /// Decode JWT payload without verification.
    fn decode_jwt_payload(jwt: &str) -> Result<serde_json::Value, AuthError> {
        let parts: Vec<&str> = jwt.split('.').collect();
        if parts.len() != 3 {
            return Err(AuthError::InvalidCredentials("Invalid JWT format".to_string()));
        }

        // Decode payload (second part)
        let payload_b64 = parts[1];
        // Add padding if needed for base64 URL-safe decoding
        let padding = (4 - payload_b64.len() % 4) % 4;
        let padded = format!("{}{}", payload_b64, "=".repeat(padding));

        // Use URL-safe base64 decoding
        let payload_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            payload_b64
        ).or_else(|_| {
            base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD,
                &padded
            )
        }).map_err(|e| AuthError::InvalidCredentials(format!("Failed to decode JWT: {}", e)))?;

        serde_json::from_slice(&payload_bytes)
            .map_err(|e| AuthError::InvalidCredentials(format!("Failed to parse JWT payload: {}", e)))
    }

    /// Sign a message using Ethereum personal_sign format.
    fn sign_message_eth(&self, message: &str) -> Result<String, AuthError> {
        let credentials = self.credentials.as_ref()
            .ok_or(AuthError::NotAuthenticated)?;

        // Parse private key (remove 0x prefix if present)
        let key_hex = credentials.private_key.strip_prefix("0x")
            .unwrap_or(&credentials.private_key);
        let key_bytes = hex::decode(key_hex)
            .map_err(|e| AuthError::InvalidCredentials(format!("Invalid private key: {}", e)))?;

        let signing_key = EcdsaSigningKey::from_slice(&key_bytes)
            .map_err(|e| AuthError::InvalidCredentials(format!("Invalid private key: {}", e)))?;

        // Ethereum personal sign: keccak256("\x19Ethereum Signed Message:\n" + len(message) + message)
        let prefix = format!("\x19Ethereum Signed Message:\n{}", message.len());
        let mut hasher = Keccak256::new();
        hasher.update(prefix.as_bytes());
        hasher.update(message.as_bytes());
        let hash = hasher.finalize();

        // Sign the hash
        let (signature, recovery_id) = signing_key
            .sign_prehash_recoverable(&hash)
            .map_err(|e| AuthError::SignatureError(format!("Failed to sign: {}", e)))?;

        // Format signature as 0x + r (32 bytes) + s (32 bytes) + v (1 byte)
        let mut sig_bytes = Vec::with_capacity(65);
        sig_bytes.extend_from_slice(&signature.to_bytes());
        // v = recovery_id + 27 for Ethereum
        sig_bytes.push(recovery_id.to_byte() + 27);

        Ok(format!("0x{}", hex::encode(sig_bytes)))
    }

    /// Authenticate and obtain a JWT token.
    pub async fn authenticate(&mut self) -> Result<&AuthToken, AuthError> {
        let credentials = self.credentials.clone()
            .ok_or(AuthError::NotAuthenticated)?;

        // Generate new keypair for this session
        self.generate_keypair();
        let request_id = self.request_id.clone().unwrap();

        // Step 1: Prepare signin
        let prepare_response = self.client
            .prepare_signin(&credentials.address, &request_id, &credentials.chain)
            .await?;

        // Step 2: Decode JWT to get message
        let payload = Self::decode_jwt_payload(&prepare_response.signed_data)?;
        let message = payload.get("message")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AuthError::InvalidCredentials("No message in JWT".to_string()))?;

        // Step 3: Sign the message with wallet private key
        let signature = self.sign_message_eth(message)?;

        // Step 4: Login with signature (1 week expiry)
        let expires_seconds = 7 * 24 * 60 * 60; // 1 week
        let login_response = self.client
            .login(&signature, &prepare_response.signed_data, &credentials.chain, expires_seconds)
            .await?;

        // Calculate expiration time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Store token
        self.token = Some(AuthToken {
            token: login_response.token,
            expires_at: now + expires_seconds,
            address: login_response.address,
        });

        self.auth_count.fetch_add(1, Ordering::Relaxed);

        Ok(self.token.as_ref().unwrap())
    }

    /// Refresh the token if expired or close to expiring.
    pub async fn refresh_if_needed(&mut self, min_remaining_secs: u64) -> Result<(), AuthError> {
        if let Some(token) = &self.token {
            if token.remaining_secs() < min_remaining_secs {
                self.authenticate().await?;
            }
        }
        Ok(())
    }

    /// Minimum token validity before proactive refresh (1 hour).
    const MIN_TOKEN_VALIDITY_SECS: u64 = 3600;

    /// Ensure we have a valid token, refreshing proactively if needed.
    async fn ensure_valid_token(&mut self) -> Result<&str, AuthError> {
        let needs_refresh = self.token.as_ref()
            .map(|t| t.remaining_secs() < Self::MIN_TOKEN_VALIDITY_SECS)
            .unwrap_or(true);

        if needs_refresh {
            self.authenticate().await?;
        }

        Ok(self.token.as_ref().unwrap().token.as_str())
    }

    /// Query positions (convenience method).
    ///
    /// Automatically refreshes token if < 1 hour remaining or on auth error.
    pub async fn query_positions(&mut self, symbol: Option<&str>) -> Result<Vec<Position>, AuthError> {
        // Proactive refresh
        let token = self.ensure_valid_token().await?.to_string();

        // Try request
        match self.client.query_positions(&token, symbol).await {
            Ok(result) => Ok(result),
            Err(ClientError::ApiError(e)) if e.contains("401") || e.contains("unauthorized") => {
                // Retry on auth error
                self.authenticate().await?;
                let new_token = self.token.as_ref().unwrap().token.clone();
                self.client.query_positions(&new_token, symbol).await.map_err(Into::into)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Query balance (convenience method).
    ///
    /// Automatically refreshes token if < 1 hour remaining or on auth error.
    pub async fn query_balance(&mut self) -> Result<Balance, AuthError> {
        // Proactive refresh
        let token = self.ensure_valid_token().await?.to_string();

        // Try request
        match self.client.query_balance(&token).await {
            Ok(result) => Ok(result),
            Err(ClientError::ApiError(e)) if e.contains("401") || e.contains("unauthorized") => {
                // Retry on auth error
                self.authenticate().await?;
                let new_token = self.token.as_ref().unwrap().token.clone();
                self.client.query_balance(&new_token).await.map_err(Into::into)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Query open orders.
    pub async fn query_open_orders(&mut self, symbol: Option<&str>) -> Result<Vec<super::client::OpenOrder>, AuthError> {
        // Proactive refresh
        let token = self.ensure_valid_token().await?.to_string();

        // Try request
        match self.client.query_open_orders(&token, symbol).await {
            Ok(result) => Ok(result),
            Err(ClientError::ApiError(e)) if e.contains("401") || e.contains("unauthorized") => {
                // Retry on auth error
                self.authenticate().await?;
                let new_token = self.token.as_ref().unwrap().token.clone();
                self.client.query_open_orders(&new_token, symbol).await.map_err(Into::into)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Cancel orders by ID list (batch).
    pub async fn cancel_orders(&mut self, order_ids: &[i64]) -> Result<(), AuthError> {
        // Ensure valid token
        let token = self.ensure_valid_token().await?.to_string();

        // Build request body
        let body = super::client::CancelOrdersRequest {
            order_id_list: Some(order_ids.to_vec()),
            cl_ord_id_list: None,
        };

        // Sign the request
        let body_json = serde_json::to_string(&body)
            .map_err(|e| AuthError::SignatureError(e.to_string()))?;
        let (request_id, timestamp_ms, signature) = self.sign_request(&body_json)?;

        // Send request
        self.client.cancel_orders(&token, &request_id, timestamp_ms, &signature, &body)
            .await
            .map_err(Into::into)
    }

    /// Cancel orders by client order ID list (batch).
    pub async fn cancel_orders_by_client_id(&mut self, cl_ord_ids: &[String]) -> Result<(), AuthError> {
        // Ensure valid token
        let token = self.ensure_valid_token().await?.to_string();

        // Build request body
        let body = super::client::CancelOrdersRequest {
            order_id_list: None,
            cl_ord_id_list: Some(cl_ord_ids.to_vec()),
        };

        // Sign the request
        let body_json = serde_json::to_string(&body)
            .map_err(|e| AuthError::SignatureError(e.to_string()))?;
        let (request_id, timestamp_ms, signature) = self.sign_request(&body_json)?;

        // Send request
        self.client.cancel_orders(&token, &request_id, timestamp_ms, &signature, &body)
            .await
            .map_err(Into::into)
    }

    /// Cancel all open orders for a given symbol (or all symbols if None).
    ///
    /// Queries open orders and cancels them in batch.
    /// Returns the number of orders canceled.
    pub async fn cancel_all_orders(&mut self, symbol: Option<&str>) -> Result<usize, AuthError> {
        // Query open orders
        let open_orders = self.query_open_orders(symbol).await?;

        if open_orders.is_empty() {
            return Ok(0);
        }

        // Extract order IDs
        let order_ids: Vec<i64> = open_orders.iter().map(|o| o.id).collect();
        let count = order_ids.len();

        // Cancel all orders
        self.cancel_orders(&order_ids).await?;

        Ok(count)
    }

    /// Place a new order via HTTP (fallback).
    pub async fn place_order(&mut self, request: &super::client::NewOrderRequest) -> Result<super::client::OrderResponse, AuthError> {
        // Ensure valid token
        let token = self.ensure_valid_token().await?.to_string();

        // Sign the request
        let body_json = serde_json::to_string(request)
            .map_err(|e| AuthError::SignatureError(e.to_string()))?;
        let (request_id, timestamp_ms, signature) = self.sign_request(&body_json)?;

        // Send request
        self.client.new_order(&token, &request_id, timestamp_ms, &signature, request)
            .await
            .map_err(Into::into)
    }

    /// Query current leverage for a symbol.
    pub async fn query_leverage(&mut self, symbol: &str) -> Result<i32, AuthError> {
        // Proactive refresh
        let token = self.ensure_valid_token().await?.to_string();

        // Try request
        match self.client.query_position_config(&token, symbol).await {
            Ok(config) => Ok(config.leverage),
            Err(ClientError::ApiError(e)) if e.contains("401") || e.contains("unauthorized") => {
                // Retry on auth error
                self.authenticate().await?;
                let new_token = self.token.as_ref().unwrap().token.clone();
                let config = self.client.query_position_config(&new_token, symbol).await?;
                Ok(config.leverage)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Set leverage for a symbol. Returns Ok if successful.
    pub async fn set_leverage(&mut self, symbol: &str, leverage: i32) -> Result<(), AuthError> {
        // Ensure valid token
        let token = self.ensure_valid_token().await?.to_string();

        // Build request body
        let body = super::client::ChangeLeverageRequest {
            symbol: symbol.to_string(),
            leverage,
        };

        // Sign the request
        let body_json = serde_json::to_string(&body)
            .map_err(|e| AuthError::SignatureError(e.to_string()))?;
        let (request_id, timestamp_ms, signature) = self.sign_request(&body_json)?;

        // Send request
        let response = self.client.change_leverage(&token, &request_id, timestamp_ms, &signature, &body)
            .await?;

        if response.is_success() {
            Ok(())
        } else {
            Err(AuthError::NetworkError(format!(
                "Failed to set leverage: {} (code={})",
                response.message, response.code
            )))
        }
    }

    /// Ensure leverage is set to target value. Queries current, changes if needed.
    ///
    /// Returns Ok(()) if leverage is already at target or was successfully changed.
    pub async fn ensure_leverage(&mut self, symbol: &str, target: i32) -> Result<(), AuthError> {
        let current = self.query_leverage(symbol).await?;

        if current == target {
            return Ok(());
        }

        self.set_leverage(symbol, target).await
    }

    /// Sign a request body for authenticated API calls.
    ///
    /// # Returns
    ///
    /// A tuple of (request_id, timestamp_ms, signature_base64)
    pub fn sign_request(&self, body: &str) -> Result<(String, u64, String), AuthError> {
        let ed25519_key = self.ed25519_key.as_ref()
            .ok_or(AuthError::NotAuthenticated)?;
        let request_id = self.request_id.as_ref()
            .ok_or(AuthError::NotAuthenticated)?;

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // Format: v1,{request_id},{timestamp_ms},{body}
        let message = format!("v1,{},{},{}", request_id, timestamp_ms, body);

        // Sign with ed25519
        use ed25519_dalek::Signer;
        let signature = ed25519_key.sign(message.as_bytes());
        let sig_base64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            signature.to_bytes()
        );

        Ok((request_id.clone(), timestamp_ms, sig_base64))
    }

    /// Get authentication count.
    pub fn auth_count(&self) -> u64 {
        self.auth_count.load(Ordering::Relaxed)
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for components that can use authentication.
pub trait Authenticated {
    /// Get the auth manager.
    fn auth(&self) -> &AuthManager;

    /// Check if authenticated.
    fn is_authenticated(&self) -> bool {
        self.auth().is_authenticated()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials() {
        let creds = Credentials::new(
            "0x1234567890abcdef",
            "deadbeef",
            "bsc"
        );
        assert_eq!(creds.chain, "bsc");
    }

    #[test]
    fn test_auth_manager() {
        let mut auth = AuthManager::new();
        assert!(!auth.has_credentials());
        assert!(!auth.is_authenticated());

        auth.set_credentials(Credentials::new("addr", "key", "bsc"));
        assert!(auth.has_credentials());
    }

    #[test]
    fn test_token_expiration() {
        let token = AuthToken {
            token: "test".to_string(),
            expires_at: 0, // Already expired
            address: "test".to_string(),
        };
        assert!(token.is_expired());

        let future_token = AuthToken {
            token: "test".to_string(),
            expires_at: u64::MAX,
            address: "test".to_string(),
        };
        assert!(!future_token.is_expired());
    }

    #[test]
    fn test_keypair_generation() {
        let mut auth = AuthManager::new();
        auth.generate_keypair();
        assert!(auth.request_id.is_some());
        let request_id = auth.request_id.as_ref().unwrap();
        // Base58 encoded ed25519 public key should be around 43-44 characters
        assert!(request_id.len() >= 40 && request_id.len() <= 50);
    }

    #[test]
    fn test_jwt_decode() {
        // Simple test JWT (not a real token)
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJtZXNzYWdlIjoiaGVsbG8ifQ.signature";
        let payload = AuthManager::decode_jwt_payload(jwt).unwrap();
        assert_eq!(payload.get("message").unwrap().as_str().unwrap(), "hello");
    }
}
