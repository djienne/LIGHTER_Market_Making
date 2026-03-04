//! Configuration loading and validation for the StandX orderbook parser.

use serde::Deserialize;
use std::path::Path;
use thiserror::Error;

use crate::websocket::reconnect::ReconnectConfig;

/// Configuration errors.
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    ReadError(#[from] std::io::Error),

    #[error("Failed to parse config file: {0}")]
    ParseError(#[from] serde_json::Error),

    #[error("Invalid configuration: {0}")]
    ValidationError(String),
}

/// WebSocket connection configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct WebSocketConfig {
    /// WebSocket URL for market data stream
    #[serde(default = "default_ws_url")]
    pub url: String,

    /// WebSocket URL for order API (authenticated)
    #[serde(default = "default_ws_api_url")]
    pub api_url: String,

    /// Initial reconnection delay in seconds
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_secs: u64,

    /// Maximum reconnection delay in seconds
    #[serde(default = "default_max_reconnect_delay")]
    pub max_reconnect_delay_secs: u64,

    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,

    /// Stale connection timeout in seconds (force reconnect if no message)
    #[serde(default = "default_stale_timeout")]
    pub stale_timeout_secs: u64,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            url: default_ws_url(),
            api_url: default_ws_api_url(),
            reconnect_delay_secs: default_reconnect_delay(),
            max_reconnect_delay_secs: default_max_reconnect_delay(),
            connect_timeout_secs: default_connect_timeout(),
            stale_timeout_secs: default_stale_timeout(),
        }
    }
}

impl WebSocketConfig {
    /// Create a ReconnectConfig for orderbook WebSocket (unlimited retries).
    pub fn to_reconnect_config(&self) -> ReconnectConfig {
        ReconnectConfig {
            initial_delay_secs: self.reconnect_delay_secs,
            max_delay_secs: self.max_reconnect_delay_secs,
            stale_timeout_secs: self.stale_timeout_secs,
            connect_timeout_secs: self.connect_timeout_secs,
            max_retries: None, // Orderbook WS retries indefinitely
        }
    }

    /// Create a ReconnectConfig for order WebSocket (limited retries).
    pub fn to_order_reconnect_config(&self, max_retries: u32) -> ReconnectConfig {
        ReconnectConfig {
            initial_delay_secs: self.reconnect_delay_secs,
            max_delay_secs: self.max_reconnect_delay_secs,
            stale_timeout_secs: self.stale_timeout_secs,
            connect_timeout_secs: self.connect_timeout_secs,
            max_retries: if max_retries == 0 { None } else { Some(max_retries) },
        }
    }
}

fn default_ws_url() -> String {
    "wss://perps.standx.com/ws-stream/v1".to_string()
}

fn default_ws_api_url() -> String {
    "wss://perps.standx.com/ws-api/v1".to_string()
}

fn default_reconnect_delay() -> u64 {
    5
}

fn default_max_reconnect_delay() -> u64 {
    60
}

fn default_connect_timeout() -> u64 {
    30
}

fn default_stale_timeout() -> u64 {
    60
}

/// OBI Strategy configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    /// FALLBACK tick size - only used if API fetch fails.
    /// The actual tick_size is automatically fetched from the exchange API.
    #[serde(default = "default_tick_size")]
    pub tick_size: f64,

    /// Simulation step in nanoseconds (100ms default)
    #[serde(default = "default_step_ns")]
    pub step_ns: u64,

    /// Rolling window length in steps
    #[serde(default = "default_window_steps")]
    pub window_steps: usize,

    /// Update interval in steps
    #[serde(default = "default_update_interval_steps")]
    pub update_interval_steps: usize,

    /// Volatility to half-spread multiplier
    #[serde(default = "default_vol_to_half_spread")]
    pub vol_to_half_spread: f64,

    /// Fixed half-spread in price units (optional, 0 to disable)
    #[serde(default)]
    pub half_spread: f64,

    /// Fixed half-spread in basis points (optional, 0 to disable)
    #[serde(default)]
    pub half_spread_bps: f64,

    /// Minimum half-spread in basis points (default: 2.0)
    /// Applied as a floor regardless of volatility calculations.
    #[serde(default = "default_min_half_spread_bps")]
    pub min_half_spread_bps: f64,

    /// Position skew factor
    #[serde(default = "default_skew")]
    pub skew: f64,

    /// Alpha coefficient in absolute price units (e.g., 3.6 means fair_price shifts by 3.6 per z-score).
    /// This is tick-size independent. If set to 0, falls back to c1_ticks * tick_size.
    #[serde(default)]
    pub c1: f64,

    /// DEPRECATED: Alpha coefficient in ticks. Use `c1` instead for tick-size independence.
    /// Only used if `c1` is 0. Calculated as: c1 = c1_ticks * tick_size_fallback.
    #[serde(default = "default_c1_ticks")]
    pub c1_ticks: f64,

    /// Depth percentage for imbalance calculation (e.g., 0.025 = 2.5%)
    #[serde(default = "default_looking_depth")]
    pub looking_depth: f64,

    /// Minimum order quantity in dollar value.
    /// Acts as a floor to prevent dust orders when equity is very low.
    /// Default: 10.0
    #[serde(default = "default_min_order_qty_dollar")]
    pub min_order_qty_dollar: f64,

    /// FALLBACK lot size - only used if API fetch fails.
    /// The actual lot_size is automatically fetched from the exchange API.
    #[serde(default = "default_lot_size")]
    pub lot_size: f64,

    /// Number of order levels per side (1 or 2).
    /// 1 = single bid + single ask (default, current behavior)
    /// 2 = 2 bids + 2 asks with spread multipliers
    #[serde(default = "default_order_levels")]
    pub order_levels: usize,

    /// Spread multiplier for level 1 (outer level).
    /// Level 0 uses base half_spread, level 1 uses half_spread * spread_level_multiplier.
    /// Default: 1.5
    #[serde(default = "default_spread_level_multiplier")]
    pub spread_level_multiplier: f64,

    /// Alpha source: "binance" (default) or "standx".
    /// When set to "binance", uses Binance Futures orderbook imbalance for alpha signal.
    /// Falls back to StandX alpha if Binance is stale or not warmed up.
    #[serde(default = "default_alpha_source")]
    pub alpha_source: String,

    /// Binance alpha stale threshold in milliseconds.
    /// If Binance alpha hasn't been updated within this time, falls back to StandX.
    /// Default: 5000 (5 seconds)
    #[serde(default = "default_binance_stale_ms")]
    pub binance_stale_ms: u64,

    /// Leverage multiplier (1.0–5.0).
    /// Multiplies effective capital for order sizing and position limits.
    /// With leverage N, order_qty and max_position scale as if equity were `equity * N`.
    /// Default: 1.0 (no leverage)
    #[serde(default = "default_leverage")]
    pub leverage: f64,
}

fn default_tick_size() -> f64 { 0.01 }
fn default_step_ns() -> u64 { 100_000_000 } // 100ms
fn default_window_steps() -> usize { 6000 }
fn default_update_interval_steps() -> usize { 1 }  // Update on every message
fn default_vol_to_half_spread() -> f64 { 0.8 }
fn default_skew() -> f64 { 1.0 }
fn default_c1_ticks() -> f64 { 160.0 }
fn default_looking_depth() -> f64 { 0.025 }
fn default_min_order_qty_dollar() -> f64 { 10.0 }
fn default_lot_size() -> f64 { 0.001 }
fn default_min_half_spread_bps() -> f64 { 2.0 }
fn default_order_levels() -> usize { 1 }
fn default_spread_level_multiplier() -> f64 { 1.5 }
fn default_alpha_source() -> String { "binance".to_string() }
fn default_binance_stale_ms() -> u64 { 5000 }
fn default_leverage() -> f64 { 1.0 }

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            tick_size: default_tick_size(),
            step_ns: default_step_ns(),
            window_steps: default_window_steps(),
            update_interval_steps: default_update_interval_steps(),
            vol_to_half_spread: default_vol_to_half_spread(),
            half_spread: 0.0,
            half_spread_bps: 0.0,
            min_half_spread_bps: default_min_half_spread_bps(),
            skew: default_skew(),
            c1: 0.0, // 0 = use c1_ticks fallback
            c1_ticks: default_c1_ticks(),
            looking_depth: default_looking_depth(),
            min_order_qty_dollar: default_min_order_qty_dollar(),
            lot_size: default_lot_size(),
            order_levels: default_order_levels(),
            spread_level_multiplier: default_spread_level_multiplier(),
            alpha_source: default_alpha_source(),
            binance_stale_ms: default_binance_stale_ms(),
            leverage: default_leverage(),
        }
    }
}

impl StrategyConfig {
    /// Get c1 in price units.
    /// If `c1` is set (> 0), returns it directly.
    /// Otherwise falls back to `c1_ticks * tick_size` (deprecated).
    #[inline]
    pub fn c1(&self) -> f64 {
        if self.c1 > 0.0 {
            self.c1
        } else {
            self.c1_ticks * self.tick_size
        }
    }

    /// Get vol_scale for converting per-step std to per-second.
    #[inline]
    pub fn vol_scale(&self) -> f64 {
        (1_000_000_000.0 / self.step_ns as f64).sqrt()
    }

    /// Calculate warm-up time in nanoseconds.
    pub fn warmup_ns(&self) -> u64 {
        // First update at: ceil((window_steps - 1) / update_interval_steps) * update_interval_steps
        let first_step = ((self.window_steps - 1 + self.update_interval_steps - 1)
            / self.update_interval_steps) * self.update_interval_steps;
        first_step as u64 * self.step_ns
    }
}

/// Position polling configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct PositionConfig {
    /// Enable position polling
    #[serde(default = "default_position_enabled")]
    pub enabled: bool,

    /// Position polling interval in seconds
    #[serde(default = "default_position_interval")]
    pub poll_interval_secs: u64,

    /// Stale position threshold in seconds (warn if older)
    #[serde(default = "default_position_stale")]
    pub stale_threshold_secs: u64,
}

fn default_position_enabled() -> bool { true }
fn default_position_interval() -> u64 { 2 }
fn default_position_stale() -> u64 { 10 }

impl Default for PositionConfig {
    fn default() -> Self {
        Self {
            enabled: default_position_enabled(),
            poll_interval_secs: default_position_interval(),
            stale_threshold_secs: default_position_stale(),
        }
    }
}

/// Order management configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderConfig {
    /// Enable automatic order placement
    #[serde(default = "default_order_enabled")]
    pub enabled: bool,

    /// Reprice threshold in basis points (default: 1.0 bps)
    /// Orders will be canceled and replaced if price changes by more than this
    #[serde(default = "default_reprice_threshold_bps")]
    pub reprice_threshold_bps: f64,

    /// Pending order timeout in seconds (default: 5)
    /// Orders stuck in pending state will be cleared after this timeout
    #[serde(default = "default_pending_timeout_secs")]
    pub pending_timeout_secs: u64,

    /// Maximum age for Live orders in seconds (default: 60)
    /// Orders will be refreshed after this time even if price is within threshold.
    /// Set to 0 to disable (orders stay until price changes).
    #[serde(default = "default_max_live_age_secs")]
    pub max_live_age_secs: u64,

    /// Maximum reconnection attempts for order WebSocket (default: 10)
    /// Set to 0 for unlimited retries
    #[serde(default = "default_max_reconnect_attempts")]
    pub max_reconnect_attempts: u32,

    /// Circuit breaker: max consecutive rejections before pausing (default: 5)
    /// Set to 0 to disable circuit breaker
    #[serde(default = "default_circuit_breaker_rejections")]
    pub circuit_breaker_rejections: u32,

    /// Circuit breaker auto-recovery: resume trading after N seconds (default: 300 = 5 minutes)
    /// Set to 0 to disable auto-recovery (manual reset required)
    #[serde(default = "default_circuit_breaker_recovery_secs")]
    pub circuit_breaker_recovery_secs: u64,

    /// Safety pause recovery: resume trading after N seconds when >2 orders detected (default: 30)
    /// This handles rare edge cases where duplicate/stuck orders accumulate.
    /// Set to 0 to disable auto-recovery (manual intervention required)
    #[serde(default = "default_safety_pause_recovery_secs")]
    pub safety_pause_recovery_secs: u64,
}

fn default_order_enabled() -> bool { false } // Disabled by default for safety
fn default_reprice_threshold_bps() -> f64 { 1.0 }
fn default_pending_timeout_secs() -> u64 { 5 }
fn default_max_live_age_secs() -> u64 { 60 }
fn default_max_reconnect_attempts() -> u32 { 0 } // 0 = unlimited retries
fn default_circuit_breaker_rejections() -> u32 { 5 }
fn default_circuit_breaker_recovery_secs() -> u64 { 300 } // 5 minutes default
fn default_safety_pause_recovery_secs() -> u64 { 30 } // 30 seconds default

impl Default for OrderConfig {
    fn default() -> Self {
        Self {
            enabled: default_order_enabled(),
            reprice_threshold_bps: default_reprice_threshold_bps(),
            pending_timeout_secs: default_pending_timeout_secs(),
            max_live_age_secs: default_max_live_age_secs(),
            max_reconnect_attempts: default_max_reconnect_attempts(),
            circuit_breaker_rejections: default_circuit_breaker_rejections(),
            circuit_breaker_recovery_secs: default_circuit_breaker_recovery_secs(),
            safety_pause_recovery_secs: default_safety_pause_recovery_secs(),
        }
    }
}

/// Wallet tracking configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct WalletConfig {
    /// Enable wallet tracking
    #[serde(default = "default_wallet_enabled")]
    pub enabled: bool,

    /// Wallet polling interval in seconds
    #[serde(default = "default_wallet_interval")]
    pub poll_interval_secs: u64,

    /// Path to the CSV file for wallet history
    #[serde(default = "default_wallet_csv_path")]
    pub csv_path: String,
}

fn default_wallet_enabled() -> bool { false } // Disabled by default
fn default_wallet_interval() -> u64 { 60 }
fn default_wallet_csv_path() -> String { "wallet_history.csv".to_string() }

impl Default for WalletConfig {
    fn default() -> Self {
        Self {
            enabled: default_wallet_enabled(),
            poll_interval_secs: default_wallet_interval(),
            csv_path: default_wallet_csv_path(),
        }
    }
}

/// Orderbook sanity check configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct SanityCheckConfig {
    /// Enable periodic sanity checks against REST API
    #[serde(default = "default_sanity_enabled")]
    pub enabled: bool,

    /// Sanity check interval in seconds
    #[serde(default = "default_sanity_interval")]
    pub interval_secs: u64,

    /// Drift threshold in basis points (1 bps = 0.01%)
    #[serde(default = "default_drift_threshold_bps")]
    pub drift_threshold_bps: f64,
}

fn default_sanity_enabled() -> bool { false }
fn default_sanity_interval() -> u64 { 30 }
fn default_drift_threshold_bps() -> f64 { 1.0 }  // 1 bps = 0.01%

impl Default for SanityCheckConfig {
    fn default() -> Self {
        Self {
            enabled: default_sanity_enabled(),
            interval_secs: default_sanity_interval(),
            drift_threshold_bps: default_drift_threshold_bps(),
        }
    }
}

/// Symbol info (tick size) polling configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct SymbolInfoConfig {
    /// Enable automatic tick size detection from API
    #[serde(default = "default_symbol_info_enabled")]
    pub enabled: bool,

    /// Polling interval in seconds to check for tick size changes
    #[serde(default = "default_symbol_info_interval")]
    pub poll_interval_secs: u64,
}

fn default_symbol_info_enabled() -> bool { true } // Enabled by default
fn default_symbol_info_interval() -> u64 { 30 }

impl Default for SymbolInfoConfig {
    fn default() -> Self {
        Self {
            enabled: default_symbol_info_enabled(),
            poll_interval_secs: default_symbol_info_interval(),
        }
    }
}

/// Main application configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// List of symbols to subscribe to
    #[serde(default = "default_symbols")]
    pub symbols: Vec<String>,

    /// Number of orderbook levels to track per side
    #[serde(default = "default_orderbook_levels")]
    pub orderbook_levels: usize,

    /// Rolling history duration in minutes
    #[serde(default = "default_history_minutes")]
    pub history_minutes: u64,

    /// Size of the history ring buffer (number of snapshots)
    #[serde(default = "default_history_buffer_size")]
    pub history_buffer_size: usize,

    /// WebSocket configuration
    #[serde(default)]
    pub websocket: WebSocketConfig,

    /// Enable verbose logging
    #[serde(default)]
    pub verbose: bool,

    /// Stats logging interval in seconds (0 to disable)
    #[serde(default = "default_stats_interval")]
    pub stats_interval_secs: u64,

    /// Enable debug logging (controls all logging output)
    #[serde(default = "default_debug")]
    pub debug: bool,

    /// OBI strategy configuration
    #[serde(default)]
    pub strategy: StrategyConfig,

    /// Position polling configuration
    #[serde(default)]
    pub position: PositionConfig,

    /// Order management configuration
    #[serde(default)]
    pub order: OrderConfig,

    /// PnL tracking configuration
    #[serde(default)]
    pub pnl_tracking: WalletConfig,

    /// Orderbook sanity check configuration
    #[serde(default)]
    pub orderbook_sanity_check: SanityCheckConfig,

    /// Symbol info (tick size) polling configuration
    #[serde(default)]
    pub symbol_info: SymbolInfoConfig,
}

fn default_symbols() -> Vec<String> {
    vec!["TEST-USD".to_string()]
}

fn default_orderbook_levels() -> usize {
    20
}

fn default_history_minutes() -> u64 {
    10
}

fn default_history_buffer_size() -> usize {
    100_000
}

fn default_stats_interval() -> u64 {
    30
}

fn default_debug() -> bool {
    true // Debug logging ON by default
}

impl Default for Config {
    fn default() -> Self {
        Self {
            symbols: default_symbols(),
            orderbook_levels: default_orderbook_levels(),
            history_minutes: default_history_minutes(),
            history_buffer_size: default_history_buffer_size(),
            websocket: WebSocketConfig::default(),
            verbose: false,
            stats_interval_secs: default_stats_interval(),
            debug: default_debug(),
            strategy: StrategyConfig::default(),
            position: PositionConfig::default(),
            order: OrderConfig::default(),
            pnl_tracking: WalletConfig::default(),
            orderbook_sanity_check: SanityCheckConfig::default(),
            symbol_info: SymbolInfoConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from a JSON file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = serde_json::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from a JSON string.
    pub fn from_str(json: &str) -> Result<Self, ConfigError> {
        let config: Config = serde_json::from_str(json)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.symbols.is_empty() {
            return Err(ConfigError::ValidationError(
                "At least one symbol must be specified".to_string()
            ));
        }

        if self.orderbook_levels == 0 || self.orderbook_levels > 50 {
            return Err(ConfigError::ValidationError(
                "orderbook_levels must be between 1 and 50".to_string()
            ));
        }

        if self.history_minutes == 0 {
            return Err(ConfigError::ValidationError(
                "history_minutes must be greater than 0".to_string()
            ));
        }

        if self.history_buffer_size < 1000 {
            return Err(ConfigError::ValidationError(
                "history_buffer_size must be at least 1000".to_string()
            ));
        }

        // Validate strategy config
        if self.strategy.tick_size <= 0.0 {
            return Err(ConfigError::ValidationError(
                "strategy.tick_size must be positive".to_string()
            ));
        }

        if self.strategy.window_steps == 0 {
            return Err(ConfigError::ValidationError(
                "strategy.window_steps must be greater than 0".to_string()
            ));
        }

        if self.strategy.looking_depth <= 0.0 || self.strategy.looking_depth >= 1.0 {
            return Err(ConfigError::ValidationError(
                "strategy.looking_depth must be between 0 and 1".to_string()
            ));
        }

        if self.strategy.order_levels == 0 || self.strategy.order_levels > 2 {
            return Err(ConfigError::ValidationError(
                "strategy.order_levels must be 1 or 2".to_string()
            ));
        }

        if self.strategy.spread_level_multiplier <= 1.0 {
            return Err(ConfigError::ValidationError(
                "strategy.spread_level_multiplier must be greater than 1.0".to_string()
            ));
        }

        if self.strategy.leverage < 1.0 || self.strategy.leverage > 5.0 {
            return Err(ConfigError::ValidationError(
                "strategy.leverage must be between 1.0 and 5.0".to_string()
            ));
        }

        Ok(())
    }

    /// Get the history retention duration in nanoseconds.
    pub fn history_retention_ns(&self) -> u64 {
        self.history_minutes * 60 * 1_000_000_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.symbols, vec!["TEST-USD"]);
        assert_eq!(config.orderbook_levels, 20);
        assert_eq!(config.history_minutes, 10);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_parse_config() {
        let json = r#"{
            "symbols": ["TEST-USD", "ETH-USD"],
            "orderbook_levels": 10,
            "history_minutes": 5
        }"#;

        let config = Config::from_str(json).unwrap();
        assert_eq!(config.symbols.len(), 2);
        assert_eq!(config.orderbook_levels, 10);
        assert_eq!(config.history_minutes, 5);
    }

    #[test]
    fn test_invalid_config() {
        let json = r#"{"orderbook_levels": 0}"#;
        assert!(Config::from_str(json).is_err());

        let json = r#"{"history_minutes": 0}"#;
        assert!(Config::from_str(json).is_err());
    }
}
