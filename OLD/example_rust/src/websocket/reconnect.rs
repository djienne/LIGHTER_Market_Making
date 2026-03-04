//! Shared WebSocket reconnection infrastructure.
//!
//! This module provides reusable reconnection logic with exponential backoff
//! for both orderbook and order WebSocket connections.

use std::sync::atomic::{AtomicU64, Ordering};

/// Configuration for WebSocket reconnection behavior.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Initial delay before first reconnection attempt (seconds).
    pub initial_delay_secs: u64,
    /// Maximum delay between reconnection attempts (seconds).
    pub max_delay_secs: u64,
    /// Timeout for considering connection stale (seconds).
    pub stale_timeout_secs: u64,
    /// Timeout for connection attempt (seconds).
    pub connect_timeout_secs: u64,
    /// Maximum number of reconnection attempts before giving up.
    /// None means retry indefinitely (for orderbook WS).
    /// Some(n) means give up after n attempts (for order WS).
    pub max_retries: Option<u32>,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_secs: 5,
            max_delay_secs: 60,
            stale_timeout_secs: 60,
            connect_timeout_secs: 30,
            max_retries: None, // Retry indefinitely by default
        }
    }
}

impl ReconnectConfig {
    /// Create a config for orderbook WebSocket (retry indefinitely).
    pub fn for_orderbook() -> Self {
        Self::default()
    }

    /// Create a config for order WebSocket (max 10 retries).
    pub fn for_orders() -> Self {
        Self {
            max_retries: Some(10),
            ..Default::default()
        }
    }

    /// Set the maximum number of retries.
    pub fn with_max_retries(mut self, max: u32) -> Self {
        self.max_retries = Some(max);
        self
    }

    /// Set initial delay.
    pub fn with_initial_delay(mut self, secs: u64) -> Self {
        self.initial_delay_secs = secs;
        self
    }

    /// Set max delay.
    pub fn with_max_delay(mut self, secs: u64) -> Self {
        self.max_delay_secs = secs;
        self
    }

    /// Set stale timeout.
    pub fn with_stale_timeout(mut self, secs: u64) -> Self {
        self.stale_timeout_secs = secs;
        self
    }

    /// Set connect timeout.
    pub fn with_connect_timeout(mut self, secs: u64) -> Self {
        self.connect_timeout_secs = secs;
        self
    }
}

/// State tracking for reconnection attempts.
#[derive(Debug)]
pub struct ReconnectState {
    /// Current delay for next reconnection attempt.
    current_delay: u64,
    /// Total number of reconnection attempts.
    reconnect_count: AtomicU64,
    /// Consecutive failures (reset on successful connection).
    consecutive_failures: u32,
}

impl ReconnectState {
    /// Create new reconnection state.
    pub fn new(config: &ReconnectConfig) -> Self {
        Self {
            current_delay: config.initial_delay_secs,
            reconnect_count: AtomicU64::new(0),
            consecutive_failures: 0,
        }
    }

    /// Reset state after successful connection.
    pub fn reset(&mut self, config: &ReconnectConfig) {
        self.current_delay = config.initial_delay_secs;
        self.consecutive_failures = 0;
    }

    /// Record a reconnection attempt and get the delay for next attempt.
    /// Returns None if max retries exceeded.
    pub fn next_delay(&mut self, config: &ReconnectConfig) -> Option<u64> {
        self.consecutive_failures += 1;
        self.reconnect_count.fetch_add(1, Ordering::Relaxed);

        // Check if we've exceeded max retries
        if let Some(max) = config.max_retries {
            if self.consecutive_failures > max {
                return None;
            }
        }

        let delay = self.current_delay;

        // Exponential backoff with max limit
        self.current_delay = (self.current_delay * 2).min(config.max_delay_secs);

        Some(delay)
    }

    /// Get total reconnection count.
    pub fn reconnect_count(&self) -> u64 {
        self.reconnect_count.load(Ordering::Relaxed)
    }

    /// Get consecutive failures count.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Check if max retries have been exceeded.
    pub fn max_retries_exceeded(&self, config: &ReconnectConfig) -> bool {
        if let Some(max) = config.max_retries {
            self.consecutive_failures > max
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ReconnectConfig::default();
        assert_eq!(config.initial_delay_secs, 5);
        assert_eq!(config.max_delay_secs, 60);
        assert!(config.max_retries.is_none());
    }

    #[test]
    fn test_for_orders_config() {
        let config = ReconnectConfig::for_orders();
        assert_eq!(config.max_retries, Some(10));
    }

    #[test]
    fn test_exponential_backoff() {
        let config = ReconnectConfig::default();
        let mut state = ReconnectState::new(&config);

        // First failure: delay = 5
        let delay = state.next_delay(&config).unwrap();
        assert_eq!(delay, 5);

        // Second failure: delay = 10
        let delay = state.next_delay(&config).unwrap();
        assert_eq!(delay, 10);

        // Third failure: delay = 20
        let delay = state.next_delay(&config).unwrap();
        assert_eq!(delay, 20);

        // Fourth failure: delay = 40
        let delay = state.next_delay(&config).unwrap();
        assert_eq!(delay, 40);

        // Fifth failure: delay = 60 (capped at max)
        let delay = state.next_delay(&config).unwrap();
        assert_eq!(delay, 60);

        // Sixth failure: delay = 60 (still capped)
        let delay = state.next_delay(&config).unwrap();
        assert_eq!(delay, 60);
    }

    #[test]
    fn test_max_retries() {
        let config = ReconnectConfig::for_orders().with_max_retries(3);
        let mut state = ReconnectState::new(&config);

        // 3 retries should succeed
        assert!(state.next_delay(&config).is_some());
        assert!(state.next_delay(&config).is_some());
        assert!(state.next_delay(&config).is_some());

        // 4th should fail (exceeded max)
        assert!(state.next_delay(&config).is_none());
        assert!(state.max_retries_exceeded(&config));
    }

    #[test]
    fn test_reset_on_success() {
        let config = ReconnectConfig::default();
        let mut state = ReconnectState::new(&config);

        // Accumulate some failures
        state.next_delay(&config);
        state.next_delay(&config);
        assert_eq!(state.consecutive_failures(), 2);
        assert_eq!(state.current_delay, 20);

        // Reset on success
        state.reset(&config);
        assert_eq!(state.consecutive_failures(), 0);
        assert_eq!(state.current_delay, 5);

        // Reconnect count should persist
        assert_eq!(state.reconnect_count(), 2);
    }
}
