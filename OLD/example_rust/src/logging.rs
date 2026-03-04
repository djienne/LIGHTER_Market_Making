//! Centralized logging module with debug flag control.
//!
//! All logging in the application goes through this module. The `debug` flag
//! in config.json controls whether logs are written. When debug is OFF,
//! logging has zero cost (checked via atomic bool before any string formatting).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

/// Global logger instance.
static LOGGER: OnceLock<Logger> = OnceLock::new();

/// Centralized logger with atomic enable/disable flag.
pub struct Logger {
    enabled: AtomicBool,
}

impl Logger {
    /// Create a new logger.
    fn new(enabled: bool) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
        }
    }

    /// Check if logging is enabled (lock-free).
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Enable or disable logging.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }
}

/// Initialize the global logger.
///
/// Should be called once at startup with the debug flag from config.
pub fn init(enabled: bool) {
    LOGGER.get_or_init(|| Logger::new(enabled));

    // Also initialize tracing if enabled
    if enabled {
        use tracing_subscriber::{fmt, EnvFilter};

        // Use RUST_LOG env var if set, otherwise default to INFO level
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"));

        let _ = fmt::Subscriber::builder()
            .with_env_filter(env_filter)
            .with_target(false)
            .with_thread_ids(false)
            .with_file(false)
            .with_line_number(false)
            .compact()
            .try_init();
    }
}

/// Get the global logger.
#[inline]
pub fn logger() -> &'static Logger {
    LOGGER.get_or_init(|| Logger::new(true))
}

/// Check if logging is enabled (convenience function).
#[inline]
pub fn is_enabled() -> bool {
    logger().is_enabled()
}

/// Log an info message if logging is enabled.
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        if $crate::logging::is_enabled() {
            tracing::info!($($arg)*);
        }
    };
}

/// Log a debug message if logging is enabled.
#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        if $crate::logging::is_enabled() {
            tracing::debug!($($arg)*);
        }
    };
}

/// Log a warning message if logging is enabled.
#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        if $crate::logging::is_enabled() {
            tracing::warn!($($arg)*);
        }
    };
}

/// Log an error message if logging is enabled.
#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        if $crate::logging::is_enabled() {
            tracing::error!($($arg)*);
        }
    };
}

/// Log a quote (special formatting for market making quotes).
#[macro_export]
macro_rules! log_quote {
    ($($arg:tt)*) => {
        if $crate::logging::is_enabled() {
            tracing::info!($($arg)*);
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logger_enabled() {
        init(true);
        assert!(is_enabled());
    }

    #[test]
    fn test_logger_toggle() {
        init(true);
        logger().set_enabled(false);
        assert!(!is_enabled());
        logger().set_enabled(true);
        assert!(is_enabled());
    }
}
