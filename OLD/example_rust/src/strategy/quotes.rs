//! Quote output and formatting.
//!
//! Provides quote structure and display formatting including
//! spread, bid/ask prices, and distances in basis points.

use crate::types::Symbol;
use crate::log_quote;
use std::time::Instant;

/// Maximum number of order levels supported (2 bids + 2 asks).
pub const MAX_ORDER_LEVELS: usize = 2;

/// A calculated market making quote.
#[derive(Debug, Clone)]
pub struct Quote {
    /// Symbol being quoted
    pub symbol: Symbol,
    /// Bid prices per level [level_0, level_1]
    pub bid_prices: [f64; MAX_ORDER_LEVELS],
    /// Ask prices per level [level_0, level_1]
    pub ask_prices: [f64; MAX_ORDER_LEVELS],
    /// Number of active levels (1 or 2)
    pub num_levels: usize,
    /// Order quantity in base asset (same qty per level)
    pub quantity: f64,
    /// Mid price (reference)
    pub mid_price: f64,
    /// Spread (ask - bid) for level 0
    pub spread: f64,
    /// Current volatility (per-second, in ticks)
    pub volatility: f64,
    /// Current alpha (imbalance z-score)
    pub alpha: f64,
    /// Current position in base asset
    pub position: f64,
    /// Half-spread in ticks (for level 0)
    pub half_spread_tick: f64,
    /// Whether this quote is valid for trading (has enough history)
    pub valid_for_trading: bool,
    /// History duration in seconds
    pub history_secs: f64,
    /// Whether bid depth was floored to minimum spread per level
    pub bid_floored: [bool; MAX_ORDER_LEVELS],
    /// Whether ask depth was floored to minimum spread per level
    pub ask_floored: [bool; MAX_ORDER_LEVELS],
}

impl Quote {
    /// Get bid price (for backward compatibility, returns level 0)
    #[inline]
    pub fn bid_price(&self) -> f64 {
        self.bid_prices[0]
    }

    /// Get ask price (for backward compatibility, returns level 0)
    #[inline]
    pub fn ask_price(&self) -> f64 {
        self.ask_prices[0]
    }
}

impl Quote {
    /// Calculate bid distance from mid in basis points (level 0).
    pub fn bid_bps(&self) -> f64 {
        self.bid_bps_at(0)
    }

    /// Calculate ask distance from mid in basis points (level 0).
    pub fn ask_bps(&self) -> f64 {
        self.ask_bps_at(0)
    }

    /// Calculate bid distance from mid in basis points for a specific level.
    pub fn bid_bps_at(&self, level: usize) -> f64 {
        if self.mid_price == 0.0 || level >= self.num_levels {
            return 0.0;
        }
        ((self.bid_prices[level] - self.mid_price) / self.mid_price) * 10000.0
    }

    /// Calculate ask distance from mid in basis points for a specific level.
    pub fn ask_bps_at(&self, level: usize) -> f64 {
        if self.mid_price == 0.0 || level >= self.num_levels {
            return 0.0;
        }
        ((self.ask_prices[level] - self.mid_price) / self.mid_price) * 10000.0
    }

    /// Calculate spread in basis points (level 0).
    pub fn spread_bps(&self) -> f64 {
        if self.mid_price == 0.0 {
            return 0.0;
        }
        (self.spread / self.mid_price) * 10000.0
    }

    /// Get half-spread in basis points (level 0).
    pub fn half_spread_bps(&self) -> f64 {
        self.spread_bps() / 2.0
    }
}

/// Quote formatter for display output with throttling.
pub struct QuoteFormatter {
    /// Price precision (decimal places)
    price_precision: usize,
    /// Quantity precision (decimal places)
    qty_precision: usize,
    /// Minimum interval between logs in milliseconds
    log_interval_ms: u64,
    /// Last time a quote was logged
    last_log_time: Option<Instant>,
}

impl Default for QuoteFormatter {
    fn default() -> Self {
        Self {
            price_precision: 2,
            qty_precision: 4,
            log_interval_ms: 1000, // Log at most once per second
            last_log_time: None,
        }
    }
}

impl QuoteFormatter {
    /// Create a new formatter with specified precision.
    pub fn new(price_precision: usize, qty_precision: usize) -> Self {
        Self {
            price_precision,
            qty_precision,
            log_interval_ms: 1000,
            last_log_time: None,
        }
    }

    /// Set the minimum interval between quote logs in milliseconds.
    pub fn with_log_interval(mut self, interval_ms: u64) -> Self {
        self.log_interval_ms = interval_ms;
        self
    }

    /// Format and log a quote (throttled to log_interval_ms).
    pub fn log_quote(&mut self, quote: &Quote) {
        // Check throttle
        let now = Instant::now();
        if let Some(last) = self.last_log_time {
            if now.duration_since(last).as_millis() < self.log_interval_ms as u128 {
                return; // Skip logging, too soon
            }
        }
        self.last_log_time = Some(now);

        // Trading status indicator
        let trade_status = if quote.valid_for_trading {
            "READY"
        } else {
            "WARM"
        };

        // Header line: symbol, mid, spread, volatility, alpha, status
        let levels_str = if quote.num_levels > 1 {
            format!(" ({}lvl)", quote.num_levels)
        } else {
            String::new()
        };
        log_quote!(
            "[{}] mid={:.prec$} spread={:.prec$} ({:.2}bps){} vol={:.4} alpha={:.3} [{} {:.0}s]",
            quote.symbol.as_str(),
            quote.mid_price,
            quote.spread,
            quote.spread_bps(),
            levels_str,
            quote.volatility,
            quote.alpha,
            trade_status,
            quote.history_secs,
            prec = self.price_precision
        );

        // Log each level
        for level in 0..quote.num_levels {
            let bid_floor_indicator = if quote.bid_floored[level] { " [FLOOR]" } else { "" };
            let ask_floor_indicator = if quote.ask_floored[level] { " [FLOOR]" } else { "" };
            let level_label = if quote.num_levels > 1 {
                format!("L{}", level)
            } else {
                "Quote".to_string()
            };
            log_quote!(
                "  {}: bid={:.prec$} ({:+.2}bps){} ask={:.prec$} ({:+.2}bps){} qty={:.qprec$}",
                level_label,
                quote.bid_prices[level],
                quote.bid_bps_at(level),
                bid_floor_indicator,
                quote.ask_prices[level],
                quote.ask_bps_at(level),
                ask_floor_indicator,
                quote.quantity,
                prec = self.price_precision,
                qprec = self.qty_precision
            );
        }

        // Position info if non-zero
        if quote.position.abs() > 1e-8 {
            log_quote!(
                "  Position: {:.qprec$} (${:.2})",
                quote.position,
                quote.position * quote.mid_price,
                qprec = self.qty_precision
            );
        }
    }

    /// Format quote as a single-line string (level 0 only for brevity).
    pub fn format_oneline(&self, quote: &Quote) -> String {
        format!(
            "[{}] bid={:.prec$}({:+.1}bp) ask={:.prec$}({:+.1}bp) spread={:.1}bp vol={:.3} alpha={:.2}",
            quote.symbol.as_str(),
            quote.bid_price(),
            quote.bid_bps(),
            quote.ask_price(),
            quote.ask_bps(),
            quote.spread_bps(),
            quote.volatility,
            quote.alpha,
            prec = self.price_precision
        )
    }

    /// Format quote as multi-line string.
    pub fn format_multiline(&self, quote: &Quote) -> String {
        let mut output = String::new();

        // Header
        let levels_str = if quote.num_levels > 1 {
            format!(" ({}lvl)", quote.num_levels)
        } else {
            String::new()
        };
        output.push_str(&format!(
            "[{}] mid={:.prec$} spread={:.prec$} ({:.2}bps){}\n",
            quote.symbol.as_str(),
            quote.mid_price,
            quote.spread,
            quote.spread_bps(),
            levels_str,
            prec = self.price_precision
        ));

        // Strategy metrics
        output.push_str(&format!(
            "  Metrics: vol={:.4} alpha={:.3} half_spread={:.2}ticks\n",
            quote.volatility,
            quote.alpha,
            quote.half_spread_tick
        ));

        // Quote prices for each level
        for level in 0..quote.num_levels {
            let level_label = if quote.num_levels > 1 {
                format!("L{}", level)
            } else {
                "Quote".to_string()
            };
            output.push_str(&format!(
                "  {}: bid={:.prec$} ({:+.2}bps) | ask={:.prec$} ({:+.2}bps)\n",
                level_label,
                quote.bid_prices[level],
                quote.bid_bps_at(level),
                quote.ask_prices[level],
                quote.ask_bps_at(level),
                prec = self.price_precision
            ));
        }

        // Quantity
        output.push_str(&format!(
            "  Size: {:.qprec$} (${:.2})\n",
            quote.quantity,
            quote.quantity * quote.mid_price,
            qprec = self.qty_precision
        ));

        // Position if non-zero
        if quote.position.abs() > 1e-8 {
            output.push_str(&format!(
                "  Position: {:.qprec$} (${:.2})\n",
                quote.position,
                quote.position * quote.mid_price,
                qprec = self.qty_precision
            ));
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_quote() -> Quote {
        Quote {
            symbol: Symbol::new("TEST-USD"),
            bid_prices: [99999.50, 99999.00],
            ask_prices: [100000.50, 100001.00],
            num_levels: 1,
            quantity: 0.001,
            mid_price: 100000.0,
            spread: 1.0,
            volatility: 0.0234,
            alpha: 0.15,
            position: 0.0,
            half_spread_tick: 50.0,
            valid_for_trading: true,
            history_secs: 600.0,
            bid_floored: [false, false],
            ask_floored: [false, false],
        }
    }

    fn sample_quote_2_levels() -> Quote {
        Quote {
            symbol: Symbol::new("TEST-USD"),
            bid_prices: [99999.50, 99999.00],
            ask_prices: [100000.50, 100001.00],
            num_levels: 2,
            quantity: 0.001,
            mid_price: 100000.0,
            spread: 1.0,
            volatility: 0.0234,
            alpha: 0.15,
            position: 0.0,
            half_spread_tick: 50.0,
            valid_for_trading: true,
            history_secs: 600.0,
            bid_floored: [false, true],
            ask_floored: [false, false],
        }
    }

    #[test]
    fn test_bid_bps() {
        let quote = sample_quote();
        // (99999.50 - 100000) / 100000 * 10000 = -0.05 bps
        assert!((quote.bid_bps() - (-0.05)).abs() < 0.001);
    }

    #[test]
    fn test_ask_bps() {
        let quote = sample_quote();
        // (100000.50 - 100000) / 100000 * 10000 = 0.05 bps
        assert!((quote.ask_bps() - 0.05).abs() < 0.001);
    }

    #[test]
    fn test_spread_bps() {
        let quote = sample_quote();
        // 1.0 / 100000 * 10000 = 0.1 bps
        assert!((quote.spread_bps() - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_multi_level_bps() {
        let quote = sample_quote_2_levels();
        // Level 0: (99999.50 - 100000) / 100000 * 10000 = -0.05 bps
        assert!((quote.bid_bps_at(0) - (-0.05)).abs() < 0.001);
        // Level 1: (99999.00 - 100000) / 100000 * 10000 = -0.10 bps
        assert!((quote.bid_bps_at(1) - (-0.10)).abs() < 0.001);
        // Level 0: (100000.50 - 100000) / 100000 * 10000 = 0.05 bps
        assert!((quote.ask_bps_at(0) - 0.05).abs() < 0.001);
        // Level 1: (100001.00 - 100000) / 100000 * 10000 = 0.10 bps
        assert!((quote.ask_bps_at(1) - 0.10).abs() < 0.001);
    }

    #[test]
    fn test_format_oneline() {
        let quote = sample_quote();
        let formatter = QuoteFormatter::default();
        let output = formatter.format_oneline(&quote);

        assert!(output.contains("TEST-USD"));
        assert!(output.contains("bid="));
        assert!(output.contains("ask="));
        assert!(output.contains("bp"));
    }

    #[test]
    fn test_format_multiline() {
        let quote = sample_quote();
        let formatter = QuoteFormatter::default();
        let output = formatter.format_multiline(&quote);

        assert!(output.contains("TEST-USD"));
        assert!(output.contains("mid="));
        assert!(output.contains("Quote:"));
        assert!(output.contains("Size:"));
    }

    #[test]
    fn test_format_multiline_2_levels() {
        let quote = sample_quote_2_levels();
        let formatter = QuoteFormatter::default();
        let output = formatter.format_multiline(&quote);

        assert!(output.contains("TEST-USD"));
        assert!(output.contains("(2lvl)"));
        assert!(output.contains("L0:"));
        assert!(output.contains("L1:"));
    }
}
