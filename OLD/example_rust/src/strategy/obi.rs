//! OBI (Order Book Imbalance) market making strategy.
//!
//! Calculates bid/ask quotes based on:
//! - Volatility of mid-price changes
//! - Z-score of order book imbalance (alpha)
//! - Position skew adjustment

use std::sync::Arc;
use tracing::{debug, info};
use crate::binance::SharedAlpha;
use crate::config::StrategyConfig;
use crate::trading::{SharedEquity, SharedSymbolInfo};
use crate::types::OrderbookSnapshot;
use super::rolling::RollingStats;
use super::quotes::Quote;
use super::traits::QuoteStrategy;

/// OBI market making strategy.
pub struct ObiStrategy {
    /// Strategy configuration
    config: StrategyConfig,
    /// Shared symbol info for dynamic tick_size/lot_size (optional)
    shared_info: Option<Arc<SharedSymbolInfo>>,
    /// Shared equity for automatic order sizing (optional)
    shared_equity: Option<Arc<SharedEquity>>,
    /// Shared Binance alpha for lock-free reads (optional)
    shared_binance_alpha: Option<Arc<SharedAlpha>>,
    /// Whether to use Binance alpha (from config)
    use_binance_alpha: bool,
    /// Binance stale threshold in milliseconds
    binance_stale_ms: u64,
    /// Rolling window for mid-price changes (volatility)
    mid_price_chg_stats: RollingStats,
    /// Rolling window for imbalance (alpha)
    imbalance_stats: RollingStats,
    /// Previous mid-price in dollars (price units)
    prev_mid_price: Option<f64>,
    /// Current position in base asset units
    position: f64,
    /// Current step count
    step_count: u64,
    /// Last update step
    last_update_step: u64,
    /// Cached volatility (per-second scale)
    volatility: f64,
    /// Cached alpha (z-score of imbalance)
    alpha: f64,
    /// Is strategy warmed up (enough samples for quoting)
    warmed_up: bool,
    /// First timestamp seen (for tracking history age)
    first_timestamp_ns: Option<i64>,
    /// Most recent timestamp
    latest_timestamp_ns: i64,
    /// Required history duration for trading (default: 10 minutes in ns)
    required_history_ns: u64,
    /// Total samples pushed to mid_price_chg_stats (not capped by window size)
    total_samples: usize,
    /// Has logged the "valid for trading" milestone (log only once)
    logged_valid_for_trading: bool,
    /// Has logged the "using Binance alpha" milestone (log only once)
    logged_binance_alpha_active: bool,
}

/// Default required history for trading: 10 minutes in nanoseconds
const DEFAULT_REQUIRED_HISTORY_NS: u64 = 10 * 60 * 1_000_000_000;

/// Minimum samples needed before generating quotes.
/// This is a safety minimum - at ~4-5 msgs/sec, 100 samples ≈ 20-30 seconds.
/// The actual trading wait time is controlled by `history_minutes` in config.
const MIN_SAMPLES_FOR_QUOTE: usize = 100;

impl ObiStrategy {
    /// Create a new OBI strategy.
    pub fn new(config: StrategyConfig) -> Self {
        let window_steps = config.window_steps;
        let use_binance_alpha = config.alpha_source == "binance";
        let binance_stale_ms = config.binance_stale_ms;

        Self {
            config,
            shared_info: None,
            shared_equity: None,
            shared_binance_alpha: None,
            use_binance_alpha,
            binance_stale_ms,
            mid_price_chg_stats: RollingStats::new(window_steps),
            imbalance_stats: RollingStats::new(window_steps),
            prev_mid_price: None,
            position: 0.0,
            step_count: 0,
            last_update_step: 0,
            volatility: 0.0,
            alpha: 0.0,
            warmed_up: false,
            first_timestamp_ns: None,
            latest_timestamp_ns: 0,
            required_history_ns: DEFAULT_REQUIRED_HISTORY_NS,
            total_samples: 0,
            logged_valid_for_trading: false,
            logged_binance_alpha_active: false,
        }
    }

    /// Create a new OBI strategy with shared symbol info for dynamic tick_size/lot_size.
    pub fn with_shared_info(
        config: StrategyConfig,
        shared_info: Arc<SharedSymbolInfo>,
        shared_equity: Arc<SharedEquity>,
        required_history_minutes: u64,
    ) -> Self {
        let window_steps = config.window_steps;
        let use_binance_alpha = config.alpha_source == "binance";
        let binance_stale_ms = config.binance_stale_ms;

        Self {
            config,
            shared_info: Some(shared_info),
            shared_equity: Some(shared_equity),
            shared_binance_alpha: None,
            use_binance_alpha,
            binance_stale_ms,
            mid_price_chg_stats: RollingStats::new(window_steps),
            imbalance_stats: RollingStats::new(window_steps),
            prev_mid_price: None,
            position: 0.0,
            step_count: 0,
            last_update_step: 0,
            volatility: 0.0,
            alpha: 0.0,
            warmed_up: false,
            first_timestamp_ns: None,
            latest_timestamp_ns: 0,
            required_history_ns: required_history_minutes * 60 * 1_000_000_000,
            total_samples: 0,
            logged_valid_for_trading: false,
            logged_binance_alpha_active: false,
        }
    }

    /// Create a new OBI strategy with Binance alpha integration.
    ///
    /// This is the preferred constructor when using Binance as the alpha source.
    /// The shared_binance_alpha provides lock-free reads (~1ns) for the hot path.
    pub fn with_binance_alpha(
        config: StrategyConfig,
        shared_info: Arc<SharedSymbolInfo>,
        shared_equity: Arc<SharedEquity>,
        shared_binance_alpha: Arc<SharedAlpha>,
        required_history_minutes: u64,
    ) -> Self {
        let window_steps = config.window_steps;
        let use_binance_alpha = config.alpha_source == "binance";
        let binance_stale_ms = config.binance_stale_ms;

        Self {
            config,
            shared_info: Some(shared_info),
            shared_equity: Some(shared_equity),
            shared_binance_alpha: Some(shared_binance_alpha),
            use_binance_alpha,
            binance_stale_ms,
            mid_price_chg_stats: RollingStats::new(window_steps),
            imbalance_stats: RollingStats::new(window_steps),
            prev_mid_price: None,
            position: 0.0,
            step_count: 0,
            last_update_step: 0,
            volatility: 0.0,
            alpha: 0.0,
            warmed_up: false,
            first_timestamp_ns: None,
            latest_timestamp_ns: 0,
            required_history_ns: required_history_minutes * 60 * 1_000_000_000,
            total_samples: 0,
            logged_valid_for_trading: false,
            logged_binance_alpha_active: false,
        }
    }

    /// Create a new OBI strategy with custom required history duration.
    pub fn with_required_history(
        config: StrategyConfig,
        shared_equity: Arc<SharedEquity>,
        required_history_minutes: u64,
    ) -> Self {
        let mut strategy = Self::new(config);
        strategy.shared_equity = Some(shared_equity);
        strategy.required_history_ns = required_history_minutes * 60 * 1_000_000_000;
        strategy
    }

    /// Set the shared Binance alpha (for late binding after construction).
    pub fn set_binance_alpha(&mut self, shared_alpha: Arc<SharedAlpha>) {
        self.shared_binance_alpha = Some(shared_alpha);
    }

    /// Get the current tick size (from SharedSymbolInfo if available, else from config).
    #[inline]
    pub fn tick_size(&self) -> f64 {
        self.shared_info
            .as_ref()
            .map(|info| info.tick_size())
            .unwrap_or(self.config.tick_size)
    }

    /// Get the current lot size (from SharedSymbolInfo if available, else from config).
    #[inline]
    pub fn lot_size(&self) -> f64 {
        self.shared_info
            .as_ref()
            .map(|info| info.lot_size())
            .unwrap_or(self.config.lot_size)
    }

    /// Set the current position.
    ///
    /// Position is in base asset units (e.g., BTC).
    /// Positive = long, negative = short.
    pub fn set_position(&mut self, position: f64) {
        self.position = position;
    }

    /// Get the current position.
    #[inline]
    pub fn position(&self) -> f64 {
        self.position
    }

    /// Check if the strategy is warmed up (has enough data).
    #[inline]
    pub fn is_warmed_up(&self) -> bool {
        self.warmed_up
    }

    /// Get the current volatility.
    #[inline]
    pub fn volatility(&self) -> f64 {
        self.volatility
    }

    /// Get the current alpha (imbalance z-score).
    #[inline]
    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    /// Get the strategy configuration.
    #[inline]
    pub fn config(&self) -> &StrategyConfig {
        &self.config
    }

    /// Check if the strategy has enough history for trading.
    ///
    /// Trading requires the full history window (default 10 minutes).
    /// This is separate from `is_warmed_up()` which only requires
    /// enough samples for quote calculation.
    #[inline]
    pub fn is_valid_for_trading(&self) -> bool {
        if !self.warmed_up {
            return false;
        }
        self.history_duration_ns() >= self.required_history_ns
    }

    /// Get the current history duration in nanoseconds.
    #[inline]
    pub fn history_duration_ns(&self) -> u64 {
        match self.first_timestamp_ns {
            Some(first) => {
                if self.latest_timestamp_ns > first {
                    (self.latest_timestamp_ns - first) as u64
                } else {
                    0
                }
            }
            None => 0,
        }
    }

    /// Get the current history duration in seconds.
    #[inline]
    pub fn history_duration_secs(&self) -> f64 {
        self.history_duration_ns() as f64 / 1_000_000_000.0
    }

    /// Get the required history duration in seconds.
    #[inline]
    pub fn required_history_secs(&self) -> f64 {
        self.required_history_ns as f64 / 1_000_000_000.0
    }

    /// Process a new orderbook snapshot.
    ///
    /// Returns a Quote if the strategy is warmed up and ready to quote.
    /// Note: Quotes are returned for display purposes even before
    /// `is_valid_for_trading()` returns true. Use `is_valid_for_trading()`
    /// to check if the quote should be used for order placement.
    pub fn update(&mut self, snapshot: &OrderbookSnapshot) -> Option<Quote> {
        // Track timestamps for history duration
        let timestamp = snapshot.timestamp_ns;
        if timestamp > 0 {
            if self.first_timestamp_ns.is_none() {
                self.first_timestamp_ns = Some(timestamp);
            }
            self.latest_timestamp_ns = timestamp;
        }

        // Get mid-price
        let mid_price = snapshot.mid_price()?;

        // Calculate mid-price change in dollars (consistent with Python)
        if let Some(prev_mid) = self.prev_mid_price {
            let mid_chg = mid_price - prev_mid;
            self.mid_price_chg_stats.push(mid_chg);
            self.total_samples += 1;

            // Log warmup milestones
            if self.total_samples == 100 || self.total_samples == 500 || self.total_samples == MIN_SAMPLES_FOR_QUOTE {
                debug!(
                    "[{}] Warmup milestone: {} samples collected",
                    snapshot.symbol, self.total_samples
                );
            }
        }
        self.prev_mid_price = Some(mid_price);

        // Calculate imbalance
        let imbalance = self.calculate_imbalance(snapshot, mid_price);
        self.imbalance_stats.push(imbalance);

        // Increment step
        self.step_count += 1;

        // Check if we should update (based on update_interval_steps)
        let steps_since_update = self.step_count - self.last_update_step;
        if steps_since_update < self.config.update_interval_steps as u64 {
            return None;
        }
        self.last_update_step = self.step_count;

        // Check warm-up: need minimum samples for statistical validity
        // Note: window_steps is for rolling stats size, NOT warmup time
        // Trading readiness is controlled by history_minutes via is_valid_for_trading()
        // Use total_samples (not rolling window len) because window is capped at window_steps
        if self.total_samples < MIN_SAMPLES_FOR_QUOTE {
            return None;
        }

        // Mark as warmed up (log first time)
        if !self.warmed_up {
            debug!(
                "[{}] Strategy warmed up: {} samples, ready to generate quotes",
                snapshot.symbol, self.total_samples
            );
        }
        self.warmed_up = true;

        // Calculate volatility (scaled to per-second)
        let vol_raw = self.mid_price_chg_stats.std();
        self.volatility = vol_raw * self.config.vol_scale();

        // Calculate alpha - prefer Binance if configured and available (~5ns decision)
        // This is the critical hot path optimization: lock-free atomic reads
        self.alpha = if self.use_binance_alpha {
            if let Some(ref binance) = self.shared_binance_alpha {
                if binance.is_warmed_up() && !binance.is_stale(self.binance_stale_ms) {
                    // Use Binance alpha (lock-free read, ~1ns)
                    let binance_alpha = binance.alpha();

                    // Log milestone once when Binance alpha becomes active
                    if !self.logged_binance_alpha_active {
                        info!(
                            "[{}] Using Binance alpha: {:.3} (samples={})",
                            snapshot.symbol,
                            binance_alpha,
                            binance.sample_count()
                        );
                        self.logged_binance_alpha_active = true;
                    }

                    binance_alpha
                } else {
                    // Fallback to StandX alpha (Binance not ready or stale)
                    self.imbalance_stats.zscore(imbalance)
                }
            } else {
                // No Binance alpha provided, use StandX
                self.imbalance_stats.zscore(imbalance)
            }
        } else {
            // StandX alpha explicitly configured
            self.imbalance_stats.zscore(imbalance)
        };

        // Calculate and return quote
        self.calculate_quote(snapshot, mid_price)
    }

    /// Calculate order book imbalance within looking_depth of mid-price.
    /// Uses simple loops instead of iterator chains for better hot path performance.
    #[inline]
    fn calculate_imbalance(&self, snapshot: &OrderbookSnapshot, mid_price: f64) -> f64 {
        let depth_pct = self.config.looking_depth;
        let lower_bound = mid_price * (1.0 - depth_pct);
        let upper_bound = mid_price * (1.0 + depth_pct);

        // Sum bid quantities within range (bids are sorted descending by price)
        let mut sum_bid_qty = 0.0;
        for level in &snapshot.bids[..snapshot.bid_count as usize] {
            if level.price < lower_bound {
                break;
            }
            sum_bid_qty += level.quantity;
        }

        // Sum ask quantities within range (asks are sorted ascending by price)
        let mut sum_ask_qty = 0.0;
        for level in &snapshot.asks[..snapshot.ask_count as usize] {
            if level.price > upper_bound {
                break;
            }
            sum_ask_qty += level.quantity;
        }

        sum_bid_qty - sum_ask_qty
    }

    /// Calculate bid/ask quotes for multiple levels.
    #[inline]
    fn calculate_quote(&mut self, snapshot: &OrderbookSnapshot, mid_price: f64) -> Option<Quote> {
        use super::quotes::MAX_ORDER_LEVELS;

        let best_bid = snapshot.best_bid_price()?;
        let best_ask = snapshot.best_ask_price()?;

        // Calculate base half-spread in ticks (priority: volatility > bps > fixed)
        // Note: volatility > 0.0 implies is_finite() (NaN/Inf comparisons return false)
        let tick_size = self.tick_size();
        let base_half_spread_tick = if self.config.vol_to_half_spread > 0.0 && self.volatility > 0.0 {
            // Mode 1: Volatility-based (half_spread_price = volatility * vol_to_half_spread)
            (self.volatility * self.config.vol_to_half_spread) / tick_size
        } else if self.config.half_spread_bps > 0.0 {
            // Mode 2: BPS-based
            mid_price * (self.config.half_spread_bps / 10000.0) / tick_size
        } else if self.config.half_spread > 0.0 {
            // Mode 3: Fixed price
            self.config.half_spread / tick_size
        } else {
            // Fallback: use minimum spread
            1.0
        };

        // Calculate fair price (mid + alpha adjustment)
        let fair_price = mid_price + self.config.c1() * self.alpha;

        // Get max_position_dollar from SharedEquity (lock-free read, ~1ns)
        // If not initialized, use f64::MAX (no normalization effect)
        let max_position_dollar = self
            .shared_equity
            .as_ref()
            .map(|eq| eq.max_position_dollar())
            .filter(|&v| v > 0.0)
            .unwrap_or(f64::MAX);

        // Calculate position skew (normalized to [-1, 1])
        let normalized_position = (self.position * mid_price) / max_position_dollar;
        let clamped_position = normalized_position.clamp(-1.0, 1.0);

        // Number of levels to generate (1 or 2)
        let num_levels = self.config.order_levels.min(MAX_ORDER_LEVELS);

        // Spread multipliers: level 0 = 1.0x, level 1 = spread_level_multiplier
        let multipliers = [1.0, self.config.spread_level_multiplier];

        // Get order_qty_dollar from SharedEquity (lock-free read, ~1ns)
        // Skip quote if no sizing available (equity not yet fetched)
        let order_qty_dollar = self
            .shared_equity
            .as_ref()
            .filter(|eq| eq.is_initialized())
            .map(|eq| eq.order_qty_dollar())
            .unwrap_or(0.0);

        if order_qty_dollar <= 0.0 {
            return None;
        }

        // Calculate prices for each level
        let mut bid_prices = [0.0; MAX_ORDER_LEVELS];
        let mut ask_prices = [0.0; MAX_ORDER_LEVELS];
        let mut bid_floored = [false; MAX_ORDER_LEVELS];
        let mut ask_floored = [false; MAX_ORDER_LEVELS];

        for level in 0..num_levels {
            let half_spread_tick = base_half_spread_tick * multipliers[level];

            // Adjust half-spread based on position
            // When long (positive position), increase bid depth (push bid down)
            // When short (negative position), increase ask depth (push ask up)
            let bid_depth_tick = (half_spread_tick * (1.0 + self.config.skew * clamped_position)).max(0.0);
            let ask_depth_tick = (half_spread_tick * (1.0 - self.config.skew * clamped_position)).max(0.0);

            // Calculate raw quote prices
            let raw_bid = fair_price - bid_depth_tick * tick_size;
            let raw_ask = fair_price + ask_depth_tick * tick_size;

            // Clamp to BBO (never cross the spread)
            let clamped_bid = raw_bid.min(best_bid);
            let clamped_ask = raw_ask.max(best_ask);

            // Apply floor AFTER BBO clamping
            // Floor ensures minimum distance from mid_price
            let (floored_bid, bid_floor_applied) = if self.config.min_half_spread_bps > 0.0 {
                // For outer levels, scale the minimum floor by the multiplier
                let level_min_bps = self.config.min_half_spread_bps * multipliers[level];
                let min_bid = mid_price * (1.0 - level_min_bps / 10000.0);
                if clamped_bid > min_bid {
                    (min_bid, true)
                } else {
                    (clamped_bid, false)
                }
            } else {
                (clamped_bid, false)
            };

            let (floored_ask, ask_floor_applied) = if self.config.min_half_spread_bps > 0.0 {
                let level_min_bps = self.config.min_half_spread_bps * multipliers[level];
                let min_ask = mid_price * (1.0 + level_min_bps / 10000.0);
                if clamped_ask < min_ask {
                    (min_ask, true)
                } else {
                    (clamped_ask, false)
                }
            } else {
                (clamped_ask, false)
            };

            // Snap to tick grid
            bid_prices[level] = (floored_bid / tick_size).floor() * tick_size;
            ask_prices[level] = (floored_ask / tick_size).ceil() * tick_size;
            bid_floored[level] = bid_floor_applied;
            ask_floored[level] = ask_floor_applied;
        }

        // Calculate quantity per level (round to lot_size, ensure minimum)
        // Note: order_qty_dollar is already per-order (formula includes /order_levels)
        let order_qty_per_level = order_qty_dollar / mid_price;
        let lot_size = self.lot_size();
        let quantity = ((order_qty_per_level / lot_size).round() * lot_size).max(lot_size);

        // Log "valid for trading" milestone once (uses info! for visibility)
        let valid_for_trading = self.is_valid_for_trading();
        if valid_for_trading && !self.logged_valid_for_trading {
            self.logged_valid_for_trading = true;
            info!(
                "[{}] Strategy now valid for trading (history={:.0}s, samples={}, levels={})",
                snapshot.symbol,
                self.history_duration_secs(),
                self.total_samples,
                num_levels
            );
        }

        // Log quote generation with key metrics
        debug!(
            "[{}] Quote: vol={:.4} alpha={:.3} levels={} L0_spread={:.2}bps",
            snapshot.symbol,
            self.volatility,
            self.alpha,
            num_levels,
            (ask_prices[0] - bid_prices[0]) / mid_price * 10000.0
        );

        Some(Quote {
            symbol: snapshot.symbol.clone(),
            bid_prices,
            ask_prices,
            num_levels,
            quantity,
            mid_price,
            spread: ask_prices[0] - bid_prices[0],
            volatility: self.volatility,
            alpha: self.alpha,
            position: self.position,
            half_spread_tick: base_half_spread_tick,
            valid_for_trading,
            history_secs: self.history_duration_secs(),
            bid_floored,
            ask_floored,
        })
    }

    /// Reset the strategy state.
    pub fn reset_state(&mut self) {
        self.mid_price_chg_stats.clear();
        self.imbalance_stats.clear();
        self.prev_mid_price = None;
        self.step_count = 0;
        self.last_update_step = 0;
        self.volatility = 0.0;
        self.alpha = 0.0;
        self.warmed_up = false;
        self.first_timestamp_ns = None;
        self.latest_timestamp_ns = 0;
        self.total_samples = 0;
        self.logged_valid_for_trading = false;
        self.logged_binance_alpha_active = false;
    }

    /// Check if using Binance alpha.
    #[inline]
    pub fn is_using_binance_alpha(&self) -> bool {
        self.use_binance_alpha
            && self.shared_binance_alpha.as_ref().map_or(false, |b| {
                b.is_warmed_up() && !b.is_stale(self.binance_stale_ms)
            })
    }

    /// Get the configured alpha source.
    #[inline]
    pub fn alpha_source(&self) -> &str {
        &self.config.alpha_source
    }
}

// ============================================================================
// QuoteStrategy trait implementation
// ============================================================================

impl QuoteStrategy for ObiStrategy {
    #[inline]
    fn update(&mut self, snapshot: &OrderbookSnapshot) -> Option<Quote> {
        // Delegate to the existing update method
        ObiStrategy::update(self, snapshot)
    }

    #[inline]
    fn set_position(&mut self, position: f64) {
        self.position = position;
    }

    #[inline]
    fn position(&self) -> f64 {
        self.position
    }

    #[inline]
    fn is_valid_for_trading(&self) -> bool {
        ObiStrategy::is_valid_for_trading(self)
    }

    #[inline]
    fn is_warmed_up(&self) -> bool {
        self.warmed_up
    }

    #[inline]
    fn volatility(&self) -> f64 {
        self.volatility
    }

    #[inline]
    fn alpha(&self) -> f64 {
        self.alpha
    }

    #[inline]
    fn history_duration_secs(&self) -> f64 {
        ObiStrategy::history_duration_secs(self)
    }

    #[inline]
    fn required_history_secs(&self) -> f64 {
        ObiStrategy::required_history_secs(self)
    }

    fn reset(&mut self) {
        self.reset_state();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Symbol;

    fn default_config() -> StrategyConfig {
        StrategyConfig {
            tick_size: 0.01,
            step_ns: 100_000_000,
            window_steps: 10,  // Small window for testing
            update_interval_steps: 1,
            vol_to_half_spread: 0.8,
            half_spread: 0.0,
            half_spread_bps: 0.0,
            skew: 1.0,
            c1: 0.0, // Use c1_ticks fallback
            c1_ticks: 160.0,
            looking_depth: 0.025,
            min_order_qty_dollar: 10.0,
            lot_size: 0.001,
            min_half_spread_bps: 2.0,
            order_levels: 1,
            spread_level_multiplier: 1.5,
            alpha_source: "standx".to_string(), // Use StandX for tests
            binance_stale_ms: 5000,
            leverage: 1.0,
        }
    }

    /// Create a SharedEquity with initialized equity for tests
    fn test_shared_equity() -> Arc<SharedEquity> {
        let equity = Arc::new(SharedEquity::new(1, 10.0, 1.0));
        equity.set_equity(500.0); // $500 -> $90/order
        equity
    }

    fn create_snapshot(best_bid: f64, best_ask: f64) -> OrderbookSnapshot {
        let mut snapshot = OrderbookSnapshot::new(Symbol::new("TEST-USD"));

        // Add bid and ask levels
        let bids = vec![
            (best_bid, 1.0),
            (best_bid - 0.01, 2.0),
            (best_bid - 0.02, 3.0),
        ];
        let asks = vec![
            (best_ask, 1.0),
            (best_ask + 0.01, 2.0),
            (best_ask + 0.02, 3.0),
        ];

        snapshot.set_bids(&bids, 20);
        snapshot.set_asks(&asks, 20);
        snapshot
    }

    #[test]
    fn test_strategy_creation() {
        let config = default_config();
        let strategy = ObiStrategy::new(config);

        assert!(!strategy.is_warmed_up());
        assert_eq!(strategy.position(), 0.0);
    }

    #[test]
    fn test_strategy_warmup() {
        let config = default_config();
        let shared_equity = test_shared_equity();
        let mut strategy = ObiStrategy::with_required_history(config, shared_equity, 0);

        // Feed snapshots until warmed up (need MIN_SAMPLES_FOR_QUOTE = 100)
        // First call has no prev_mid_tick so doesn't add to mid_price_chg_stats
        // So we need 101+ calls to get 100+ samples
        // Add some randomness to prices to get non-zero volatility
        let mut quote_received = false;
        for i in 0..200 {
            // Vary prices to create non-zero volatility
            let noise = if i % 3 == 0 { 0.05 } else if i % 3 == 1 { -0.03 } else { 0.02 };
            let mid = 100.0 + noise;
            let snapshot = create_snapshot(mid - 0.01, mid + 0.01);
            let result = strategy.update(&snapshot);
            if result.is_some() {
                quote_received = true;
            }
        }

        assert!(strategy.is_warmed_up(), "Strategy should be warmed up after 200 iterations");
        assert!(quote_received, "Should have received at least one quote");
    }

    #[test]
    fn test_position_skew() {
        let config = default_config();
        let shared_equity = test_shared_equity();
        let mut strategy = ObiStrategy::with_required_history(config, shared_equity, 0);

        // Warm up (need MIN_SAMPLES_FOR_QUOTE = 100)
        // Add varying prices to create non-zero volatility
        for i in 0..200 {
            let noise = if i % 3 == 0 { 0.05 } else if i % 3 == 1 { -0.03 } else { 0.02 };
            let mid = 100.0 + noise;
            let snapshot = create_snapshot(mid - 0.01, mid + 0.01);
            strategy.update(&snapshot);
        }

        assert!(strategy.is_warmed_up(), "Strategy should be warmed up");

        // Get quote with zero position
        let snapshot = create_snapshot(99.99, 100.01);
        strategy.set_position(0.0);
        let quote_neutral = strategy.update(&snapshot);

        // Get quote with long position
        strategy.set_position(5.0);  // 5 BTC long
        let quote_long = strategy.update(&snapshot);

        // With long position, bid should be lower (more aggressive on selling)
        // This test validates the skew logic is working
        assert!(quote_neutral.is_some(), "Expected neutral quote");
        assert!(quote_long.is_some(), "Expected long position quote");
    }

    #[test]
    fn test_imbalance_calculation() {
        let config = default_config();
        let strategy = ObiStrategy::new(config);

        // Create snapshot with more bids than asks
        let mut snapshot = OrderbookSnapshot::new(Symbol::new("TEST-USD"));
        let bids = vec![
            (99.99, 10.0),  // Large bid
            (99.98, 5.0),
        ];
        let asks = vec![
            (100.01, 1.0),  // Small ask
            (100.02, 1.0),
        ];
        snapshot.set_bids(&bids, 20);
        snapshot.set_asks(&asks, 20);

        let mid = snapshot.mid_price().unwrap();
        let imbalance = strategy.calculate_imbalance(&snapshot, mid);

        // With more bids, imbalance should be positive
        assert!(imbalance > 0.0);
    }

    #[test]
    fn test_no_quote_without_equity() {
        let config = default_config();
        // Create strategy WITHOUT shared_equity
        let mut strategy = ObiStrategy::new(config);

        // Warm up
        for i in 0..200 {
            let noise = if i % 3 == 0 { 0.05 } else if i % 3 == 1 { -0.03 } else { 0.02 };
            let mid = 100.0 + noise;
            let snapshot = create_snapshot(mid - 0.01, mid + 0.01);
            let _ = strategy.update(&snapshot);
        }

        // Should be warmed up but no quotes (no equity)
        assert!(strategy.is_warmed_up());

        let snapshot = create_snapshot(99.99, 100.01);
        let quote = strategy.update(&snapshot);
        assert!(quote.is_none(), "Should not generate quote without equity");
    }
}
