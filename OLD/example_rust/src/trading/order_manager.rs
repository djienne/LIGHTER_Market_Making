//! Low-latency order manager for market making.
//!
//! This module provides synchronous order decision-making with
//! fire-and-forget async execution for minimal latency impact.
//!
//! # Architecture
//!
//! ```text
//! OrderManager.on_quote()  ──►  Vec<OrderDecision>  ──►  mpsc channel
//!       (SYNC, <10µs)                                      (async executor)
//! ```
//!
//! - Hot path (`on_quote`) is synchronous and lock-free
//! - Order execution happens asynchronously via channel
//! - State machine tracks orders: Pending → Live → Canceling
//! - Supports up to 2 order levels per side (4 orders total)

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{debug, error, info, warn};

use crate::strategy::Quote;
use crate::trading::{SharedEquity, SharedPosition};

/// Maximum number of order levels supported (2 bids + 2 asks).
/// Must match MAX_ORDER_LEVELS in strategy/quotes.rs
pub const MAX_ORDER_LEVELS: usize = 2;

/// Order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "buy"),
            Side::Sell => write!(f, "sell"),
        }
    }
}

/// Order state in the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    /// Order sent, awaiting confirmation from exchange.
    Pending,
    /// Order confirmed and live on the exchange.
    Live,
    /// Cancel request sent, awaiting confirmation.
    Canceling,
}

/// Tracked live order (one per level per side per symbol).
#[derive(Debug, Clone)]
pub struct LiveOrder {
    /// Client order ID (used for cancellation).
    pub cl_ord_id: String,
    /// Exchange order ID (set on acceptance).
    pub order_id: Option<i64>,
    /// Order side.
    pub side: Side,
    /// Order level (0 = inner, 1 = outer).
    pub level: usize,
    /// Order price.
    pub price: f64,
    /// Order quantity.
    pub quantity: f64,
    /// Current state in the state machine.
    pub state: OrderState,
    /// Timestamp when order was sent (nanoseconds).
    pub sent_at_ns: i64,
    /// Consecutive cancel failure count (for detecting stuck orders).
    pub cancel_fail_count: u32,
}

/// Order decision from the hot path.
#[derive(Debug, Clone)]
pub enum OrderDecision {
    /// No action needed.
    NoAction,
    /// Send a new order.
    Send {
        side: Side,
        level: usize,
        price: f64,
        qty: f64,
        cl_ord_id: String,
    },
    /// Cancel an existing order.
    Cancel {
        cl_ord_id: String,
    },
    /// Cancel existing order and prepare for replacement.
    /// Note: New order will be sent on next quote cycle after cancel confirms.
    CancelAndReplace {
        cancel_id: String,
        level: usize,
        new_price: f64,
        qty: f64,
    },
}

/// Order manager statistics.
#[derive(Debug, Default, Clone)]
pub struct OrderManagerStats {
    /// Total orders sent.
    pub orders_sent: u64,
    /// Orders accepted by exchange.
    pub orders_accepted: u64,
    /// Orders canceled.
    pub orders_canceled: u64,
    /// Orders rejected.
    pub rejections: u64,
    /// Reprice events (cancel + replace).
    pub reprices: u64,
    /// Timeouts (pending orders that timed out).
    pub timeouts: u64,
}

/// Order manager configuration.
#[derive(Debug, Clone)]
pub struct OrderManagerConfig {
    /// Symbol being managed.
    pub symbol: String,
    /// Reprice threshold in basis points (default: 1.0).
    pub reprice_threshold_bps: f64,
    /// Order timeout in nanoseconds (applies to Pending and Canceling states).
    pub pending_timeout_ns: u64,
    /// Maximum age for Live orders in nanoseconds before forcing refresh.
    /// Even if price is within reprice threshold, orders older than this are repriced.
    /// Set to 0 to disable (no max age for Live orders).
    pub max_live_age_ns: u64,
    /// Tick size for price snapping.
    pub tick_size: f64,
    /// Lot size for quantity.
    pub lot_size: f64,
    /// Enable debug logging for order state tracking.
    pub debug: bool,
    /// Maximum consecutive rejections before pausing trading (circuit breaker).
    /// Set to 0 to disable.
    pub circuit_breaker_rejections: u32,
    /// Number of order levels per side (1 or 2).
    pub num_levels: usize,
}

impl Default for OrderManagerConfig {
    fn default() -> Self {
        Self {
            symbol: "TEST-USD".to_string(),
            reprice_threshold_bps: 1.0,
            pending_timeout_ns: 5_000_000_000, // 5 seconds
            max_live_age_ns: 60_000_000_000,   // 60 seconds max age for Live orders
            tick_size: 0.01,
            lot_size: 0.001,
            debug: false,
            circuit_breaker_rejections: 5,     // Pause after 5 consecutive rejections
            num_levels: 1,                     // Default to single level
        }
    }
}

/// Timeout check interval in nanoseconds (1 second).
/// This limits how often we check for timed out orders to minimize hot path latency.
const TIMEOUT_CHECK_INTERVAL_NS: i64 = 1_000_000_000;

/// Low-latency order manager for market making.
///
/// Provides synchronous order decision-making in the hot path with
/// fire-and-forget async execution.
///
/// Supports up to MAX_ORDER_LEVELS (2) per side for multi-level market making.
pub struct OrderManager {
    /// Configuration.
    config: OrderManagerConfig,

    /// Current bid orders per level (buy).
    bid_orders: [Option<LiveOrder>; MAX_ORDER_LEVELS],

    /// Current ask orders per level (sell).
    ask_orders: [Option<LiveOrder>; MAX_ORDER_LEVELS],

    /// Shared position from poller (lock-free reads).
    position: Arc<SharedPosition>,

    /// Shared equity for max_position_dollar (lock-free reads, ~1ns).
    shared_equity: Arc<SharedEquity>,

    /// Client order ID counter.
    cl_ord_id_counter: AtomicU64,

    /// Session prefix for client order IDs.
    session_prefix: String,

    /// Statistics.
    stats: OrderManagerStats,

    /// Shutdown flag.
    shutdown: AtomicBool,

    /// Paused flag (for WebSocket disconnection).
    /// When paused, no new orders are generated.
    paused: AtomicBool,

    /// Last time we checked for timeouts (throttles hot path).
    last_timeout_check_ns: i64,

    /// Pending bid prices after cancel confirms per level (for CancelAndReplace).
    /// Stores (price, qty) to place immediately when slot is cleared.
    pending_bid_prices: [Option<(f64, f64)>; MAX_ORDER_LEVELS],

    /// Pending ask prices after cancel confirms per level (for CancelAndReplace).
    /// Stores (price, qty) to place immediately when slot is cleared.
    pending_ask_prices: [Option<(f64, f64)>; MAX_ORDER_LEVELS],

    /// Consecutive rejection count for circuit breaker.
    consecutive_rejections: u32,

    /// Timestamp when circuit breaker was triggered (Unix millis, 0 if not triggered).
    circuit_breaker_triggered_at_ms: u64,

    /// Timestamp when safety pause was triggered (Unix millis, 0 if not triggered).
    /// Safety pause is triggered when orders exceed expected count (duplicate/stuck state).
    safety_pause_triggered_at_ms: u64,

    /// Timestamp when last order was sent (Unix millis, 0 if never).
    last_order_sent_at_ms: u64,
}

impl OrderManager {
    /// Create a new order manager.
    pub fn new(
        config: OrderManagerConfig,
        position: Arc<SharedPosition>,
        shared_equity: Arc<SharedEquity>,
    ) -> Self {
        // Generate session prefix including symbol for O(1) lookup by symbol in event handlers
        // Format: mm_<symbol>_<timestamp>
        let session_prefix = format!("mm_{}_{}", config.symbol, chrono::Utc::now().timestamp_millis() % 1_000_000);

        Self {
            config,
            bid_orders: [None, None],
            ask_orders: [None, None],
            position,
            shared_equity,
            cl_ord_id_counter: AtomicU64::new(1),
            session_prefix,
            stats: OrderManagerStats::default(),
            shutdown: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            last_timeout_check_ns: 0,
            pending_bid_prices: [None, None],
            pending_ask_prices: [None, None],
            consecutive_rejections: 0,
            circuit_breaker_triggered_at_ms: 0,
            safety_pause_triggered_at_ms: 0,
            last_order_sent_at_ms: 0,
        }
    }

    /// Get the configured number of levels.
    #[inline]
    pub fn num_levels(&self) -> usize {
        self.config.num_levels
    }

    /// Get the symbol being managed.
    pub fn symbol(&self) -> &str {
        &self.config.symbol
    }

    /// Get current statistics.
    pub fn stats(&self) -> &OrderManagerStats {
        &self.stats
    }

    /// Generate a unique client order ID with level.
    /// Format: mm_<symbol>_<timestamp>_<level>_<seq>
    #[inline]
    pub fn generate_cl_ord_id(&self, level: usize) -> String {
        let seq = self.cl_ord_id_counter.fetch_add(1, Ordering::Relaxed);
        format!("{}_{}_{}", self.session_prefix, level, seq)
    }

    /// Extract symbol from a client order ID.
    /// Format: mm_<symbol>_<timestamp>_<level>_<seq>
    /// Returns None if format doesn't match.
    #[inline]
    pub fn extract_symbol_from_cl_ord_id(cl_ord_id: &str) -> Option<&str> {
        // Format: mm_<symbol>_<timestamp>_<level>_<seq>
        // Skip "mm_", find symbol between first and second "_" after "mm_"
        let rest = cl_ord_id.strip_prefix("mm_")?;
        // Find the first underscore (after symbol)
        let underscore_pos = rest.find('_')?;
        Some(&rest[..underscore_pos])
    }

    /// Extract level from a client order ID.
    /// Format: mm_<symbol>_<timestamp>_<level>_<seq>
    /// Returns None if format doesn't match.
    #[inline]
    pub fn extract_level_from_cl_ord_id(cl_ord_id: &str) -> Option<usize> {
        // Format: mm_<symbol>_<timestamp>_<level>_<seq>
        let rest = cl_ord_id.strip_prefix("mm_")?;
        let parts: Vec<&str> = rest.split('_').collect();
        // parts: [symbol, timestamp, level, seq]
        if parts.len() >= 4 {
            parts[2].parse().ok()
        } else {
            // Backward compatibility: old format without level = level 0
            Some(0)
        }
    }

    /// Check for timed out pending orders and return cancel decisions.
    ///
    /// This should be called on EVERY orderbook update, not just when quotes
    /// are generated. This ensures pending orders are cleaned up even when
    /// the strategy isn't producing quotes.
    ///
    /// To minimize hot path latency, this only performs the actual timeout
    /// check once per second (TIMEOUT_CHECK_INTERVAL_NS).
    #[inline]
    pub fn check_pending_timeouts(&mut self, current_time_ns: i64) -> Vec<OrderDecision> {
        // Fast path: skip check if not enough time has passed (single comparison)
        if current_time_ns - self.last_timeout_check_ns < TIMEOUT_CHECK_INTERVAL_NS {
            return vec![];
        }

        // Update last check time
        self.last_timeout_check_ns = current_time_ns;

        // Perform the actual timeout check
        self.check_timeouts(current_time_ns)
    }

    /// Check for timed out pending orders (no throttling).
    ///
    /// This is used by the background timeout checker task to ensure
    /// pending orders are cleaned up even when market data updates are sparse.
    #[inline]
    pub fn check_timeouts_now(&mut self, current_time_ns: i64) -> Vec<OrderDecision> {
        self.check_timeouts(current_time_ns)
    }

    /// Process a new quote and return order decisions.
    ///
    /// This is the HOT PATH - must complete in <10µs.
    /// Called synchronously in the main event loop.
    ///
    /// NOTE: Timeout checks are done separately via check_pending_timeouts()
    /// which is called on every orderbook update, not just when quotes are generated.
    #[inline]
    pub fn on_quote(&mut self, quote: &Quote, current_time_ns: i64) -> Vec<OrderDecision> {
        // Early exit if shutting down or paused (atomic, no latency)
        // Use Acquire ordering to ensure we see the Release store from shutdown()/pause()
        if self.shutdown.load(Ordering::Acquire) || self.paused.load(Ordering::Acquire) {
            return vec![];
        }

        // Early exit if quote not valid for trading
        if !quote.valid_for_trading {
            return vec![];
        }

        // Validate mid_price to prevent NaN/Inf from bypassing position limits
        // (NaN comparisons always return false, which would skip all limit checks)
        if !quote.mid_price.is_finite() || quote.mid_price <= 0.0 {
            return vec![];
        }

        // Calculate position in dollars (from poller - source of truth)
        let position_dollar = self.position.get() * quote.mid_price;

        // Get max_position from SharedEquity (lock-free read, ~1ns)
        // If equity not initialized (max_position = 0), don't enforce limits
        let max_position = self.shared_equity.max_position_dollar();

        // Use > and < (not >= and <=) so that at exactly the limit we can still
        // place orders on the opposite side to rebalance position
        let at_max_long = max_position > 0.0 && position_dollar > max_position;
        let at_max_short = max_position > 0.0 && position_dollar < -max_position;

        // Use the minimum of configured levels and quote levels
        let num_levels = self.config.num_levels.min(quote.num_levels);

        let mut decisions = Vec::with_capacity(num_levels * 4); // 2 sides x 2 levels x 2 actions max

        // Process each level
        for level in 0..num_levels {
            // Process BID side (buy) - skip if at max long position
            if !at_max_long {
                if let Some(decision) = self.process_side_level(
                    Side::Buy,
                    level,
                    quote.bid_prices[level],
                    quote.quantity,
                    current_time_ns,
                ) {
                    decisions.push(decision);
                }
            } else {
                // At max long - cancel any existing bid at this level
                if let Some(bid) = &self.bid_orders[level] {
                    if bid.state != OrderState::Canceling {
                        debug!(
                            "[{}] At max long position ({:.2} > {:.2}), canceling bid L{}",
                            self.config.symbol, position_dollar, max_position, level
                        );
                        decisions.push(OrderDecision::Cancel {
                            cl_ord_id: bid.cl_ord_id.clone(),
                        });
                    }
                }
            }

            // Process ASK side (sell) - skip if at max short position
            if !at_max_short {
                if let Some(decision) = self.process_side_level(
                    Side::Sell,
                    level,
                    quote.ask_prices[level],
                    quote.quantity,
                    current_time_ns,
                ) {
                    decisions.push(decision);
                }
            } else {
                // At max short - cancel any existing ask at this level
                if let Some(ask) = &self.ask_orders[level] {
                    if ask.state != OrderState::Canceling {
                        debug!(
                            "[{}] At max short position ({:.2} < -{:.2}), canceling ask L{}",
                            self.config.symbol, position_dollar, max_position, level
                        );
                        decisions.push(OrderDecision::Cancel {
                            cl_ord_id: ask.cl_ord_id.clone(),
                        });
                    }
                }
            }
        }

        decisions
    }

    /// Process one side at a specific level and return decision.
    #[inline]
    fn process_side_level(
        &mut self,
        side: Side,
        level: usize,
        new_price: f64,
        qty: f64,
        current_time_ns: i64,
    ) -> Option<OrderDecision> {
        // Check order state without cloning - only clone cl_ord_id when needed
        let order = match side {
            Side::Buy => self.bid_orders[level].as_ref(),
            Side::Sell => self.ask_orders[level].as_ref(),
        };

        match order {
            None => {
                // No order at this level - place new one
                let cl_ord_id = self.generate_cl_ord_id(level);
                debug!(
                    "[{}] NEW {} L{} order: price={:.2}, qty={:.6}, id={}",
                    self.config.symbol, side, level, new_price, qty, cl_ord_id
                );
                self.set_order_pending(side, level, cl_ord_id.clone(), new_price, qty, current_time_ns);
                self.stats.orders_sent += 1;
                self.last_order_sent_at_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                Some(OrderDecision::Send {
                    side,
                    level,
                    price: new_price,
                    qty,
                    cl_ord_id,
                })
            }
            Some(o) if o.state == OrderState::Pending => {
                // Still pending - wait for confirmation
                let age_secs = (current_time_ns - o.sent_at_ns) / 1_000_000_000;
                if self.config.debug {
                    info!(
                        "[{}] {} L{} order blocked: pending confirmation for {} ({}s ago)",
                        self.config.symbol, side, level, o.cl_ord_id, age_secs
                    );
                }
                None
            }
            Some(o) if o.state == OrderState::Canceling => {
                // Cancel in progress - wait for confirmation
                let age_secs = (current_time_ns - o.sent_at_ns) / 1_000_000_000;
                // Always log at info level for Canceling - this blocks new orders
                info!(
                    "[{}] {} L{} order blocked: cancel pending for {} ({}s ago, order_id={:?})",
                    self.config.symbol, side, level, o.cl_ord_id, age_secs, o.order_id
                );
                None
            }
            Some(o) => {
                // Live order - check if reprice needed
                let price_changed = self.should_reprice(o.price, new_price);
                let age_ns = current_time_ns - o.sent_at_ns;
                let order_too_old = self.config.max_live_age_ns > 0
                    && age_ns > self.config.max_live_age_ns as i64;

                if price_changed || order_too_old {
                    let change_bps = ((new_price - o.price) / o.price).abs() * 10_000.0;
                    let age_secs = age_ns / 1_000_000_000;

                    if order_too_old && !price_changed {
                        debug!(
                            "[{}] REFRESH {} L{} order (age={}s > {}s): {:.2} -> {:.2} ({:.1}bps), id={}",
                            self.config.symbol, side, level, age_secs,
                            self.config.max_live_age_ns / 1_000_000_000,
                            o.price, new_price, change_bps, o.cl_ord_id
                        );
                    } else {
                        debug!(
                            "[{}] REPRICE {} L{} order: {:.2} -> {:.2} ({:.1}bps), id={}",
                            self.config.symbol, side, level, o.price, new_price, change_bps, o.cl_ord_id
                        );
                    }
                    let cancel_id = o.cl_ord_id.clone(); // Only clone when actually repricing
                    self.set_order_canceling(side, level, current_time_ns);
                    self.stats.reprices += 1;

                    // Store pending price for immediate placement after cancel confirms
                    match side {
                        Side::Buy => self.pending_bid_prices[level] = Some((new_price, qty)),
                        Side::Sell => self.pending_ask_prices[level] = Some((new_price, qty)),
                    }

                    Some(OrderDecision::CancelAndReplace {
                        cancel_id,
                        level,
                        new_price,
                        qty,
                    })
                } else {
                    // Price within threshold and order not too old - no action
                    None
                }
            }
        }
    }

    /// Check if price change exceeds threshold.
    #[inline]
    fn should_reprice(&self, old_price: f64, new_price: f64) -> bool {
        if old_price <= 0.0 {
            return true;
        }
        let change_bps = ((new_price - old_price) / old_price).abs() * 10_000.0;
        change_bps >= self.config.reprice_threshold_bps
    }

    /// Check for and handle timed out orders.
    ///
    /// Simple logic: any order older than timeout is cleared immediately
    /// and a cancel request is sent. No complex state machine - just clear
    /// the slot so new orders can be placed.
    #[inline]
    fn check_timeouts(&mut self, current_time_ns: i64) -> Vec<OrderDecision> {
        let mut cancels = Vec::with_capacity(MAX_ORDER_LEVELS * 2); // At most 4 orders
        let timeout_ns = self.config.pending_timeout_ns as i64;
        let timeout_secs = timeout_ns / 1_000_000_000;

        // Check bid orders at all levels - only timeout Pending or Canceling orders
        // Live orders should NOT timeout - they're valid on the exchange
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                // Skip Live orders - they don't need timeout checking
                if order.state == OrderState::Live {
                    // Live order is fine, no timeout needed
                } else {
                    let age_ns = current_time_ns - order.sent_at_ns;
                    let age_secs = age_ns / 1_000_000_000;

                    // Debug: log every check when order exists and is getting old
                    if age_secs >= 50 {
                        info!(
                            "[{}] Timeout check: {} L{} order {} state={:?} age={}s timeout={}s",
                            self.config.symbol, order.side, level, order.cl_ord_id, order.state, age_secs, timeout_secs
                        );
                    }

                    if age_ns > timeout_ns {
                        warn!(
                            "[{}] {} L{} order {} ({:?}) timed out after {}s (>{}s) - CLEARING SLOT",
                            self.config.symbol, order.side, level, order.cl_ord_id, order.state, age_secs, timeout_secs
                        );
                        cancels.push(OrderDecision::Cancel {
                            cl_ord_id: order.cl_ord_id.clone(),
                        });
                        self.stats.timeouts += 1;
                        // Clear immediately
                        self.bid_orders[level] = None;
                    }
                }
            }
        }

        // Check ask orders at all levels - only timeout Pending or Canceling orders
        // Live orders should NOT timeout - they're valid on the exchange
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.ask_orders[level] {
                // Skip Live orders - they don't need timeout checking
                if order.state == OrderState::Live {
                    // Live order is fine, no timeout needed
                } else {
                    let age_ns = current_time_ns - order.sent_at_ns;
                    let age_secs = age_ns / 1_000_000_000;

                    // Debug: log every check when order exists and is getting old
                    if age_secs >= 50 {
                        info!(
                            "[{}] Timeout check: {} L{} order {} state={:?} age={}s timeout={}s",
                            self.config.symbol, order.side, level, order.cl_ord_id, order.state, age_secs, timeout_secs
                        );
                    }

                    if age_ns > timeout_ns {
                        warn!(
                            "[{}] {} L{} order {} ({:?}) timed out after {}s (>{}s) - CLEARING SLOT",
                            self.config.symbol, order.side, level, order.cl_ord_id, order.state, age_secs, timeout_secs
                        );
                        cancels.push(OrderDecision::Cancel {
                            cl_ord_id: order.cl_ord_id.clone(),
                        });
                        self.stats.timeouts += 1;
                        // Clear immediately
                        self.ask_orders[level] = None;
                    }
                }
            }
        }

        cancels
    }

    /// Set order state to pending.
    fn set_order_pending(
        &mut self,
        side: Side,
        level: usize,
        cl_ord_id: String,
        price: f64,
        quantity: f64,
        sent_at_ns: i64,
    ) {
        let order = LiveOrder {
            cl_ord_id,
            order_id: None,
            side,
            level,
            price,
            quantity,
            state: OrderState::Pending,
            sent_at_ns,
            cancel_fail_count: 0,
        };

        match side {
            Side::Buy => self.bid_orders[level] = Some(order),
            Side::Sell => self.ask_orders[level] = Some(order),
        }
    }

    /// Set order state to canceling and reset the timeout clock.
    ///
    /// Updating `sent_at_ns` when entering Canceling state ensures that:
    /// 1. Timeout is measured from when cancel was initiated, not original order placement
    /// 2. If cancel fails and we revert to Live, the timeout won't fire prematurely
    fn set_order_canceling(&mut self, side: Side, level: usize, current_time_ns: i64) {
        let order = match side {
            Side::Buy => &mut self.bid_orders[level],
            Side::Sell => &mut self.ask_orders[level],
        };

        if let Some(o) = order {
            o.state = OrderState::Canceling;
            o.sent_at_ns = current_time_ns; // Reset timeout clock for cancel operation
        }
    }

    // ========== Event Handlers (called from async context) ==========

    /// Called when an order is accepted by the exchange.
    pub fn on_order_accepted(&mut self, cl_ord_id: &str, order_id: i64) {
        // Reset circuit breaker on successful acceptance
        self.consecutive_rejections = 0;

        // Warn if order_id is 0 or negative (potentially invalid)
        if order_id <= 0 {
            warn!(
                "[{}] Order {} accepted with suspicious order_id={} - cancellation may fail",
                self.config.symbol, cl_ord_id, order_id
            );
        }

        if let Some(order) = self.find_order_mut(cl_ord_id) {
            let side = order.side;
            let prev_state = order.state;
            let sent_at_ns = order.sent_at_ns;

            // Always update order_id (needed for cancel matching)
            order.order_id = Some(order_id);

            // Only transition to Live if still Pending
            // If already Canceling, keep it Canceling (cancel is in flight)
            if prev_state == OrderState::Pending {
                order.state = OrderState::Live;
                self.stats.orders_accepted += 1;

                // Calculate acceptance latency
                use std::time::{SystemTime, UNIX_EPOCH};
                let now_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64;
                let latency_ms = (now_ns - sent_at_ns) / 1_000_000;

                debug!(
                    "[{}] {} order accepted: {} -> {} (latency={}ms)",
                    self.config.symbol, side, cl_ord_id, order_id, latency_ms
                );
            } else if prev_state == OrderState::Canceling {
                // Order was already being canceled when acceptance arrived
                // Keep in Canceling state - cancel request is in flight
                warn!(
                    "[{}] {} order {} accepted while Canceling (order_id={}) - keeping Canceling state",
                    self.config.symbol, side, cl_ord_id, order_id
                );
                self.stats.orders_accepted += 1;
            }
        }
    }

    /// Called when an order is rejected.
    pub fn on_order_rejected(&mut self, cl_ord_id: &str, reason: &str) {
        // Get side and level before clearing
        let order_info = self.find_order(cl_ord_id).map(|o| (o.side, o.level));
        let side_str = order_info.map(|(s, l)| format!("{} L{} ", s, l)).unwrap_or_default();
        warn!(
            "[{}] {}order rejected: {} - {}",
            self.config.symbol, side_str, cl_ord_id, reason
        );
        self.clear_order_by_cl_ord_id(cl_ord_id);
        self.stats.rejections += 1;

        // Also clear pending price for this side/level to avoid placing stale orders
        if let Some((side, level)) = order_info {
            self.clear_pending_price(side, level);
        }

        // Circuit breaker: pause trading after too many consecutive rejections
        self.consecutive_rejections += 1;
        if self.config.circuit_breaker_rejections > 0
            && self.consecutive_rejections >= self.config.circuit_breaker_rejections
        {
            error!(
                "[{}] CIRCUIT BREAKER: {} consecutive rejections - PAUSING trading",
                self.config.symbol, self.consecutive_rejections
            );
            self.paused.store(true, Ordering::Release);
            // Record when circuit breaker was triggered
            self.circuit_breaker_triggered_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        }
    }

    /// Reset circuit breaker and resume trading.
    pub fn reset_circuit_breaker(&mut self) {
        self.consecutive_rejections = 0;
        self.circuit_breaker_triggered_at_ms = 0;
        self.paused.store(false, Ordering::Release);
        info!("[{}] Circuit breaker reset, trading resumed", self.config.symbol);
    }

    /// Check if circuit breaker should auto-recover.
    /// Returns true if recovery occurred.
    pub fn check_circuit_breaker_recovery(&mut self, recovery_secs: u64) -> bool {
        // Only check if circuit breaker is active and auto-recovery is enabled
        if recovery_secs == 0 || self.circuit_breaker_triggered_at_ms == 0 {
            return false;
        }

        // Don't recover if not actually paused (could be WS disconnect)
        if !self.paused.load(Ordering::Acquire) {
            return false;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let elapsed_secs = (now_ms - self.circuit_breaker_triggered_at_ms) / 1000;

        if elapsed_secs >= recovery_secs {
            warn!(
                "[{}] CIRCUIT BREAKER AUTO-RECOVERY: {} seconds elapsed, resuming trading",
                self.config.symbol, elapsed_secs
            );
            self.reset_circuit_breaker();
            return true;
        }

        false
    }

    /// Trigger a safety pause from external signal (e.g., >2 orders detected).
    ///
    /// This is called when the OpenOrdersChecker detects more than 2 orders
    /// on the exchange, indicating a duplicate/stuck state that requires
    /// immediate intervention.
    pub fn trigger_safety_pause(&mut self, reason: &str) {
        error!(
            "[{}] SAFETY PAUSE: {} - pausing trading",
            self.config.symbol, reason
        );
        self.paused.store(true, Ordering::Release);
        self.safety_pause_triggered_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
    }

    /// Check if safety pause should auto-recover.
    /// Returns true if recovery occurred.
    ///
    /// Unlike circuit breaker which is triggered by rejections, safety pause
    /// is triggered by detecting >2 orders on the exchange. Both have their
    /// own recovery timers to allow independent tuning.
    pub fn check_safety_pause_recovery(&mut self, recovery_secs: u64) -> bool {
        // Only check if safety pause is active and auto-recovery is enabled
        if recovery_secs == 0 || self.safety_pause_triggered_at_ms == 0 {
            return false;
        }

        // Don't recover if not actually paused
        if !self.paused.load(Ordering::Acquire) {
            return false;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let elapsed_secs = (now_ms - self.safety_pause_triggered_at_ms) / 1000;

        if elapsed_secs >= recovery_secs {
            warn!(
                "[{}] SAFETY PAUSE RECOVERY: {} seconds elapsed, resuming trading",
                self.config.symbol, elapsed_secs
            );
            self.safety_pause_triggered_at_ms = 0;
            self.paused.store(false, Ordering::Release);
            return true;
        }

        false
    }

    /// Get consecutive rejection count.
    pub fn consecutive_rejections(&self) -> u32 {
        self.consecutive_rejections
    }

    /// Get seconds since last order was sent.
    /// Returns None if no order has ever been sent.
    pub fn seconds_since_last_order(&self) -> Option<u64> {
        if self.last_order_sent_at_ms == 0 {
            return None;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Some((now_ms - self.last_order_sent_at_ms) / 1000)
    }

    /// Called when an order is canceled.
    pub fn on_order_canceled(&mut self, order_id: i64) {
        // Get info for logging before clearing - search all levels
        let mut order_info: Option<(String, Side, usize, OrderState)> = None;

        for level in 0..MAX_ORDER_LEVELS {
            if let Some(o) = &self.bid_orders[level] {
                if o.order_id == Some(order_id) {
                    order_info = Some((o.cl_ord_id.clone(), o.side, level, o.state));
                    break;
                }
            }
            if let Some(o) = &self.ask_orders[level] {
                if o.order_id == Some(order_id) {
                    order_info = Some((o.cl_ord_id.clone(), o.side, level, o.state));
                    break;
                }
            }
        }

        if let Some((cl_ord_id, side, level, state)) = order_info {
            info!(
                "[{}] {} L{} order canceled: {} (order_id={}, was {:?}) - slot freed",
                self.config.symbol, side, level, cl_ord_id, order_id, state
            );
        } else {
            // Order not found - may have been force-cleared or already canceled
            debug!(
                "[{}] Cancel confirmation for unknown order_id={} - already cleared?",
                self.config.symbol, order_id
            );
        }
        self.clear_order_by_exchange_id(order_id);
        self.stats.orders_canceled += 1;
    }

    /// Called when an order cancel is confirmed by client order ID.
    pub fn on_order_canceled_by_cl_ord_id(&mut self, cl_ord_id: &str) {
        // Get info for logging before clearing
        let order_info = self.find_order(cl_ord_id).map(|o| (o.side, o.level, o.state));

        if let Some((side, level, state)) = order_info {
            info!(
                "[{}] {} L{} order canceled: {} (was {:?}) - slot freed",
                self.config.symbol, side, level, cl_ord_id, state
            );
        } else {
            debug!(
                "[{}] Cancel confirmation for unknown cl_ord_id={} - already cleared?",
                self.config.symbol, cl_ord_id
            );
        }
        self.clear_order_by_cl_ord_id(cl_ord_id);
        self.stats.orders_canceled += 1;
    }

    /// Called when a cancel request fails.
    ///
    /// The order is still live on the exchange, so we revert from Canceling
    /// back to Live state to allow repricing on the next quote.
    /// Maximum consecutive cancel failures before forcefully clearing the slot.
    /// This prevents infinite cancel loops when order_id is invalid (e.g., 0).
    const MAX_CANCEL_FAILURES: u32 = 3;

    pub fn on_cancel_failed(&mut self, order_id: i64, reason: &str) {
        warn!(
            "[{}] Cancel failed for order {}: {}",
            self.config.symbol, order_id, reason
        );

        // Find the order and handle the failure - search all levels
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &mut self.bid_orders[level] {
                if order.order_id == Some(order_id) && order.state == OrderState::Canceling {
                    order.cancel_fail_count += 1;

                    // If too many failures, forcefully clear the slot to break the loop
                    if order.cancel_fail_count >= Self::MAX_CANCEL_FAILURES {
                        error!(
                            "[{}] Bid L{} order {} exceeded {} cancel failures - FORCE CLEARING SLOT (order_id={}, cl_ord_id={})",
                            self.config.symbol, level, order_id, Self::MAX_CANCEL_FAILURES,
                            order_id, order.cl_ord_id
                        );
                        self.bid_orders[level] = None;
                        self.pending_bid_prices[level] = None; // Clear pending price too
                    } else {
                        // Revert to Live for retry
                        order.state = OrderState::Live;
                        debug!(
                            "[{}] Reverted bid L{} order {} to Live state (fail count: {})",
                            self.config.symbol, level, order_id, order.cancel_fail_count
                        );
                    }
                    return;
                }
            }
        }

        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &mut self.ask_orders[level] {
                if order.order_id == Some(order_id) && order.state == OrderState::Canceling {
                    order.cancel_fail_count += 1;

                    // If too many failures, forcefully clear the slot to break the loop
                    if order.cancel_fail_count >= Self::MAX_CANCEL_FAILURES {
                        error!(
                            "[{}] Ask L{} order {} exceeded {} cancel failures - FORCE CLEARING SLOT (order_id={}, cl_ord_id={})",
                            self.config.symbol, level, order_id, Self::MAX_CANCEL_FAILURES,
                            order_id, order.cl_ord_id
                        );
                        self.ask_orders[level] = None;
                        self.pending_ask_prices[level] = None; // Clear pending price too
                    } else {
                        // Revert to Live for retry
                        order.state = OrderState::Live;
                        debug!(
                            "[{}] Reverted ask L{} order {} to Live state (fail count: {})",
                            self.config.symbol, level, order_id, order.cancel_fail_count
                        );
                    }
                    return;
                }
            }
        }
    }

    // ========== Pending Order Handling ==========

    /// Check if we have pending prices to place after a slot was cleared.
    ///
    /// This implements immediate order placement after cancel confirms,
    /// avoiding the race condition where price changes during cancel wait.
    /// Returns decisions for immediate order placement.
    pub fn check_pending_orders(&mut self, current_time_ns: i64) -> Vec<OrderDecision> {
        let mut decisions = Vec::new();

        // Check bid orders at all levels - only place if slot is empty
        for level in 0..MAX_ORDER_LEVELS {
            if self.bid_orders[level].is_none() {
                if let Some((price, qty)) = self.pending_bid_prices[level].take() {
                    let cl_ord_id = self.generate_cl_ord_id(level);
                    debug!(
                        "[{}] Placing pending BID L{}: {:.2} x {:.6}, id={}",
                        self.config.symbol, level, price, qty, cl_ord_id
                    );
                    self.set_order_pending(Side::Buy, level, cl_ord_id.clone(), price, qty, current_time_ns);
                    self.stats.orders_sent += 1;
                    self.last_order_sent_at_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    decisions.push(OrderDecision::Send {
                        side: Side::Buy,
                        level,
                        price,
                        qty,
                        cl_ord_id,
                    });
                }
            }
        }

        // Check ask orders at all levels - only place if slot is empty
        for level in 0..MAX_ORDER_LEVELS {
            if self.ask_orders[level].is_none() {
                if let Some((price, qty)) = self.pending_ask_prices[level].take() {
                    let cl_ord_id = self.generate_cl_ord_id(level);
                    debug!(
                        "[{}] Placing pending ASK L{}: {:.2} x {:.6}, id={}",
                        self.config.symbol, level, price, qty, cl_ord_id
                    );
                    self.set_order_pending(Side::Sell, level, cl_ord_id.clone(), price, qty, current_time_ns);
                    self.stats.orders_sent += 1;
                    self.last_order_sent_at_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    decisions.push(OrderDecision::Send {
                        side: Side::Sell,
                        level,
                        price,
                        qty,
                        cl_ord_id,
                    });
                }
            }
        }

        decisions
    }

    /// Clear pending price for a side at a specific level (e.g., on rejection or timeout).
    pub fn clear_pending_price(&mut self, side: Side, level: usize) {
        if level >= MAX_ORDER_LEVELS {
            return;
        }
        match side {
            Side::Buy => self.pending_bid_prices[level] = None,
            Side::Sell => self.pending_ask_prices[level] = None,
        }
    }

    /// Clear all pending prices (e.g., on disconnect).
    pub fn clear_all_pending_prices(&mut self) {
        for level in 0..MAX_ORDER_LEVELS {
            self.pending_bid_prices[level] = None;
            self.pending_ask_prices[level] = None;
        }
    }

    // ========== Order Lookup Helpers ==========

    #[inline]
    fn find_order(&self, cl_ord_id: &str) -> Option<&LiveOrder> {
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    return Some(order);
                }
            }
            if let Some(order) = &self.ask_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    return Some(order);
                }
            }
        }
        None
    }

    #[inline]
    fn find_order_mut(&mut self, cl_ord_id: &str) -> Option<&mut LiveOrder> {
        // First find which array and level contains the order
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    return self.bid_orders[level].as_mut();
                }
            }
            if let Some(order) = &self.ask_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    return self.ask_orders[level].as_mut();
                }
            }
        }
        None
    }

    #[inline]
    fn clear_order_by_cl_ord_id(&mut self, cl_ord_id: &str) {
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    self.bid_orders[level] = None;
                    return;
                }
            }
            if let Some(order) = &self.ask_orders[level] {
                if order.cl_ord_id == cl_ord_id {
                    self.ask_orders[level] = None;
                    return;
                }
            }
        }
    }

    #[inline]
    fn clear_order_by_exchange_id(&mut self, order_id: i64) {
        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                if order.order_id == Some(order_id) {
                    self.bid_orders[level] = None;
                    return;
                }
            }
            if let Some(order) = &self.ask_orders[level] {
                if order.order_id == Some(order_id) {
                    self.ask_orders[level] = None;
                    return;
                }
            }
        }
    }

    // ========== Pause/Resume (for WebSocket disconnection) ==========

    /// Pause trading (stop generating new orders).
    /// Called when Order WebSocket disconnects.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Release);
        info!("[{}] Order manager paused (WebSocket disconnected)", self.config.symbol);
    }

    /// Resume trading after WebSocket reconnection.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Release);
        info!("[{}] Order manager resumed (WebSocket reconnected)", self.config.symbol);
    }

    /// Check if trading is paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    // ========== Shutdown ==========

    /// Initiate graceful shutdown - stop placing new orders.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        info!("[{}] Order manager shutdown initiated", self.config.symbol);
    }

    /// Check if shutdown is in progress.
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Get all live order client IDs for batch cancel on shutdown.
    pub fn get_all_live_order_ids(&self) -> Vec<String> {
        let mut ids = Vec::with_capacity(MAX_ORDER_LEVELS * 2);

        for level in 0..MAX_ORDER_LEVELS {
            if let Some(order) = &self.bid_orders[level] {
                if matches!(order.state, OrderState::Live | OrderState::Pending) {
                    ids.push(order.cl_ord_id.clone());
                }
            }
            if let Some(order) = &self.ask_orders[level] {
                if matches!(order.state, OrderState::Live | OrderState::Pending) {
                    ids.push(order.cl_ord_id.clone());
                }
            }
        }

        ids
    }

    /// Clear all tracked orders from internal state.
    ///
    /// Use this after canceling all orders via HTTP to ensure the order manager's
    /// internal state is synchronized. This prevents stale orders from blocking
    /// new order placement after reconnection.
    pub fn clear_all_orders(&mut self) {
        for level in 0..MAX_ORDER_LEVELS {
            if self.bid_orders[level].is_some() {
                debug!("[{}] Clearing bid L{} order from state", self.config.symbol, level);
                self.bid_orders[level] = None;
            }
            if self.ask_orders[level].is_some() {
                debug!("[{}] Clearing ask L{} order from state", self.config.symbol, level);
                self.ask_orders[level] = None;
            }
        }
    }

    /// Get current bid order info at level 0 (for logging/monitoring).
    /// For backward compatibility, returns the level 0 bid order.
    pub fn bid_order(&self) -> Option<&LiveOrder> {
        self.bid_orders[0].as_ref()
    }

    /// Get current ask order info at level 0 (for logging/monitoring).
    /// For backward compatibility, returns the level 0 ask order.
    pub fn ask_order(&self) -> Option<&LiveOrder> {
        self.ask_orders[0].as_ref()
    }

    /// Get bid order at a specific level.
    pub fn bid_order_at(&self, level: usize) -> Option<&LiveOrder> {
        if level < MAX_ORDER_LEVELS {
            self.bid_orders[level].as_ref()
        } else {
            None
        }
    }

    /// Get ask order at a specific level.
    pub fn ask_order_at(&self, level: usize) -> Option<&LiveOrder> {
        if level < MAX_ORDER_LEVELS {
            self.ask_orders[level].as_ref()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_position() -> Arc<SharedPosition> {
        Arc::new(SharedPosition::new("TEST-USD".to_string()))
    }

    /// Create SharedEquity with a given max_position_dollar.
    /// Uses reverse formula: equity = max_position / 0.9 (since max_pos = equity * 0.9)
    fn create_test_equity(max_position_dollar: f64) -> Arc<SharedEquity> {
        let equity = Arc::new(SharedEquity::new(1, 10.0, 1.0));
        // Reverse the formula: max_pos = equity * 0.9 => equity = max_pos / 0.9
        let raw_equity = max_position_dollar / 0.9;
        equity.set_equity(raw_equity);
        equity
    }

    /// Create SharedEquity with default high limit (won't trigger position limits in tests).
    fn create_test_equity_high_limit() -> Arc<SharedEquity> {
        create_test_equity(100_000_000.0) // $100M max position
    }

    fn create_test_quote(bid: f64, ask: f64, qty: f64) -> Quote {
        Quote {
            symbol: "TEST-USD".into(),
            bid_prices: [bid, bid - 1.0], // Level 1 is 1.0 lower
            ask_prices: [ask, ask + 1.0], // Level 1 is 1.0 higher
            num_levels: 1, // Default to 1 level for backward compatibility
            quantity: qty,
            mid_price: (bid + ask) / 2.0,
            spread: ask - bid,
            volatility: 0.001,
            alpha: 0.0,
            position: 0.0,
            half_spread_tick: 1.0,
            valid_for_trading: true,
            history_secs: 600.0,
            bid_floored: [false, false],
            ask_floored: [false, false],
        }
    }

    fn create_test_quote_2_levels(bid: f64, ask: f64, qty: f64) -> Quote {
        Quote {
            symbol: "TEST-USD".into(),
            bid_prices: [bid, bid - 10.0], // Level 1 is wider
            ask_prices: [ask, ask + 10.0], // Level 1 is wider
            num_levels: 2,
            quantity: qty,
            mid_price: (bid + ask) / 2.0,
            spread: ask - bid,
            volatility: 0.001,
            alpha: 0.0,
            position: 0.0,
            half_spread_tick: 1.0,
            valid_for_trading: true,
            history_secs: 600.0,
            bid_floored: [false, false],
            ask_floored: [false, false],
        }
    }

    #[test]
    fn test_new_order_manager() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let config = OrderManagerConfig::default();
        let manager = OrderManager::new(config, position, equity);

        assert!(manager.bid_order().is_none());
        assert!(manager.ask_order().is_none());
        assert_eq!(manager.stats().orders_sent, 0);
    }

    #[test]
    fn test_initial_order_placement() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let config = OrderManagerConfig::default();
        let mut manager = OrderManager::new(config, position, equity);

        let quote = create_test_quote(99000.0, 101000.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);

        // Should place both bid and ask
        assert_eq!(decisions.len(), 2);

        let has_buy = decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Buy, .. }));
        let has_sell = decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Sell, .. }));

        assert!(has_buy, "Should have buy order");
        assert!(has_sell, "Should have sell order");
    }

    #[test]
    fn test_no_reprice_within_threshold() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let mut config = OrderManagerConfig::default();
        config.reprice_threshold_bps = 10.0; // 10 bps threshold
        let mut manager = OrderManager::new(config, position, equity);

        // Place initial orders
        let quote = create_test_quote(100000.0, 100010.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);
        assert_eq!(decisions.len(), 2);

        // Simulate acceptance
        if let Some(OrderDecision::Send { cl_ord_id, .. }) = decisions.get(0) {
            manager.on_order_accepted(cl_ord_id, 1001);
        }
        if let Some(OrderDecision::Send { cl_ord_id, .. }) = decisions.get(1) {
            manager.on_order_accepted(cl_ord_id, 1002);
        }

        // Small price change within threshold (5 bps)
        let quote2 = create_test_quote(100005.0, 100015.0, 0.001);
        let decisions2 = manager.on_quote(&quote2, 2_000_000_000);

        // Should not reprice
        assert_eq!(decisions2.len(), 0, "Should not reprice within threshold");
    }

    #[test]
    fn test_reprice_beyond_threshold() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let mut config = OrderManagerConfig::default();
        config.reprice_threshold_bps = 1.0; // 1 bps threshold
        let mut manager = OrderManager::new(config, position, equity);

        // Place initial orders
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);

        // Simulate acceptance
        for decision in &decisions {
            if let OrderDecision::Send { cl_ord_id, .. } = decision {
                manager.on_order_accepted(cl_ord_id, 1001);
            }
        }

        // Large price change (100 bps)
        let quote2 = create_test_quote(101000.0, 101100.0, 0.001);
        let decisions2 = manager.on_quote(&quote2, 2_000_000_000);

        // Should reprice
        assert!(!decisions2.is_empty(), "Should reprice beyond threshold");
    }

    #[test]
    fn test_position_limit_long() {
        let position = create_test_position();
        position.set(0.005); // 0.005 BTC position

        // Set max_position to $500 via SharedEquity
        let equity = create_test_equity(500.0);
        let config = OrderManagerConfig::default();
        let mut manager = OrderManager::new(config, position, equity);

        // Quote at $100,000 means position = $500 (at max)
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);

        // Should only place ask (sell), not bid (buy) since at max long
        assert!(decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Sell, .. })));
        assert!(!decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Buy, .. })));
    }

    #[test]
    fn test_shutdown() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let config = OrderManagerConfig::default();
        let mut manager = OrderManager::new(config, position, equity);

        // Place initial orders
        let quote = create_test_quote(100000.0, 100100.0, 0.001);
        let _decisions = manager.on_quote(&quote, 1_000_000_000);

        // Shutdown
        manager.shutdown();
        assert!(manager.is_shutting_down());

        // Should not place any orders after shutdown
        let decisions2 = manager.on_quote(&quote, 2_000_000_000);
        assert!(decisions2.is_empty(), "Should not place orders after shutdown");
    }

    #[test]
    fn test_two_level_order_placement() {
        let position = create_test_position();
        let equity = create_test_equity_high_limit();
        let mut config = OrderManagerConfig::default();
        config.num_levels = 2; // Enable 2 levels
        let mut manager = OrderManager::new(config, position, equity);

        // Create a 2-level quote
        let quote = create_test_quote_2_levels(99000.0, 101000.0, 0.001);
        let decisions = manager.on_quote(&quote, 1_000_000_000);

        // Should place 4 orders total (2 bids + 2 asks)
        assert_eq!(decisions.len(), 4, "Should place 4 orders for 2-level mode");

        // Check we have orders at both levels
        let mut bid_levels = vec![];
        let mut ask_levels = vec![];

        for decision in &decisions {
            if let OrderDecision::Send { side, level, .. } = decision {
                match side {
                    Side::Buy => bid_levels.push(*level),
                    Side::Sell => ask_levels.push(*level),
                }
            }
        }

        assert!(bid_levels.contains(&0), "Should have bid at level 0");
        assert!(bid_levels.contains(&1), "Should have bid at level 1");
        assert!(ask_levels.contains(&0), "Should have ask at level 0");
        assert!(ask_levels.contains(&1), "Should have ask at level 1");
    }

    #[test]
    fn test_two_level_position_limit_cancels_both() {
        let position = create_test_position();
        position.set(0.006); // 0.006 BTC position = $600 at $100k

        // Set max_position to $500 via SharedEquity
        let equity = create_test_equity(500.0);
        let mut config = OrderManagerConfig::default();
        config.num_levels = 2;
        let mut manager = OrderManager::new(config, position, equity);

        // First place orders (simulating already having orders)
        let quote = create_test_quote_2_levels(100000.0, 100100.0, 0.001);
        let initial_decisions = manager.on_quote(&quote, 1_000_000_000);

        // Accept only the sell orders (we're over max long so no buys should be placed)
        for decision in &initial_decisions {
            if let OrderDecision::Send { side: Side::Sell, cl_ord_id, .. } = decision {
                manager.on_order_accepted(cl_ord_id, 1001);
            }
        }

        // At max long, should not place any buy orders at either level
        assert!(!initial_decisions.iter().any(|d| matches!(d, OrderDecision::Send { side: Side::Buy, .. })),
            "Should not place buy orders when at max long position");

        // Should only have sell orders (at both levels)
        let sell_count = initial_decisions.iter()
            .filter(|d| matches!(d, OrderDecision::Send { side: Side::Sell, .. }))
            .count();
        assert_eq!(sell_count, 2, "Should have 2 sell orders (one per level) when at max long");
    }
}
