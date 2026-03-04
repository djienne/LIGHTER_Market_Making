//! Comparison test: Binance vs StandX OBI/Alpha values.
//!
//! Runs both WebSocket connections simultaneously and compares the OBI metrics.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::time::timeout;

use standx_orderbook::binance::{BinanceClient, BinanceClientConfig, BinanceEvent, BinanceObiCalculator};
use standx_orderbook::config::WebSocketConfig;
use standx_orderbook::strategy::RollingStats;
use standx_orderbook::types::OrderbookSnapshot;
use standx_orderbook::websocket::{WsClient, WsEvent, StandXMessage};

/// Simple OBI calculator for StandX (mirrors BinanceObiCalculator logic)
struct StandXObiCalculator {
    imbalance_stats: RollingStats,
    mid_change_stats: RollingStats,
    prev_mid: Option<f64>,
    looking_depth: f64,
    min_samples: usize,
}

impl StandXObiCalculator {
    fn new(window_size: usize, looking_depth: f64) -> Self {
        Self {
            imbalance_stats: RollingStats::new(window_size),
            mid_change_stats: RollingStats::new(window_size),
            prev_mid: None,
            looking_depth,
            min_samples: window_size / 2,
        }
    }

    fn update(&mut self, snapshot: &OrderbookSnapshot) -> Option<(f64, f64)> {
        let mid = snapshot.mid_price()?;

        // Calculate raw imbalance
        let imbalance = self.calculate_imbalance(snapshot, mid);
        self.imbalance_stats.push(imbalance);

        // Calculate mid price return for volatility
        if let Some(prev) = self.prev_mid {
            if prev > 0.0 {
                let ret = (mid - prev) / prev;
                self.mid_change_stats.push(ret);
            }
        }
        self.prev_mid = Some(mid);

        if self.imbalance_stats.len() < self.min_samples {
            return None;
        }

        let alpha = self.imbalance_stats.zscore(imbalance);
        let volatility = self.mid_change_stats.std();

        Some((alpha, volatility))
    }

    fn alpha(&self) -> f64 {
        self.imbalance_stats
            .latest()
            .map(|v| self.imbalance_stats.zscore(v))
            .unwrap_or(0.0)
    }

    fn volatility(&self) -> f64 {
        self.mid_change_stats.std()
    }

    fn is_warmed_up(&self) -> bool {
        self.imbalance_stats.len() >= self.min_samples
    }

    fn sample_count(&self) -> usize {
        self.imbalance_stats.len()
    }

    fn calculate_imbalance(&self, snapshot: &OrderbookSnapshot, mid: f64) -> f64 {
        let min_price = mid * (1.0 - self.looking_depth);
        let max_price = mid * (1.0 + self.looking_depth);

        let mut bid_volume = 0.0;
        let mut ask_volume = 0.0;

        for level in snapshot.bid_levels() {
            if level.price >= min_price {
                bid_volume += level.quantity;
            } else {
                break;
            }
        }

        for level in snapshot.ask_levels() {
            if level.price <= max_price {
                ask_volume += level.quantity;
            } else {
                break;
            }
        }

        let total = bid_volume + ask_volume;
        if total > 0.0 {
            (bid_volume - ask_volume) / total
        } else {
            0.0
        }
    }
}

#[derive(Default, Clone)]
struct Metrics {
    mid_price: f64,
    spread: f64,
    alpha: f64,
    vol_bps: f64,
    bid_count: u8,
    ask_count: u8,
    samples: usize,
}

/// Sanity check results for an orderbook
#[derive(Default, Clone)]
struct SanityCheck {
    bids_ordered: bool,      // Bids sorted descending by price
    asks_ordered: bool,      // Asks sorted ascending by price
    spread_positive: bool,   // Best ask > best bid
    no_crossed_book: bool,   // No bid >= any ask
    has_levels: bool,        // Has at least 1 bid and 1 ask
    total_checks: u64,
    failed_checks: u64,
}

impl SanityCheck {
    fn check_snapshot(snapshot: &OrderbookSnapshot) -> Self {
        let bids = snapshot.bid_levels();
        let asks = snapshot.ask_levels();

        // Check bids are sorted descending
        let bids_ordered = bids.windows(2).all(|w| w[0].price >= w[1].price);

        // Check asks are sorted ascending
        let asks_ordered = asks.windows(2).all(|w| w[0].price <= w[1].price);

        // Check spread is positive
        let spread_positive = if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first()) {
            best_ask.price > best_bid.price
        } else {
            false
        };

        // Check no crossed book (no bid price >= any ask price)
        let no_crossed_book = if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first()) {
            best_bid.price < best_ask.price
        } else {
            true
        };

        let has_levels = !bids.is_empty() && !asks.is_empty();

        Self {
            bids_ordered,
            asks_ordered,
            spread_positive,
            no_crossed_book,
            has_levels,
            total_checks: 1,
            failed_checks: if !bids_ordered || !asks_ordered || !spread_positive || !no_crossed_book || !has_levels { 1 } else { 0 },
        }
    }

    fn merge(&mut self, other: &SanityCheck) {
        self.total_checks += other.total_checks;
        self.failed_checks += other.failed_checks;
        // Keep the most recent check results
        self.bids_ordered = other.bids_ordered;
        self.asks_ordered = other.asks_ordered;
        self.spread_positive = other.spread_positive;
        self.no_crossed_book = other.no_crossed_book;
        self.has_levels = other.has_levels;
    }

    fn pass_rate(&self) -> f64 {
        if self.total_checks == 0 {
            return 0.0;
        }
        (self.total_checks - self.failed_checks) as f64 / self.total_checks as f64 * 100.0
    }
}

/// Track alpha correlation over time
struct CorrelationTracker {
    binance_alphas: Vec<f64>,
    standx_alphas: Vec<f64>,
    alpha_diffs: Vec<f64>,
}

impl CorrelationTracker {
    fn new() -> Self {
        Self {
            binance_alphas: Vec::with_capacity(1000),
            standx_alphas: Vec::with_capacity(1000),
            alpha_diffs: Vec::with_capacity(1000),
        }
    }

    fn push(&mut self, binance_alpha: f64, standx_alpha: f64) {
        self.binance_alphas.push(binance_alpha);
        self.standx_alphas.push(standx_alpha);
        self.alpha_diffs.push(binance_alpha - standx_alpha);
    }

    fn correlation(&self) -> f64 {
        if self.binance_alphas.len() < 10 {
            return 0.0;
        }

        let n = self.binance_alphas.len() as f64;
        let mean_b: f64 = self.binance_alphas.iter().sum::<f64>() / n;
        let mean_s: f64 = self.standx_alphas.iter().sum::<f64>() / n;

        let mut cov = 0.0;
        let mut var_b = 0.0;
        let mut var_s = 0.0;

        for i in 0..self.binance_alphas.len() {
            let db = self.binance_alphas[i] - mean_b;
            let ds = self.standx_alphas[i] - mean_s;
            cov += db * ds;
            var_b += db * db;
            var_s += ds * ds;
        }

        if var_b < 1e-10 || var_s < 1e-10 {
            return 0.0;
        }

        cov / (var_b.sqrt() * var_s.sqrt())
    }

    fn mean_diff(&self) -> f64 {
        if self.alpha_diffs.is_empty() {
            return 0.0;
        }
        self.alpha_diffs.iter().sum::<f64>() / self.alpha_diffs.len() as f64
    }

    fn std_diff(&self) -> f64 {
        if self.alpha_diffs.len() < 2 {
            return 0.0;
        }
        let mean = self.mean_diff();
        let variance: f64 = self.alpha_diffs.iter()
            .map(|d| (d - mean).powi(2))
            .sum::<f64>() / self.alpha_diffs.len() as f64;
        variance.sqrt()
    }

    fn sample_count(&self) -> usize {
        self.binance_alphas.len()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info,standx_orderbook::binance::client=info,standx_orderbook::websocket::client=info")
        .init();

    println!("=== Binance vs StandX OBI Comparison ===\n");

    // Configuration
    let test_duration = Duration::from_secs(120); // 2 minutes
    let print_interval = Duration::from_secs(2);
    let window_size = 300; // 30 seconds at ~10 updates/sec
    let looking_depth = 0.025; // 2.5%

    println!("Duration: {} seconds", test_duration.as_secs());
    println!("Window size: {} samples", window_size);
    println!("Looking depth: {:.1}%\n", looking_depth * 100.0);

    // Create Binance client (Futures by default)
    let binance_config = BinanceClientConfig::futures("btcusdt");
    println!("Binance market type: {:?}", binance_config.market_type);
    let binance_client = Arc::new(BinanceClient::with_config(binance_config));
    let mut binance_obi = BinanceObiCalculator::new(window_size, looking_depth);

    // Create StandX client
    let standx_config = WebSocketConfig {
        url: "wss://perps.standx.com/ws-stream/v1".to_string(),
        api_url: "wss://perps.standx.com/ws-api/v1".to_string(),
        reconnect_delay_secs: 5,
        max_reconnect_delay_secs: 60,
        connect_timeout_secs: 30,
        stale_timeout_secs: 60,
    };
    let standx_client = Arc::new(WsClient::new(standx_config, vec!["BTC-USD".to_string()]));
    let mut standx_obi = StandXObiCalculator::new(window_size, looking_depth);

    println!("Starting WebSocket connections...\n");

    // Start both clients
    let mut binance_rx = binance_client.clone().run().await;
    let mut standx_rx = standx_client.clone().run().await;

    // Tracking
    let start = Instant::now();
    let mut last_print = Instant::now();

    let mut binance_metrics = Metrics::default();
    let mut standx_metrics = Metrics::default();
    let mut binance_count = 0u64;
    let mut standx_count = 0u64;
    let mut correlation_tracker = CorrelationTracker::new();
    let mut binance_sanity = SanityCheck::default();
    let mut standx_sanity = SanityCheck::default();

    // Print header
    println!("{:>6} | {:^42} | {:^42} | {:>8}",
             "Time",
             "BINANCE (BTCUSDT)",
             "STANDX (BTC-USD)",
             "Diff");
    println!("{:>6} | {:>10} {:>8} {:>8} {:>8} | {:>10} {:>8} {:>8} {:>8} | {:>8}",
             "(sec)", "Mid", "Spread", "Alpha", "Vol(bp)",
             "Mid", "Spread", "Alpha", "Vol(bp)", "Alpha");
    println!("{}", "-".repeat(110));

    // Main loop
    while start.elapsed() < test_duration {
        tokio::select! {
            // Binance events
            result = timeout(Duration::from_millis(100), binance_rx.recv()) => {
                if let Ok(Some(event)) = result {
                    match event {
                        BinanceEvent::Snapshot(snapshot) => {
                            binance_count += 1;
                            // Run sanity check
                            let check = SanityCheck::check_snapshot(&snapshot);
                            binance_sanity.merge(&check);

                            if let Some((alpha, vol)) = binance_obi.update(&snapshot) {
                                binance_metrics = Metrics {
                                    mid_price: snapshot.mid_price().unwrap_or(0.0),
                                    spread: snapshot.spread().unwrap_or(0.0),
                                    alpha,
                                    vol_bps: vol * 10000.0,
                                    bid_count: snapshot.bid_count,
                                    ask_count: snapshot.ask_count,
                                    samples: binance_obi.sample_count(),
                                };
                            }
                        }
                        BinanceEvent::Error(e) => {
                            eprintln!("[Binance ERROR] {}", e);
                        }
                        _ => {}
                    }
                }
            }

            // StandX events
            result = timeout(Duration::from_millis(100), standx_rx.recv()) => {
                if let Ok(Some(event)) = result {
                    match event {
                        WsEvent::Message(StandXMessage::DepthBook(data), _ts) => {
                            standx_count += 1;
                            // Convert to OrderbookSnapshot
                            let mut snapshot = OrderbookSnapshot::new(standx_orderbook::types::Symbol::new("BTC-USD"));
                            snapshot.set_bids_from_strings(&data.bids, 50);
                            snapshot.set_asks_from_strings(&data.asks, 50);

                            // Run sanity check
                            let check = SanityCheck::check_snapshot(&snapshot);
                            standx_sanity.merge(&check);

                            if let Some((alpha, vol)) = standx_obi.update(&snapshot) {
                                standx_metrics = Metrics {
                                    mid_price: snapshot.mid_price().unwrap_or(0.0),
                                    spread: snapshot.spread().unwrap_or(0.0),
                                    alpha,
                                    vol_bps: vol * 10000.0,
                                    bid_count: snapshot.bid_count,
                                    ask_count: snapshot.ask_count,
                                    samples: standx_obi.sample_count(),
                                };
                            }
                        }
                        WsEvent::Error(e) => {
                            eprintln!("[StandX ERROR] {}", e);
                        }
                        WsEvent::Connected => {
                            println!("[StandX] Connected");
                        }
                        WsEvent::Disconnected(reason) => {
                            eprintln!("[StandX] Disconnected: {}", reason);
                        }
                        _ => {}
                    }
                }
            }
        }

        // Print comparison every interval
        if last_print.elapsed() >= print_interval && binance_obi.is_warmed_up() {
            let elapsed = start.elapsed().as_secs();
            let alpha_diff = binance_metrics.alpha - standx_metrics.alpha;

            // Track correlation
            if standx_obi.is_warmed_up() {
                correlation_tracker.push(binance_metrics.alpha, standx_metrics.alpha);
            }

            println!("{:>6} | {:>10.2} {:>8.2} {:>8.3} {:>8.3} | {:>10.2} {:>8.2} {:>8.3} {:>8.3} | {:>+8.3}",
                     elapsed,
                     binance_metrics.mid_price,
                     binance_metrics.spread,
                     binance_metrics.alpha,
                     binance_metrics.vol_bps,
                     standx_metrics.mid_price,
                     standx_metrics.spread,
                     standx_metrics.alpha,
                     standx_metrics.vol_bps,
                     alpha_diff);

            last_print = Instant::now();
        }
    }

    // Stop clients
    binance_client.stop();
    standx_client.stop();

    // Print summary
    println!("\n{}", "=".repeat(110));
    println!("=== Summary ===\n");

    println!("{:20} {:>15} {:>15}", "", "Binance", "StandX");
    println!("{}", "-".repeat(52));
    println!("{:20} {:>15} {:>15}", "Total messages:", binance_count, standx_count);
    println!("{:20} {:>15} {:>15}", "Samples collected:", binance_obi.sample_count(), standx_obi.sample_count());
    println!("{:20} {:>15} {:>15}", "Warmed up:", binance_obi.is_warmed_up(), standx_obi.is_warmed_up());
    println!("{:20} {:>15.4} {:>15.4}", "Final alpha:", binance_obi.alpha(), standx_obi.alpha());
    println!("{:20} {:>15.4} {:>15.4}", "Final vol (bps):", binance_obi.volatility() * 10000.0, standx_obi.volatility() * 10000.0);
    println!("{:20} {:>15.2} {:>15.2}", "Final mid price:", binance_metrics.mid_price, standx_metrics.mid_price);
    println!("{:20} {:>15.2} {:>15.2}", "Final spread:", binance_metrics.spread, standx_metrics.spread);

    // Calculation consistency analysis
    println!("\n=== Calculation Consistency Analysis ===\n");
    println!("Both exchanges use the same OBI formula:");
    println!("  Imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol) within {}% of mid", looking_depth * 100.0);
    println!("  Alpha = z-score of imbalance over {} sample window\n", window_size);

    println!("Correlation between Binance and StandX alpha:");
    println!("  Samples compared: {}", correlation_tracker.sample_count());
    println!("  Pearson correlation: {:.4}", correlation_tracker.correlation());
    println!("  Mean difference (Binance - StandX): {:+.4}", correlation_tracker.mean_diff());
    println!("  Std dev of difference: {:.4}", correlation_tracker.std_diff());

    let correlation = correlation_tracker.correlation();
    if correlation > 0.7 {
        println!("\n  -> STRONG positive correlation: Calculations are consistent!");
    } else if correlation > 0.3 {
        println!("\n  -> MODERATE positive correlation: Some consistency.");
    } else if correlation > -0.3 {
        println!("\n  -> WEAK/NO correlation: Markets may be moving independently.");
    } else {
        println!("\n  -> NEGATIVE correlation: Unexpected - check calculation logic.");
    }

    // Sanity check results
    println!("\n=== Sanity Check Results ===\n");
    println!("{:30} {:>15} {:>15}", "", "Binance", "StandX");
    println!("{}", "-".repeat(62));
    println!("{:30} {:>15} {:>15}", "Total checks:", binance_sanity.total_checks, standx_sanity.total_checks);
    println!("{:30} {:>15} {:>15}", "Failed checks:", binance_sanity.failed_checks, standx_sanity.failed_checks);
    println!("{:30} {:>14.1}% {:>14.1}%", "Pass rate:", binance_sanity.pass_rate(), standx_sanity.pass_rate());
    println!("{:30} {:>15} {:>15}", "Bids sorted (descending):", binance_sanity.bids_ordered, standx_sanity.bids_ordered);
    println!("{:30} {:>15} {:>15}", "Asks sorted (ascending):", binance_sanity.asks_ordered, standx_sanity.asks_ordered);
    println!("{:30} {:>15} {:>15}", "Spread positive:", binance_sanity.spread_positive, standx_sanity.spread_positive);
    println!("{:30} {:>15} {:>15}", "No crossed book:", binance_sanity.no_crossed_book, standx_sanity.no_crossed_book);
    println!("{:30} {:>15} {:>15}", "Has levels:", binance_sanity.has_levels, standx_sanity.has_levels);

    if binance_sanity.pass_rate() == 100.0 && standx_sanity.pass_rate() == 100.0 {
        println!("\n  -> ALL SANITY CHECKS PASSED for both exchanges!");
    } else {
        if binance_sanity.pass_rate() < 100.0 {
            println!("\n  -> WARNING: Binance had {:.1}% sanity check failures", 100.0 - binance_sanity.pass_rate());
        }
        if standx_sanity.pass_rate() < 100.0 {
            println!("  -> WARNING: StandX had {:.1}% sanity check failures", 100.0 - standx_sanity.pass_rate());
        }
    }

    println!("\nNote: Alpha differences are expected due to:");
    println!("  - Different orderbook depths and liquidity");
    println!("  - Both are perpetual futures but different venues/participants");
    println!("  - Different market participants and order flow");
    println!("  - Network latency differences in data arrival");

    let binance_stats = binance_client.stats().snapshot();
    println!("\nBinance connection stats:");
    println!("  Bytes received: {} KB", binance_stats.bytes_received / 1024);
    println!("  Reconnects: {}", binance_stats.reconnects);
    println!("  Sequence gaps: {}", binance_stats.sequence_gaps);

    let standx_stats = standx_client.stats().snapshot();
    println!("\nStandX connection stats:");
    println!("  Bytes received: {} KB", standx_stats.bytes_received / 1024);
    println!("  Reconnects: {}", standx_stats.reconnect_count);

    Ok(())
}
