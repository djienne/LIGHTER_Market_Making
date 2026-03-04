//! Test binary for Binance orderbook and OBI calculation.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use standx_orderbook::binance::{BinanceClient, BinanceClientConfig, BinanceEvent, BinanceObiCalculator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    println!("=== Binance Orderbook Test ===\n");

    // Create client for BTCUSDT (Futures by default)
    let config = BinanceClientConfig::futures("btcusdt");
    let client = Arc::new(BinanceClient::with_config(config.clone()));

    // Create OBI calculator with 300 sample window (30s at 100ms updates), 2.5% depth
    let mut obi = BinanceObiCalculator::new(300, 0.025);

    println!("Connecting to Binance WebSocket...");
    println!("Market type: {:?}", config.market_type);
    println!("Symbol: BTCUSDT");
    println!("Update interval: 100ms");
    println!("OBI depth: 2.5% from mid\n");

    // Start the client
    let mut rx = client.clone().run().await;

    let mut snapshot_count = 0;
    let mut last_print = std::time::Instant::now();

    // Run for 30 seconds to collect data
    let test_duration = Duration::from_secs(30);
    let start = std::time::Instant::now();

    println!("Collecting data for 30 seconds...\n");
    println!("{:>8} {:>12} {:>12} {:>10} {:>10} {:>8} {:>8}",
             "Count", "Mid Price", "Spread", "Alpha", "Vol (bps)", "Bids", "Asks");
    println!("{}", "-".repeat(78));

    while start.elapsed() < test_duration {
        match timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(event)) => {
                match event {
                    BinanceEvent::Snapshot(snapshot) => {
                        snapshot_count += 1;

                        // Update OBI calculator
                        let obi_result = obi.update(&snapshot);

                        // Print every 100ms worth of updates (roughly every second)
                        if last_print.elapsed() >= Duration::from_secs(1) {
                            let mid = snapshot.mid_price().unwrap_or(0.0);
                            let spread = snapshot.spread().unwrap_or(0.0);
                            let spread_bps = snapshot.spread_bps().unwrap_or(0.0);

                            let (alpha, vol) = obi_result.unwrap_or((0.0, 0.0));
                            let vol_bps = vol * 10000.0; // Convert to basis points

                            println!("{:>8} {:>12.2} {:>12.2} {:>10.4} {:>10.4} {:>8} {:>8}",
                                     snapshot_count,
                                     mid,
                                     spread,
                                     alpha,
                                     vol_bps,
                                     snapshot.bid_count,
                                     snapshot.ask_count);

                            last_print = std::time::Instant::now();
                        }
                    }
                    BinanceEvent::Synced => {
                        println!("\n[SYNCED] WebSocket synchronized with orderbook\n");
                    }
                    BinanceEvent::Disconnected(reason) => {
                        println!("\n[DISCONNECTED] {}\n", reason);
                    }
                    BinanceEvent::Error(e) => {
                        println!("\n[ERROR] {}\n", e);
                    }
                }
            }
            Ok(None) => {
                println!("Channel closed");
                break;
            }
            Err(_) => {
                println!("Timeout waiting for event");
            }
        }
    }

    // Stop the client
    client.stop();

    // Print summary
    println!("\n{}", "=".repeat(78));
    println!("=== Summary ===\n");

    let stats = client.stats().snapshot();
    println!("Total snapshots received: {}", snapshot_count);
    println!("Messages received: {}", stats.messages_received);
    println!("Bytes received: {} KB", stats.bytes_received / 1024);
    println!("Snapshots fetched (REST): {}", stats.snapshots_fetched);
    println!("Reconnects: {}", stats.reconnects);
    println!("Sequence gaps: {}", stats.sequence_gaps);

    println!("\nOBI Calculator:");
    println!("  Samples collected: {}", obi.sample_count());
    println!("  Warmed up: {}", obi.is_warmed_up());
    println!("  Final alpha: {:.4}", obi.alpha());
    println!("  Final volatility: {:.6} ({:.2} bps)", obi.volatility(), obi.volatility() * 10000.0);

    Ok(())
}
