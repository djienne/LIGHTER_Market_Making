//! Standalone test binary for leverage API.
//!
//! Tests the query_position_config and change_leverage endpoints.
//!
//! Usage:
//! ```bash
//! cargo run --bin test_leverage
//! ```

use standx_orderbook::{AuthManager, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env for credentials
    dotenvy::dotenv().ok();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Load config to get symbols
    let config = Config::from_file("config.json").unwrap_or_default();
    let symbol = config.symbols.first()
        .map(|s| s.as_str())
        .unwrap_or("BTC-USD");

    println!("Testing leverage API for symbol: {}", symbol);

    // Create auth manager from environment
    let mut auth = AuthManager::from_env()?;

    // Authenticate
    println!("Authenticating...");
    let token = auth.authenticate().await?;
    println!("Authenticated (expires in {} hours)", token.remaining_secs() / 3600);

    // Query current leverage
    println!("\nQuerying current leverage...");
    let current_leverage = auth.query_leverage(symbol).await?;
    println!("[{}] Current leverage: {}x", symbol, current_leverage);

    // If leverage != 1, change it to 1
    if current_leverage != 1 {
        println!("\n[{}] Setting leverage to 1x...", symbol);
        auth.set_leverage(symbol, 1).await?;
        println!("[{}] Leverage changed successfully!", symbol);

        // Verify the change
        println!("\nVerifying leverage change...");
        let new_leverage = auth.query_leverage(symbol).await?;
        println!("[{}] Verified leverage: {}x", symbol, new_leverage);

        if new_leverage == 1 {
            println!("\nSUCCESS: Leverage is now 1x");
        } else {
            println!("\nWARNING: Leverage is {} (expected 1)", new_leverage);
        }
    } else {
        println!("\n[{}] Leverage is already at 1x, no change needed", symbol);
    }

    println!("\nTest complete!");
    Ok(())
}
