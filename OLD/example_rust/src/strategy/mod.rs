//! Market making strategy module.
//!
//! This module provides quote calculation strategies for market making.
//! Strategies take orderbook snapshots and output bid/ask quotes.
//!
//! ## Architecture
//!
//! - [`QuoteStrategy`]: Trait defining the strategy interface
//! - [`ObiStrategy`]: OBI (Order Book Imbalance) implementation
//! - [`Quote`]: Output quote with bid/ask prices
//!
//! ## Adding New Strategies
//!
//! To implement a new strategy:
//! 1. Create a new module (e.g., `my_strategy.rs`)
//! 2. Implement the [`QuoteStrategy`] trait
//! 3. Export from this module
//! 4. Use in `main.rs` by changing the strategy type

mod traits;
mod rolling;
mod obi;
mod quotes;

pub use traits::QuoteStrategy;
pub use rolling::{RollingWindow, RollingStats};
pub use obi::ObiStrategy;
pub use quotes::{Quote, QuoteFormatter, MAX_ORDER_LEVELS};
