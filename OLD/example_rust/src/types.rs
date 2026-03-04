//! Core data types for the StandX orderbook parser.
//!
//! These types are designed for high performance with minimal allocations:
//! - Fixed-size arrays for price levels
//! - Copy semantics where possible
//! - No heap allocations in hot paths

use std::fmt;

/// Maximum number of price levels supported.
/// This is a compile-time constant to enable fixed-size arrays.
pub const MAX_LEVELS: usize = 50;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PriceOrder {
    Ascending,
    Descending,
    Unknown,
}

/// A single price level in the orderbook.
///
/// Uses f64 for performance. For applications requiring exact decimal
/// precision, consider using rust_decimal in the parsing layer and
/// converting to f64 only for calculations.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
#[repr(C)]
pub struct PriceLevel {
    pub price: f64,
    pub quantity: f64,
}

impl PriceLevel {
    /// Create a new price level.
    #[inline]
    pub const fn new(price: f64, quantity: f64) -> Self {
        Self { price, quantity }
    }

    /// Check if this level is empty (zero price and quantity).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.price == 0.0 && self.quantity == 0.0
    }

    /// Calculate the notional value (price * quantity).
    #[inline]
    pub fn notional(&self) -> f64 {
        self.price * self.quantity
    }
}

/// Fixed-size symbol string to avoid heap allocations.
/// Supports symbols up to 15 characters (plus null terminator).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Symbol {
    data: [u8; 16],
    len: u8,
}

impl Symbol {
    /// Create a new symbol from a string slice.
    /// Truncates if longer than 15 characters.
    pub fn new(s: &str) -> Self {
        let bytes = s.as_bytes();
        let len = bytes.len().min(15) as u8;
        let mut data = [0u8; 16];
        data[..len as usize].copy_from_slice(&bytes[..len as usize]);
        Self { data, len }
    }

    /// Get the symbol as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: We only store valid UTF-8 from the constructor
        unsafe { std::str::from_utf8_unchecked(&self.data[..self.len as usize]) }
    }

    /// Get the length of the symbol.
    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Check if the symbol is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for Symbol {
    fn default() -> Self {
        Self {
            data: [0u8; 16],
            len: 0,
        }
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Symbol(\"{}\")", self.as_str())
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl AsRef<str> for Symbol {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// A complete orderbook snapshot at a point in time.
///
/// This struct is designed for lock-free operations:
/// - Fixed-size arrays (no heap allocation)
/// - Copy-friendly layout
/// - Sequence number for consistency checks
#[derive(Clone)]
pub struct OrderbookSnapshot {
    /// Trading symbol (e.g., "TEST-USD")
    pub symbol: Symbol,

    /// Server timestamp in nanoseconds since Unix epoch
    pub timestamp_ns: i64,

    /// Local receive time in nanoseconds since Unix epoch
    pub received_at_ns: i64,

    /// Exchange sequence number for ordering
    pub sequence: u64,

    /// Bid levels, sorted descending by price (best bid first)
    pub bids: [PriceLevel; MAX_LEVELS],

    /// Ask levels, sorted ascending by price (best ask first)
    pub asks: [PriceLevel; MAX_LEVELS],

    /// Number of valid bid levels
    pub bid_count: u8,

    /// Number of valid ask levels
    pub ask_count: u8,
}

impl Default for OrderbookSnapshot {
    fn default() -> Self {
        Self {
            symbol: Symbol::default(),
            timestamp_ns: 0,
            received_at_ns: 0,
            sequence: 0,
            bids: [PriceLevel::default(); MAX_LEVELS],
            asks: [PriceLevel::default(); MAX_LEVELS],
            bid_count: 0,
            ask_count: 0,
        }
    }
}

impl OrderbookSnapshot {
    /// Create a new empty orderbook snapshot.
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            ..Default::default()
        }
    }

    /// Get the best bid price and quantity, if available.
    #[inline]
    pub fn best_bid(&self) -> Option<PriceLevel> {
        if self.bid_count > 0 {
            Some(self.bids[0])
        } else {
            None
        }
    }

    /// Get the best ask price and quantity, if available.
    #[inline]
    pub fn best_ask(&self) -> Option<PriceLevel> {
        if self.ask_count > 0 {
            Some(self.asks[0])
        } else {
            None
        }
    }

    /// Get the best bid price, if available.
    #[inline]
    pub fn best_bid_price(&self) -> Option<f64> {
        self.best_bid().map(|l| l.price)
    }

    /// Get the best ask price, if available.
    #[inline]
    pub fn best_ask_price(&self) -> Option<f64> {
        self.best_ask().map(|l| l.price)
    }

    /// Calculate the bid-ask spread.
    #[inline]
    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid_price(), self.best_ask_price()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    /// Calculate the mid price.
    #[inline]
    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid_price(), self.best_ask_price()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }

    /// Calculate the spread as a percentage of the mid price.
    #[inline]
    pub fn spread_bps(&self) -> Option<f64> {
        match (self.spread(), self.mid_price()) {
            (Some(spread), Some(mid)) if mid > 0.0 => Some(spread / mid * 10000.0),
            _ => None,
        }
    }

    /// Get bid levels as a slice.
    #[inline]
    pub fn bid_levels(&self) -> &[PriceLevel] {
        &self.bids[..self.bid_count as usize]
    }

    /// Get ask levels as a slice.
    #[inline]
    pub fn ask_levels(&self) -> &[PriceLevel] {
        &self.asks[..self.ask_count as usize]
    }

    /// Calculate total bid volume across all levels.
    pub fn total_bid_volume(&self) -> f64 {
        self.bid_levels().iter().map(|l| l.quantity).sum()
    }

    /// Calculate total ask volume across all levels.
    pub fn total_ask_volume(&self) -> f64 {
        self.ask_levels().iter().map(|l| l.quantity).sum()
    }

    /// Calculate volume imbalance: (bid_volume - ask_volume) / (bid_volume + ask_volume)
    pub fn volume_imbalance(&self) -> Option<f64> {
        let bid_vol = self.total_bid_volume();
        let ask_vol = self.total_ask_volume();
        let total = bid_vol + ask_vol;
        if total > 0.0 {
            Some((bid_vol - ask_vol) / total)
        } else {
            None
        }
    }

    /// Check if the orderbook is valid (has both bids and asks, not crossed).
    pub fn is_valid(&self) -> bool {
        if self.bid_count == 0 || self.ask_count == 0 {
            return false;
        }

        // Check for crossed book
        if let (Some(bid), Some(ask)) = (self.best_bid_price(), self.best_ask_price()) {
            if bid >= ask {
                return false;
            }
        }

        true
    }

    /// Validate orderbook integrity and return any issues found.
    ///
    /// Checks:
    /// 1. Bids are sorted descending (best bid first)
    /// 2. Asks are sorted ascending (best ask first)
    /// 3. No crossed book (best_bid < best_ask)
    /// 4. No zero or negative prices
    pub fn validate(&self) -> Result<(), String> {
        // Check bids are descending
        let bid_levels = self.bid_levels();
        for i in 1..bid_levels.len() {
            if bid_levels[i].price > bid_levels[i - 1].price {
                return Err(format!(
                    "Bids not sorted descending: bid[{}]={:.2} > bid[{}]={:.2}",
                    i, bid_levels[i].price, i - 1, bid_levels[i - 1].price
                ));
            }
            if bid_levels[i].price <= 0.0 {
                return Err(format!("Invalid bid price at level {}: {:.2}", i, bid_levels[i].price));
            }
        }

        // Check asks are ascending
        let ask_levels = self.ask_levels();
        for i in 1..ask_levels.len() {
            if ask_levels[i].price < ask_levels[i - 1].price {
                return Err(format!(
                    "Asks not sorted ascending: ask[{}]={:.2} < ask[{}]={:.2}",
                    i, ask_levels[i].price, i - 1, ask_levels[i - 1].price
                ));
            }
            if ask_levels[i].price <= 0.0 {
                return Err(format!("Invalid ask price at level {}: {:.2}", i, ask_levels[i].price));
            }
        }

        // Check for crossed book
        if let (Some(bid), Some(ask)) = (self.best_bid_price(), self.best_ask_price()) {
            if bid >= ask {
                return Err(format!(
                    "Crossed book: best_bid={:.2} >= best_ask={:.2}",
                    bid, ask
                ));
            }
        }

        Ok(())
    }

    /// Debug helper: dump first N levels of the orderbook.
    pub fn debug_levels(&self, n: usize) -> String {
        let mut s = String::new();
        s.push_str(&format!("Bids ({} levels): ", self.bid_count));
        for (i, level) in self.bid_levels().iter().take(n).enumerate() {
            if i > 0 { s.push_str(", "); }
            s.push_str(&format!("{:.2}@{:.4}", level.price, level.quantity));
        }
        s.push_str(&format!(" | Asks ({} levels): ", self.ask_count));
        for (i, level) in self.ask_levels().iter().take(n).enumerate() {
            if i > 0 { s.push_str(", "); }
            s.push_str(&format!("{:.2}@{:.4}", level.price, level.quantity));
        }
        s
    }

    /// Set bid levels from a slice, sorting by price descending.
    ///
    /// Uses in-place sorting on fixed arrays to avoid heap allocation.
    pub fn set_bids(&mut self, levels: &[(f64, f64)], max_levels: usize) {
        // Copy valid levels directly into fixed array (no allocation)
        let mut count = 0;
        let limit = max_levels.min(MAX_LEVELS);
        for (price, qty) in levels.iter() {
            if *price > 0.0 && *qty > 0.0 && count < limit {
                self.bids[count] = PriceLevel::new(*price, *qty);
                count += 1;
            }
        }
        self.bid_count = count as u8;

        // Sort in-place (descending by price for bids)
        self.bids[..count].sort_by(|a, b| {
            b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Zero out remaining levels
        for i in count..MAX_LEVELS {
            self.bids[i] = PriceLevel::default();
        }
    }

    /// Set ask levels from a slice, sorting by price ascending.
    ///
    /// Uses in-place sorting on fixed arrays to avoid heap allocation.
    pub fn set_asks(&mut self, levels: &[(f64, f64)], max_levels: usize) {
        // Copy valid levels directly into fixed array (no allocation)
        let mut count = 0;
        let limit = max_levels.min(MAX_LEVELS);
        for (price, qty) in levels.iter() {
            if *price > 0.0 && *qty > 0.0 && count < limit {
                self.asks[count] = PriceLevel::new(*price, *qty);
                count += 1;
            }
        }
        self.ask_count = count as u8;

        // Sort in-place (ascending by price for asks)
        self.asks[..count].sort_by(|a, b| {
            a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Zero out remaining levels
        for i in count..MAX_LEVELS {
            self.asks[i] = PriceLevel::default();
        }
    }

    #[inline]
    fn detect_price_order(levels: &[(String, String)]) -> PriceOrder {
        let mut prev: Option<f64> = None;
        let mut direction = PriceOrder::Unknown;

        for (price_str, _) in levels.iter() {
            let price = match fast_float::parse::<f64, _>(price_str) {
                Ok(price) if price > 0.0 => price,
                _ => continue,
            };

            if let Some(prev_price) = prev {
                if direction == PriceOrder::Unknown {
                    if price > prev_price {
                        direction = PriceOrder::Ascending;
                    } else if price < prev_price {
                        direction = PriceOrder::Descending;
                    }
                } else if (direction == PriceOrder::Ascending && price < prev_price)
                    || (direction == PriceOrder::Descending && price > prev_price)
                {
                    return PriceOrder::Unknown;
                }
            }

            prev = Some(price);
        }

        direction
    }

    /// Set bid levels directly from string slices, parsing in-place.
    ///
    /// This avoids intermediate Vec allocation by parsing directly into fixed arrays.
    /// Detects ascending vs descending input ordering to keep best levels
    /// even if the feed flips its sort direction.
    /// Sorts bids descending (best bid first) to ensure correctness.
    #[inline]
    pub fn set_bids_from_strings(&mut self, levels: &[(String, String)], max_levels: usize) {
        let order = Self::detect_price_order(levels);
        let mut count = 0;
        let limit = max_levels.min(MAX_LEVELS);
        match order {
            PriceOrder::Ascending => {
                // Bids arrive low->high, so iterate in reverse to get best bids first.
                for (price_str, qty_str) in levels.iter().rev() {
                    if count >= limit {
                        break;
                    }
                    if let (Ok(price), Ok(qty)) = (
                        fast_float::parse::<f64, _>(price_str),
                        fast_float::parse::<f64, _>(qty_str),
                    ) {
                        if price > 0.0 && qty > 0.0 {
                            self.bids[count] = PriceLevel::new(price, qty);
                            count += 1;
                        }
                    }
                }
            }
            PriceOrder::Descending => {
                // Bids arrive high->low, so iterate in order to keep best bids.
                for (price_str, qty_str) in levels.iter() {
                    if count >= limit {
                        break;
                    }
                    if let (Ok(price), Ok(qty)) = (
                        fast_float::parse::<f64, _>(price_str),
                        fast_float::parse::<f64, _>(qty_str),
                    ) {
                        if price > 0.0 && qty > 0.0 {
                            self.bids[count] = PriceLevel::new(price, qty);
                            count += 1;
                        }
                    }
                }
            }
            PriceOrder::Unknown => {
                for (price_str, qty_str) in levels.iter() {
                    if let (Ok(price), Ok(qty)) = (
                        fast_float::parse::<f64, _>(price_str),
                        fast_float::parse::<f64, _>(qty_str),
                    ) {
                        if price > 0.0 && qty > 0.0 {
                            if count < limit {
                                self.bids[count] = PriceLevel::new(price, qty);
                                count += 1;
                            } else if limit > 0 {
                                let mut worst_idx = 0;
                                let mut worst_price = self.bids[0].price;
                                for i in 1..count {
                                    if self.bids[i].price < worst_price {
                                        worst_price = self.bids[i].price;
                                        worst_idx = i;
                                    }
                                }
                                if price > worst_price {
                                    self.bids[worst_idx] = PriceLevel::new(price, qty);
                                }
                            }
                        }
                    }
                }
            }
        }
        self.bid_count = count as u8;

        // Sort bids descending (best bid = highest price first)
        self.bids[..count].sort_by(|a, b| {
            b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Zero out remaining levels
        for i in count..MAX_LEVELS {
            self.bids[i] = PriceLevel::default();
        }
    }

    /// Set ask levels directly from string slices, parsing in-place.
    ///
    /// This avoids intermediate Vec allocation by parsing directly into fixed arrays.
    /// Detects ascending vs descending input ordering to keep best levels
    /// even if the feed flips its sort direction.
    /// Sorts asks ascending (best ask first) to ensure correctness.
    #[inline]
    pub fn set_asks_from_strings(&mut self, levels: &[(String, String)], max_levels: usize) {
        let order = Self::detect_price_order(levels);
        let mut count = 0;
        let limit = max_levels.min(MAX_LEVELS);
        match order {
            PriceOrder::Ascending => {
                for (price_str, qty_str) in levels.iter() {
                    if count >= limit {
                        break;
                    }
                    if let (Ok(price), Ok(qty)) = (
                        fast_float::parse::<f64, _>(price_str),
                        fast_float::parse::<f64, _>(qty_str),
                    ) {
                        if price > 0.0 && qty > 0.0 {
                            self.asks[count] = PriceLevel::new(price, qty);
                            count += 1;
                        }
                    }
                }
            }
            PriceOrder::Descending => {
                for (price_str, qty_str) in levels.iter().rev() {
                    if count >= limit {
                        break;
                    }
                    if let (Ok(price), Ok(qty)) = (
                        fast_float::parse::<f64, _>(price_str),
                        fast_float::parse::<f64, _>(qty_str),
                    ) {
                        if price > 0.0 && qty > 0.0 {
                            self.asks[count] = PriceLevel::new(price, qty);
                            count += 1;
                        }
                    }
                }
            }
            PriceOrder::Unknown => {
                for (price_str, qty_str) in levels.iter() {
                    if let (Ok(price), Ok(qty)) = (
                        fast_float::parse::<f64, _>(price_str),
                        fast_float::parse::<f64, _>(qty_str),
                    ) {
                        if price > 0.0 && qty > 0.0 {
                            if count < limit {
                                self.asks[count] = PriceLevel::new(price, qty);
                                count += 1;
                            } else if limit > 0 {
                                let mut worst_idx = 0;
                                let mut worst_price = self.asks[0].price;
                                for i in 1..count {
                                    if self.asks[i].price > worst_price {
                                        worst_price = self.asks[i].price;
                                        worst_idx = i;
                                    }
                                }
                                if price < worst_price {
                                    self.asks[worst_idx] = PriceLevel::new(price, qty);
                                }
                            }
                        }
                    }
                }
            }
        }
        self.ask_count = count as u8;

        // Sort asks ascending (best ask = lowest price first)
        self.asks[..count].sort_by(|a, b| {
            a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Zero out remaining levels
        for i in count..MAX_LEVELS {
            self.asks[i] = PriceLevel::default();
        }
    }
}

impl fmt::Debug for OrderbookSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderbookSnapshot")
            .field("symbol", &self.symbol)
            .field("sequence", &self.sequence)
            .field("bid_count", &self.bid_count)
            .field("ask_count", &self.ask_count)
            .field("best_bid", &self.best_bid_price())
            .field("best_ask", &self.best_ask_price())
            .field("spread", &self.spread())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SYMBOL: &str = "TST-USD";

    #[test]
    fn test_price_level() {
        let level = PriceLevel::new(100.0, 1.5);
        assert_eq!(level.notional(), 150.0);
        assert!(!level.is_empty());

        let empty = PriceLevel::default();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_symbol() {
        let symbol = Symbol::new(TEST_SYMBOL);
        assert_eq!(symbol.as_str(), TEST_SYMBOL);
        assert_eq!(symbol.len(), 7);

        // Test truncation
        let long = Symbol::new("VERYLONGSYMBOLNAME");
        assert_eq!(long.len(), 15);
    }

    #[test]
    fn test_orderbook_snapshot() {
        let mut ob = OrderbookSnapshot::new(Symbol::new(TEST_SYMBOL));

        ob.set_bids(&[(100.0, 1.0), (99.0, 2.0), (98.0, 3.0)], 20);
        ob.set_asks(&[(101.0, 1.0), (102.0, 2.0), (103.0, 3.0)], 20);

        assert_eq!(ob.best_bid_price(), Some(100.0));
        assert_eq!(ob.best_ask_price(), Some(101.0));
        assert_eq!(ob.spread(), Some(1.0));
        assert_eq!(ob.mid_price(), Some(100.5));
        assert!(ob.is_valid());
    }

    #[test]
    fn test_set_bids_from_strings_descending() {
        let mut ob = OrderbookSnapshot::new(Symbol::new(TEST_SYMBOL));
        let bids = vec![
            ("100.0".to_string(), "1.0".to_string()),
            ("99.0".to_string(), "1.0".to_string()),
            ("98.0".to_string(), "1.0".to_string()),
        ];

        ob.set_bids_from_strings(&bids, 2);

        let bid_levels = ob.bid_levels();
        assert_eq!(bid_levels.len(), 2);
        assert_eq!(bid_levels[0].price, 100.0);
        assert_eq!(bid_levels[1].price, 99.0);
    }

    #[test]
    fn test_set_asks_from_strings_descending() {
        let mut ob = OrderbookSnapshot::new(Symbol::new(TEST_SYMBOL));
        let asks = vec![
            ("103.0".to_string(), "1.0".to_string()),
            ("102.0".to_string(), "1.0".to_string()),
            ("101.0".to_string(), "1.0".to_string()),
        ];

        ob.set_asks_from_strings(&asks, 2);

        let ask_levels = ob.ask_levels();
        assert_eq!(ask_levels.len(), 2);
        assert_eq!(ask_levels[0].price, 101.0);
        assert_eq!(ask_levels[1].price, 102.0);
    }

    #[test]
    fn test_set_bids_from_strings_unsorted() {
        let mut ob = OrderbookSnapshot::new(Symbol::new(TEST_SYMBOL));
        let bids = vec![
            ("99.0".to_string(), "1.0".to_string()),
            ("101.0".to_string(), "1.0".to_string()),
            ("100.0".to_string(), "1.0".to_string()),
        ];

        ob.set_bids_from_strings(&bids, 2);

        let bid_levels = ob.bid_levels();
        assert_eq!(bid_levels.len(), 2);
        assert_eq!(bid_levels[0].price, 101.0);
        assert_eq!(bid_levels[1].price, 100.0);
    }

    #[test]
    fn test_set_asks_from_strings_unsorted() {
        let mut ob = OrderbookSnapshot::new(Symbol::new(TEST_SYMBOL));
        let asks = vec![
            ("102.0".to_string(), "1.0".to_string()),
            ("100.0".to_string(), "1.0".to_string()),
            ("101.0".to_string(), "1.0".to_string()),
        ];

        ob.set_asks_from_strings(&asks, 2);

        let ask_levels = ob.ask_levels();
        assert_eq!(ask_levels.len(), 2);
        assert_eq!(ask_levels[0].price, 100.0);
        assert_eq!(ask_levels[1].price, 101.0);
    }
}
