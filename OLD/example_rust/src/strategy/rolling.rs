//! Rolling window statistics with O(1) incremental updates.
//!
//! Provides efficient calculation of mean, standard deviation, and z-score
//! over a fixed-size rolling window using incremental sum tracking.

/// A ring buffer for storing rolling values.
///
/// Designed for single-threaded use (no atomics needed since always &mut self).
#[derive(Debug)]
pub struct RollingWindow {
    /// Pre-allocated buffer of values
    buffer: Box<[f64]>,
    /// Buffer capacity (window size)
    capacity: usize,
    /// Current write position (wraps around)
    write_pos: usize,
    /// Number of values written (capped at capacity)
    count: usize,
}

impl RollingWindow {
    /// Create a new rolling window with the specified capacity.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");

        let buffer: Vec<f64> = vec![0.0; capacity];

        Self {
            buffer: buffer.into_boxed_slice(),
            capacity,
            write_pos: 0,
            count: 0,
        }
    }

    /// Push a new value into the window.
    ///
    /// Returns the old value that was overwritten (if buffer is full).
    #[inline]
    pub fn push(&mut self, value: f64) -> Option<f64> {
        let idx = self.write_pos % self.capacity;

        let old_value = if self.count >= self.capacity {
            Some(self.buffer[idx])
        } else {
            None
        };

        self.buffer[idx] = value;
        self.write_pos = self.write_pos.wrapping_add(1);

        if self.count < self.capacity {
            self.count += 1;
        }

        old_value
    }

    /// Get the number of values in the window.
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if the window is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Check if the window is full (warmed up).
    #[inline]
    pub fn is_full(&self) -> bool {
        self.count >= self.capacity
    }

    /// Get the buffer capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the most recent value.
    #[inline]
    pub fn latest(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let idx = (self.write_pos + self.capacity - 1) % self.capacity;
        Some(self.buffer[idx])
    }

    /// Iterate over values from oldest to newest.
    pub fn iter(&self) -> impl Iterator<Item = f64> + '_ {
        let start = if self.count >= self.capacity {
            self.write_pos % self.capacity
        } else {
            0
        };

        (0..self.count).map(move |i| {
            let idx = (start + i) % self.capacity;
            self.buffer[idx]
        })
    }

    /// Clear the window.
    pub fn clear(&mut self) {
        self.write_pos = 0;
        self.count = 0;
    }
}

/// Rolling statistics calculator with O(1) incremental updates.
///
/// Uses the formula: Var(X) = E[X²] - E[X]²
/// This allows O(1) updates by tracking sum and sum of squares.
#[derive(Debug)]
pub struct RollingStats {
    /// Rolling window of values (needed for old value retrieval)
    window: RollingWindow,
    /// Running sum of values: Σx
    sum: f64,
    /// Running sum of squared values: Σx²
    sum_sq: f64,
    /// Cached standard deviation (avoids sqrt() on every zscore call)
    cached_std: f64,
    /// Cached mean (for zscore calculation)
    cached_mean: f64,
}

impl RollingStats {
    /// Create a new rolling statistics calculator.
    pub fn new(capacity: usize) -> Self {
        Self {
            window: RollingWindow::new(capacity),
            sum: 0.0,
            sum_sq: 0.0,
            cached_std: 0.0,
            cached_mean: 0.0,
        }
    }

    /// Push a new value and update statistics in O(1).
    #[inline]
    pub fn push(&mut self, value: f64) {
        let old_value = self.window.push(value);

        // Add new value's contribution
        self.sum += value;
        self.sum_sq += value * value;

        // Remove old value's contribution (if window was full)
        if let Some(old_val) = old_value {
            self.sum -= old_val;
            self.sum_sq -= old_val * old_val;
        }

        // Update cached statistics (avoids sqrt() on every zscore call)
        let n = self.window.len();
        if n >= 2 {
            self.cached_mean = self.sum / n as f64;
            let mean_sq = self.sum_sq / n as f64;
            let variance = (mean_sq - self.cached_mean * self.cached_mean).max(0.0);
            self.cached_std = variance.sqrt();
        } else if n == 1 {
            self.cached_mean = self.sum;
            self.cached_std = 0.0;
        }
    }

    /// Get the current mean.
    #[inline]
    pub fn mean(&self) -> f64 {
        let n = self.window.len();
        if n == 0 {
            return 0.0;
        }
        self.sum / n as f64
    }

    /// Get the current variance using E[X²] - E[X]².
    #[inline]
    pub fn variance(&self) -> f64 {
        let n = self.window.len();
        if n < 2 {
            return 0.0;
        }
        let mean = self.sum / n as f64;
        let mean_sq = self.sum_sq / n as f64;
        // Clamp to 0 for numerical stability (can go slightly negative due to float errors)
        (mean_sq - mean * mean).max(0.0)
    }

    /// Get the current standard deviation.
    #[inline]
    pub fn std(&self) -> f64 {
        self.variance().sqrt()
    }

    /// Calculate z-score for a value using cached statistics.
    /// This avoids the expensive sqrt() call on every quote.
    #[inline]
    pub fn zscore(&self, value: f64) -> f64 {
        if self.cached_std < 1e-10 {
            return 0.0;
        }
        (value - self.cached_mean) / self.cached_std
    }

    /// Get the number of values in the window.
    #[inline]
    pub fn len(&self) -> usize {
        self.window.len()
    }

    /// Check if the window is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.window.is_empty()
    }

    /// Check if the window is full (warmed up).
    #[inline]
    pub fn is_full(&self) -> bool {
        self.window.is_full()
    }

    /// Get the buffer capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.window.capacity()
    }

    /// Get the most recent value.
    #[inline]
    pub fn latest(&self) -> Option<f64> {
        self.window.latest()
    }

    /// Clear the statistics.
    pub fn clear(&mut self) {
        self.window.clear();
        self.sum = 0.0;
        self.sum_sq = 0.0;
        self.cached_std = 0.0;
        self.cached_mean = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rolling_window_basic() {
        let mut window = RollingWindow::new(5);

        assert!(window.is_empty());
        assert!(!window.is_full());

        window.push(1.0);
        window.push(2.0);
        window.push(3.0);

        assert_eq!(window.len(), 3);
        assert!(!window.is_full());
        assert_eq!(window.latest(), Some(3.0));
    }

    #[test]
    fn test_rolling_window_wrap() {
        let mut window = RollingWindow::new(3);

        window.push(1.0);
        window.push(2.0);
        window.push(3.0);

        assert!(window.is_full());

        // This should overwrite 1.0
        let old = window.push(4.0);
        assert_eq!(old, Some(1.0));
        assert_eq!(window.latest(), Some(4.0));

        let values: Vec<f64> = window.iter().collect();
        assert_eq!(values, vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_rolling_stats_mean() {
        let mut stats = RollingStats::new(5);

        stats.push(1.0);
        stats.push(2.0);
        stats.push(3.0);
        stats.push(4.0);
        stats.push(5.0);

        assert!((stats.mean() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_rolling_stats_std() {
        let mut stats = RollingStats::new(5);

        // Values: 1, 2, 3, 4, 5
        // Mean: 3
        // Variance: ((1-3)^2 + (2-3)^2 + (3-3)^2 + (4-3)^2 + (5-3)^2) / 5 = 2
        // Std: sqrt(2) ≈ 1.414
        stats.push(1.0);
        stats.push(2.0);
        stats.push(3.0);
        stats.push(4.0);
        stats.push(5.0);

        assert!((stats.variance() - 2.0).abs() < 1e-10);
        assert!((stats.std() - 2.0_f64.sqrt()).abs() < 1e-10);
    }

    #[test]
    fn test_rolling_stats_zscore() {
        let mut stats = RollingStats::new(5);

        stats.push(1.0);
        stats.push(2.0);
        stats.push(3.0);
        stats.push(4.0);
        stats.push(5.0);

        // Mean = 3, Std = sqrt(2)
        // zscore(3) = (3 - 3) / sqrt(2) = 0
        // zscore(5) = (5 - 3) / sqrt(2) = sqrt(2)
        assert!((stats.zscore(3.0)).abs() < 1e-10);
        assert!((stats.zscore(5.0) - 2.0_f64.sqrt()).abs() < 1e-10);
    }

    #[test]
    fn test_rolling_stats_with_wrap() {
        let mut stats = RollingStats::new(3);

        stats.push(1.0);
        stats.push(2.0);
        stats.push(3.0);

        // Mean of [1, 2, 3] = 2
        assert!((stats.mean() - 2.0).abs() < 1e-10);

        stats.push(4.0);  // Now [2, 3, 4]

        // Mean of [2, 3, 4] = 3
        assert!((stats.mean() - 3.0).abs() < 1e-10);

        stats.push(5.0);  // Now [3, 4, 5]

        // Mean of [3, 4, 5] = 4
        assert!((stats.mean() - 4.0).abs() < 1e-10);
    }
}
