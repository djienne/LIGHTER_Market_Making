"""
Avellaneda-Stoikov Market Making Spread Calculator
Data-driven approach: all parameters derived from historical data via backtesting
"""

import pandas as pd
import numpy as np
import logging
import json
import argparse
from dataclasses import dataclass, asdict
from pathlib import Path
from scipy.stats import linregress
import warnings
from numba import jit
from arch import arch_model
warnings.filterwarnings('ignore')

# =============================================================================
# USER-CONFIGURABLE PARAMETERS (edit these to customize behavior)
# =============================================================================

# Trading fees
MAKER_FEE_BPS = 2.0  # Maker fee in basis points (0.015%)

# Backtesting configuration

BACKTEST_TRAIN_FRACTION = 0.7  # Fraction of window for training (rest for testing)
BASE_QUOTE_SIZE = 1.0  # Quote size in base currency
MAX_INVENTORY = 10.0  # Maximum inventory limit

# Mean-reversion horizon (tau) - baseline for calibration
TAU_SECONDS = 2.0  # Time horizon over which inventory reverts to zero

# Volatility calculation
VOL_EWMA_MAX_HALFLIFE_TICKS = 50  # Max halflife for volatility EWMA (in ticks)

# Adverse selection horizons to measure (in milliseconds)
ADVERSE_SELECTION_HORIZONS_MS = [50, 100, 200, 500, 1000, 2000, 5000]

# Fill probability fallback parameters (used if data estimation fails)
FILL_K_BID_DEFAULT = 6.9315  # Decay rate for bid fill intensity
FILL_K_ASK_DEFAULT = 6.9315  # Decay rate for ask fill intensity
FILL_A_DEFAULT = 10.0  # Base fill rate (fills per second at zero spread)

# Parameter optimization grid search (Phase 5)
# Grid will test combinations of: gamma × tau × beta_alpha
GAMMA_GRID_MULTIPLIERS = np.linspace(0.1, 2.0, 16)  # Multipliers of baseline gamma 
TAU_GRID_SECONDS = np.linspace(1.0, 10.0, 16)  # Mean-reversion horizons to test in seconds
BETA_ALPHA_GRID_MULTIPLIERS = np.linspace(0.0, 1.0, 16)  # Multipliers of initial beta_alpha
# Total grid size = len(GAMMA) × len(TAU) × len(BETA_ALPHA) combinations
# Example: 3 × 3 × 3 = 27 backtests
# To change granularity: np.linspace(0.5, 2.0, 5) = 5 points from 0.5 to 2.0

# =============================================================================

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)


@jit(nopython=True)
def _run_backtest_numba(
    mid_values,
    sigma_values,
    micro_dev_values,
    timestamp_values,
    gamma,
    tau,
    beta_alpha,
    k_bid,
    k_ask,
    A_bid,
    A_ask,
    c_AS_bid,
    c_AS_ask,
    maker_fee_bps,
    tick_size,
    base_quote_size,
    max_inventory,
    sigma_p95
):
    inventory = 0.0
    cash = 0.0
    num_fills = 0
    num_snapshots = len(mid_values)
    pnl_series = np.empty(num_snapshots - 1, dtype=np.float64)
    inventory_series = np.empty(num_snapshots - 1, dtype=np.float64)
    
    k_mid = (k_bid + k_ask) / 2
    c_AS_mid = (c_AS_bid + c_AS_ask) / 2
    Delta_bid_as = c_AS_bid - c_AS_mid
    Delta_ask_as = c_AS_ask - c_AS_mid

    for i in range(1, num_snapshots):
        mid_i = mid_values[i]
        sigma_i = sigma_values[i]
        alpha_i = micro_dev_values[i]

        r = mid_i + beta_alpha * alpha_i - gamma * (sigma_i**2) * tau * inventory

        k_mid_price_based = k_mid * 10000 / mid_i
        phi_term1 = (1 / gamma) * np.log(1 + gamma / k_mid_price_based) if gamma > 0 and k_mid_price_based > 0 else 0.01
        phi_term2 = 0.5 * gamma * (sigma_i**2) * tau
        phi_term3 = (c_AS_bid + c_AS_ask) / 2
        phi_term4 = maker_fee_bps / 10000 * mid_i
        phi = max(phi_term1 + phi_term2 + phi_term3 + phi_term4, tick_size)

        if sigma_i > sigma_p95:
            phi *= 2.0

        bid = r - phi - Delta_bid_as
        ask = r + phi - Delta_ask_as

        bid_size = base_quote_size
        ask_size = base_quote_size
        enable_bid = True
        enable_ask = True
        abs_inventory = abs(inventory)
        if abs_inventory > 0.6 * max_inventory:
            if inventory > 0:
                bid_size *= 0.5
            else:
                ask_size *= 0.5
        if abs_inventory > max_inventory:
            if inventory > 0:
                enable_bid = False
            elif inventory < 0:
                enable_ask = False

        dt = (timestamp_values[i] - timestamp_values[i-1]) / 1_000_000_000

        bid_fill_prob = 0.0
        if enable_bid:
            bid_distance_bps = ((mid_i - bid) / mid_i) * 10000 if mid_i > 0 else 0
            if bid_distance_bps >= 0:
                lambda_bid = A_bid * np.exp(-k_bid * bid_distance_bps)
                bid_fill_prob = 1 - np.exp(-lambda_bid * dt)
        
        if np.random.rand() < bid_fill_prob:
            inventory += bid_size
            cash -= bid * bid_size
            cash -= maker_fee_bps / 10000 * bid * bid_size
            num_fills += 1

        ask_fill_prob = 0.0
        if enable_ask:
            ask_distance_bps = ((ask - mid_i) / mid_i) * 10000 if mid_i > 0 else 0
            if ask_distance_bps >= 0:
                lambda_ask = A_ask * np.exp(-k_ask * ask_distance_bps)
                ask_fill_prob = 1 - np.exp(-lambda_ask * dt)

        if np.random.rand() < ask_fill_prob:
            inventory -= ask_size
            cash += ask * ask_size
            cash -= maker_fee_bps / 10000 * ask * ask_size
            num_fills += 1

        pnl = cash + inventory * mid_i
        pnl_series[i-1] = pnl
        inventory_series[i-1] = abs(inventory)

    return pnl_series, inventory_series, num_fills


@dataclass
class SpreadConfig:
    """Configuration class - only maker_fee needs to be specified by user"""
    # USER INPUT (from globals above)
    maker_fee_bps: float = MAKER_FEE_BPS

    # Backtest configuration
    backtest_train_fraction: float = BACKTEST_TRAIN_FRACTION
    base_quote_size: float = BASE_QUOTE_SIZE
    max_inventory: float = MAX_INVENTORY

    # AUTO-CALIBRATED (will be set from data)
    tick_size: float = None
    vol_halflife_sec: float = None
    gamma: float = None  # Risk aversion (optimized)
    tau: float = None  # Mean-reversion horizon (optimized)
    beta_alpha: float = None  # Alpha signal weight (optimized)
    k_bid: float = None  # Fill intensity decay (from data)
    k_ask: float = None
    A_bid: float = None  # Fill intensity base rate (from data)
    A_ask: float = None


class SpreadCalculator:
    """Main calculator class"""

    def __init__(self, config: SpreadConfig, symbol: str = "PAXG"):
        self.config = config
        self.symbol = symbol
        self.orderbook = None
        self.trades = None
        self.mid = None
        self.tick_size = None
        self.median_tick_interval = None

    def load_data(self):
        """Phase 0: Load and validate data"""
        log.info("="*70)
        log.info("PHASE 0: DATA LOADING & MARKET CHARACTERISTICS")
        log.info("="*70)

        # Load datasets
        log.info(f"Loading data files for {self.symbol}...")
        data_dir = Path("lighter_data")

        self.orderbook = pd.read_csv(data_dir / f"prices_{self.symbol}.csv")
        self.trades = pd.read_csv(data_dir / f"trades_{self.symbol}.csv")

        log.info(f"  ✓ Loaded {len(self.orderbook):,} orderbook snapshots")
        log.info(f"  ✓ Loaded {len(self.trades):,} trades")

        # Convert timestamps
        log.info("Converting timestamps...")
        self.orderbook['timestamp'] = pd.to_datetime(self.orderbook['timestamp'], format='mixed')
        self.trades['timestamp'] = pd.to_datetime(self.trades['timestamp'], format='mixed')

        # Create unix_timestamp column to prevent KeyError later
        self.orderbook['unix_timestamp'] = (self.orderbook['timestamp'].astype(np.int64) // 10**9)

        # Sort
        self.orderbook = self.orderbook.sort_values('timestamp').reset_index(drop=True)
        self.trades = self.trades.sort_values('timestamp').reset_index(drop=True)

        # Log time ranges
        log.info(f"  Orderbook: {self.orderbook['timestamp'].min()} to {self.orderbook['timestamp'].max()}")
        log.info(f"  Trades:    {self.trades['timestamp'].min()} to {self.trades['timestamp'].max()}")
        duration = (self.orderbook['timestamp'].max() - self.orderbook['timestamp'].min()).total_seconds()
        log.info(f"  Duration: {duration:.1f} seconds ({duration/3600:.2f} hours)")

    def extract_market_characteristics(self):
        """Extract basic market characteristics from data"""
        log.info("\nExtracting market characteristics...")

        self.orderbook = self.orderbook.rename(columns={
            'bid_price': 'bid_price_0',
            'bid_size': 'bid_qty_0',
            'ask_price': 'ask_price_0',
            'ask_size': 'ask_qty_0'
        })
        
        self.trades = self.trades.rename(columns={'size': 'quantity'})

        # Calculate mid price
        self.mid = (self.orderbook['bid_price_0'] + self.orderbook['ask_price_0']) / 2

        # Tick size (smallest non-zero price difference)
        price_diffs = self.orderbook['bid_price_0'].diff()
        self.tick_size = price_diffs[price_diffs > 0].min()
        self.config.tick_size = self.tick_size
        log.info(f"  ✓ Tick size: {self.tick_size}")

        # Market spread statistics
        market_spread = self.orderbook['ask_price_0'] - self.orderbook['bid_price_0']
        market_spread_bps = (market_spread / self.mid * 10000)
        log.info(f"  ✓ Market spread (price): mean={market_spread.mean():.4f}, median={market_spread.median():.4f}")
        log.info(f"  ✓ Market spread (bps):   mean={market_spread_bps.mean():.2f}, median={market_spread_bps.median():.2f}")

        # Tick frequency
        tick_intervals = self.orderbook['timestamp'].diff().dt.total_seconds()
        self.median_tick_interval = tick_intervals.median()
        log.info(f"  ✓ Median tick interval: {self.median_tick_interval*1000:.1f} ms")
        log.info(f"  ✓ Tick frequency: {1/self.median_tick_interval:.1f} Hz")

        # Trade frequency
        trade_intervals = self.trades['timestamp'].diff().dt.total_seconds()
        median_trade_interval = trade_intervals.median()
        log.info(f"  ✓ Median trade interval: {median_trade_interval*1000:.1f} ms")
        log.info(f"  ✓ Trade frequency: {1/median_trade_interval:.1f} Hz")

        # Trade size statistics
        log.info(f"  ✓ Trade size: mean={self.trades['quantity'].mean():.4f}, "
                f"median={self.trades['quantity'].median():.4f}")

        # Buy/sell distribution
        buy_trades = (self.trades['side'] == 'buy').sum()
        sell_trades = (self.trades['side'] == 'sell').sum()
        log.info(f"  ✓ Trade flow: {buy_trades:,} buys ({buy_trades/len(self.trades)*100:.1f}%), "
                f"{sell_trades:,} sells ({sell_trades/len(self.trades)*100:.1f}%)")

        log.info("✓ Phase 0 complete\n")

    def calculate_garch_volatility(self):
        """Calculates volatility using a GARCH(1,1) model on 1-second resampled data."""
        log.info("\n1.1a Calculating volatility (σ_t) using GARCH(1,1)...")
        
        # Create uniform time series (1s candles)
        mid_series = pd.Series(self.mid.values, index=self.orderbook['timestamp'])
        mid_1s = mid_series.resample('1s').last().ffill().dropna()
        
        returns = mid_1s.pct_change().dropna() * 100.0

        if len(returns) < 200:
            log.warning("  ⚠ Not enough data for GARCH model (need >200 seconds).")
            return None

        try:
            am = arch_model(returns, vol='Garch', p=1, q=1, dist='t')
            res = am.fit(disp='off', show_warning=False)
            
            # Volatility from GARCH on 1s data is already "per second" (in %)
            garch_vol_pct = res.conditional_volatility / 100.0
            
            # Convert to price units ($/sqrt(s))
            # align indices
            aligned_mid = mid_1s.reindex(garch_vol_pct.index)
            sigma_garch_1s = garch_vol_pct * aligned_mid
            
            log.info("  ✓ GARCH model fitted successfully on 1s data.")
            
            # Map back to original timestamps
            # Reindex using ffill to propagate the last known 1s volatility to subsequent ticks within that second
            sigma_garch = sigma_garch_1s.reindex(self.orderbook['timestamp'], method='ffill').bfill()
            
            # Reset index to match self.mid's RangeIndex
            sigma_garch = sigma_garch.reset_index(drop=True)
            
            return sigma_garch
            
        except Exception as e:
            log.error(f"  ⚠ GARCH model failed: {e}.")
            return None

    def calculate_ewma_volatility(self):
        """Calculates volatility using the original EWMA method as a fallback."""
        log.info("\n1.1b Calculating fallback volatility (σ_t) using EWMA...")
        price_changes = self.mid.diff().dropna()
        
        max_lag = max(2, min(VOL_EWMA_MAX_HALFLIFE_TICKS, len(price_changes) // 100))
        autocorrs = [price_changes.autocorr(lag=i) for i in range(1, max_lag)]
        
        target_autocorr = 0.5
        autocorrs_array = np.array(autocorrs)
        if np.any(autocorrs_array < target_autocorr):
            halflife_ticks = np.argmin(np.abs(autocorrs_array - target_autocorr)) + 1
        else:
            halflife_ticks = max_lag // 2
            
        halflife_sec = halflife_ticks * self.median_tick_interval
        self.config.vol_halflife_sec = halflife_sec

        alpha_ewma = 1 - np.exp(-np.log(2) / (halflife_sec / self.median_tick_interval))
        variance_ewma = (price_changes**2).ewm(alpha=alpha_ewma, adjust=False).mean()
        sigma_price_per_tick = np.sqrt(variance_ewma)
        sigma_ewma = sigma_price_per_tick / np.sqrt(self.median_tick_interval)
        
        log.info(f"  ✓ EWMA volatility calculated (halflife: {halflife_sec:.2f}s).")
        return sigma_ewma.reindex(self.orderbook.index).ffill().bfill()

    def calculate_volatility(self):
        """Calculates volatility using GARCH with an EWMA fallback."""
        log.info("="*70)
        log.info("PHASE 1: FEATURE ENGINEERING")
        log.info("="*70)
        
        sigma_garch = self.calculate_garch_volatility()
        sigma_ewma = self.calculate_ewma_volatility()

        if sigma_garch is None:
            log.warning("\n  --> Using EWMA volatility due to GARCH failure.")
            self.sigma_t = sigma_ewma
        else:
            # Compare mean volatility and switch to EWMA if GARCH is an outlier
            mean_garch = sigma_garch.mean()
            mean_ewma = sigma_ewma.mean()
            ratio = mean_garch / mean_ewma if mean_ewma > 0 else float('inf')

            if ratio > 5.0 or ratio < 0.2:
                log.warning(f"\n  --> GARCH vol ({mean_garch:.4f}) differs from EWMA ({mean_ewma:.4f}) by >5x. Using EWMA.")
                self.sigma_t = sigma_ewma
            else:
                log.info(f"\n  --> GARCH vol ({mean_garch:.4f}) is consistent with EWMA ({mean_ewma:.4f}). Using GARCH.")
                self.sigma_t = sigma_garch

        # Log final stats
        mean_sigma_price_per_sec = self.sigma_t.mean()
        median_mid = self.mid.median()
        annualized_vol_pct = (mean_sigma_price_per_sec / median_mid) * np.sqrt(365 * 24 * 3600) * 100

        log.info(f"\n  ✓ Final Volatility Selected (in price / sqrt(sec))")
        log.info(f"    Mean: ${self.sigma_t.mean():.4f}/sqrt(s)")
        log.info(f"    Median: ${self.sigma_t.median():.4f}/sqrt(s)")
        log.info(f"    Equivalent Annualized Vol: {annualized_vol_pct:.1f}%")

    def calculate_microprice_alpha(self):
        """Calculate microprice and alpha signals"""
        log.info("\n1.2 Calculating microprice & alpha signals...")

        # Weighted microprice (top level)
        log.info("  Using top level for microprice...")

        bid_p = self.orderbook['bid_price_0']
        bid_q = self.orderbook['bid_qty_0']
        ask_p = self.orderbook['ask_price_0']
        ask_q = self.orderbook['ask_qty_0']

        # Calculate microprice, with a fallback to mid-price if the book is empty
        denominator = bid_q + ask_q
        self.microprice = (ask_p * bid_q + bid_p * ask_q) / denominator
        self.microprice = self.microprice.where(denominator > 0, self.mid)

        # Microprice deviation (alpha signal)
        self.micro_deviation = self.microprice - self.mid

        log.info(f"  ✓ Microprice calculated")
        log.info(f"    Mean deviation: ${self.micro_deviation.mean():.6f}")
        log.info(f"    Std deviation: ${self.micro_deviation.std():.6f}")
        log.info(f"    Mean deviation (bps): {(self.micro_deviation / self.mid * 10000).mean():.3f}")

        # Volume imbalance
        bid_volume = self.orderbook['bid_qty_0']
        ask_volume = self.orderbook['ask_qty_0']
        total_volume = bid_volume + ask_volume
        self.volume_imbalance = (bid_volume - ask_volume) / total_volume
        self.volume_imbalance = self.volume_imbalance.where(total_volume > 0, 0)

        log.info(f"  ✓ Volume imbalance calculated")
        log.info(f"    Mean: {self.volume_imbalance.mean():.4f}")

        # Initial beta_alpha estimate from regression (will be optimized later)
        log.info("\n  Calibrating initial β_α from predictive regression...")
        future_horizon = 10  # 10 ticks ahead
        future_return = np.log(self.mid.shift(-future_horizon) / self.mid)
        micro_dev_normalized = self.micro_deviation / self.mid

        valid = ~(future_return.isna() | micro_dev_normalized.isna())
        if valid.sum() > 1000:
            result = linregress(micro_dev_normalized[valid], future_return[valid])
            beta_alpha_initial = result.slope
            r_squared = result.rvalue**2
            log.info(f"  ✓ Initial β_α: {beta_alpha_initial:.4f} (R²={r_squared:.4f})")
        else:
            beta_alpha_initial = 0.0
            log.info(f"  ⚠ Insufficient data for α calibration, setting β_α = 0")

        # Store for optimization starting point
        self.beta_alpha_initial = beta_alpha_initial

        log.info("✓ Phase 1 complete\n")

    def estimate_fill_probabilities(self):
        """Phase 2: Estimate fill probability parameters (A, k) from order book dynamics"""
        log.info("="*70)
        log.info("PHASE 2: FILL PROBABILITY ESTIMATION")
        log.info("="*70)
        log.info("\nTracking order book level consumption to estimate λ(δ) = A·e^(-k·δ)...\n")
        log.warning("  ⚠ NOTE: This model measures top-of-book CONSUMPTION, which conflates")
        log.warning("     cancellations, other participants' fills, and queue priority.")
        log.warning("     For production, track YOUR OWN fills with queue position & latency.")
        log.warning("     This provides a market-wide baseline estimate.\n")

        # Track when top-of-book changes (level got consumed)
        bid_price_changes = self.orderbook['bid_price_0'] != self.orderbook['bid_price_0'].shift(1)
        ask_price_changes = self.orderbook['ask_price_0'] != self.orderbook['ask_price_0'].shift(1)

        fill_data_bid = []
        fill_data_ask = []

        log.info("  Scanning bid side for level consumption...")
        # Bid side: when bid_price drops, previous level was hit
        for i in range(1, len(self.orderbook)):
            if i % 100000 == 0:
                log.info(f"    Processed {i:,}/{len(self.orderbook):,} snapshots...")

            if bid_price_changes.iloc[i]:
                prev_price = self.orderbook['bid_price_0'].iloc[i-1]
                curr_price = self.orderbook['bid_price_0'].iloc[i]

                # Only track when price drops (level consumed)
                if curr_price < prev_price:
                    prev_qty = self.orderbook['bid_qty_0'].iloc[i-1]
                    mid_prev = self.mid.iloc[i-1]
                    distance_price = abs(mid_prev - prev_price)
                    distance_bps = (distance_price / mid_prev) * 10000  # Convert to basis points
                    time_delta = (self.orderbook['timestamp'].iloc[i] -
                                 self.orderbook['timestamp'].iloc[i-1]).total_seconds()

                    if time_delta > 0 and distance_bps > 0 and prev_qty > 0:
                        fill_rate = prev_qty / time_delta
                        fill_data_bid.append({'distance_bps': distance_bps, 'fill_rate': fill_rate})

        log.info(f"  ✓ Found {len(fill_data_bid):,} bid level consumption events")

        log.info("\n  Scanning ask side for level consumption...")
        # Ask side: when ask_price rises, previous level was lifted
        for i in range(1, len(self.orderbook)):
            if i % 100000 == 0:
                log.info(f"    Processed {i:,}/{len(self.orderbook):,} snapshots...")

            if ask_price_changes.iloc[i]:
                prev_price = self.orderbook['ask_price_0'].iloc[i-1]
                curr_price = self.orderbook['ask_price_0'].iloc[i]

                # Only track when price rises (level consumed)
                if curr_price > prev_price:
                    prev_qty = self.orderbook['ask_qty_0'].iloc[i-1]
                    mid_prev = self.mid.iloc[i-1]
                    distance_price = abs(prev_price - mid_prev)
                    distance_bps = (distance_price / mid_prev) * 10000  # Convert to basis points
                    time_delta = (self.orderbook['timestamp'].iloc[i] -
                                 self.orderbook['timestamp'].iloc[i-1]).total_seconds()

                    if time_delta > 0 and distance_bps > 0 and prev_qty > 0:
                        fill_rate = prev_qty / time_delta
                        fill_data_ask.append({'distance_bps': distance_bps, 'fill_rate': fill_rate})

        log.info(f"  ✓ Found {len(fill_data_ask):,} ask level consumption events\n")

        # Fit exponential model: λ(δ) = A·e^(-k·δ)
        def fit_fill_intensity(fill_data, side_name):
            """Fit exponential decay model to fill rate vs distance"""
            if len(fill_data) < 20:
                log.warning(f"  ⚠ Insufficient {side_name} fill data ({len(fill_data)} events)")
                return None, None, None

            df = pd.DataFrame(fill_data)

            # Filter outliers (extreme fill rates likely errors)
            q99 = df['fill_rate'].quantile(0.99)
            df = df[(df['fill_rate'] > 0) & (df['fill_rate'] < q99)]

            log.info(f"  Fitting {side_name} side:")
            log.info(f"    Data points: {len(df):,}")
            log.info(f"    Distance range: {df['distance_bps'].min():.2f} to {df['distance_bps'].max():.2f} bps")
            log.info(f"    Fill rate range: {df['fill_rate'].min():.2f} to {df['fill_rate'].max():.2f} units/sec")

            # Log-linear regression: ln(fill_rate) = ln(A) - k·distance_bps
            X = df['distance_bps'].values
            y = np.log(df['fill_rate'].values)

            result = linregress(X, y)
            k = -result.slope  # Negative slope → positive k (now in 1/bps)
            A = np.exp(result.intercept)
            r_squared = result.rvalue**2

            log.info(f"    Fitted: A={A:.2f}, k={k:.4f} per bps (R²={r_squared:.3f})")

            # Sanity check: expected fill rate at different distances
            for dist_bps in [0.5, 1.0, 2.0, 5.0]:
                expected_rate = A * np.exp(-k * dist_bps)
                log.info(f"    Expected fill rate @ {dist_bps} bps: {expected_rate:.2f} units/sec")

            return A, k, r_squared

        log.info("Fitting fill intensity models...\n")
        A_bid, k_bid, r2_bid = fit_fill_intensity(fill_data_bid, "bid")
        log.info("")
        A_ask, k_ask, r2_ask = fit_fill_intensity(fill_data_ask, "ask")

        # Store results
        self.config.A_bid = A_bid if A_bid is not None else 1.0
        self.config.k_bid = k_bid if k_bid is not None else 1.0
        self.config.A_ask = A_ask if A_ask is not None else 1.0
        self.config.k_ask = k_ask if k_ask is not None else 1.0

        # Fallback if fitting failed
        if A_bid is None or k_bid is None or k_bid < 0:
            # Use reasonable default: fill rate decays with typical spread
            market_spread_bps = (self.orderbook['ask_price_0'] - self.orderbook['bid_price_0']) / self.mid * 10000
            typical_spread_bps = market_spread_bps.median()
            k_default = np.log(2) / typical_spread_bps if typical_spread_bps > 0 else FILL_K_BID_DEFAULT
            self.config.k_bid = k_default
            self.config.A_bid = FILL_A_DEFAULT
            log.warning(f"\n  ⚠ Using default bid k={k_default:.4f}")

        if A_ask is None or k_ask is None or k_ask < 0:
            market_spread_bps = (self.orderbook['ask_price_0'] - self.orderbook['bid_price_0']) / self.mid * 10000
            typical_spread_bps = market_spread_bps.median()
            k_default = np.log(2) / typical_spread_bps if typical_spread_bps > 0 else FILL_K_ASK_DEFAULT
            self.config.k_ask = k_default
            self.config.A_ask = FILL_A_DEFAULT
            log.warning(f"\n  ⚠ Using default ask k={k_default:.4f}")

        log.info("\n✓ Phase 2 complete\n")

    def backtest_strategy(self, params, data_slice=None):
        """
        Phase 4: Backtest a strategy with given parameters
        Returns: metrics dict (sharpe, pnl, max_inventory, etc.)
        """
        gamma = params['gamma']
        tau = params['tau']
        beta_alpha = params['beta_alpha']

        if data_slice is None:
            ob = self.orderbook
            mid = self.mid
            sigma = self.sigma_t
            micro_dev = self.micro_deviation
        else:
            ob = self.orderbook.iloc[data_slice]
            mid = self.mid.iloc[data_slice]
            sigma = self.sigma_t.iloc[data_slice]
            micro_dev = self.micro_deviation.iloc[data_slice]

        sigma_p95 = sigma.quantile(0.95)

        pnl_series, inventory_series, num_fills = _run_backtest_numba(
            mid.values, sigma.values, micro_dev.values,
            ob['timestamp'].astype(np.int64).values,
            gamma, tau, beta_alpha,
            self.config.k_bid, self.config.k_ask,
            self.config.A_bid, self.config.A_ask,
            self.c_AS_bid, self.c_AS_ask,
            self.config.maker_fee_bps, self.config.tick_size,
            self.config.base_quote_size, self.config.max_inventory,
            sigma_p95
        )

        pnl_array = np.array(pnl_series)
        inventory_array = np.array(inventory_series)

        if len(pnl_array) < 2:
            return {
                'sharpe': 0, 'pnl': 0, 'max_inventory': 0, 'num_fills': num_fills,
                'fill_rate_per_hour': 0, 'max_drawdown': 0
            }

        timestamps = ob['timestamp'].iloc[1:1+len(pnl_array)].reset_index(drop=True)
        pnl_df = pd.DataFrame({'timestamp': timestamps, 'pnl': pnl_array})
        pnl_df = pnl_df.set_index('timestamp')

        pnl_1s = pnl_df['pnl'].resample('1s').last().ffill()
        returns_1s = pnl_1s.diff().dropna()

        if len(returns_1s) > 1 and returns_1s.std() > 0:
            sharpe = returns_1s.mean() / returns_1s.std() * np.sqrt(365 * 24 * 3600)
        else:
            sharpe = 0

        cumulative_max = np.maximum.accumulate(pnl_array)
        drawdown = cumulative_max - pnl_array
        max_drawdown = np.max(drawdown) if len(drawdown) > 0 else 0
        max_inventory = np.max(inventory_array) if len(inventory_array) > 0 else 0

        total_duration_hours = (ob['timestamp'].iloc[-1] - ob['timestamp'].iloc[0]).total_seconds() / 3600

        return {
            'sharpe': sharpe,
            'pnl': pnl_array[-1] if len(pnl_array) > 0 else 0,
            'max_drawdown': max_drawdown,
            'max_inventory': max_inventory,
            'num_fills': num_fills,
            'fill_rate_per_hour': num_fills / total_duration_hours if total_duration_hours > 0 else 0
        }

    def measure_adverse_selection(self):
        """Phase 3: Measure adverse selection from post-trade price impact"""
        log.info("="*70)
        log.info("PHASE 3: ADVERSE SELECTION MEASUREMENT")
        log.info("="*70)
        log.info("\nMeasuring post-trade price impact at multiple horizons...\n")
        log.warning("  ⚠ IMPORTANT: This measures impact from ALL market trades,")
        log.warning("     not YOUR specific fills. Actual adverse selection depends on")
        log.warning("     your queue position, latency, and toxic vs. benign fill selection.")
        log.warning("     Treat these values as UPPER BOUNDS and use safety factors.\n")

        # Test multiple horizons to see where impact stabilizes
        horizons_ms = ADVERSE_SELECTION_HORIZONS_MS

        # Store results
        impact_results = {h: {'buy': [], 'sell': []} for h in horizons_ms}

        # Sample trades for efficiency (use every Nth trade)
        sample_rate = max(1, len(self.trades) // 10000)  # Max 10K trades to process (faster)
        trades_sample = self.trades.iloc[::sample_rate].reset_index(drop=True)

        log.info(f"  Processing {len(trades_sample):,} trades (sample rate: 1/{sample_rate})...")

        # Create timestamp index for fast lookups (convert to int64 for searchsorted)
        ob_timestamps = self.orderbook['timestamp'].astype('int64').values
        trade_timestamps = trades_sample['timestamp'].astype('int64').values

        for idx in range(len(trades_sample)):
            if idx % 2000 == 0 and idx > 0:
                log.info(f"    Processed {idx:,}/{len(trades_sample):,} trades...")

            trade = trades_sample.iloc[idx]
            trade_time = trade['timestamp']
            trade_time_int = int(trade_time.value)  # Convert to int64

            # Fast binary search for orderbook snapshot BEFORE trade
            # Use side='left' to find first index >= trade_time, then subtract 1 to get strictly < trade_time
            pre_idx = np.searchsorted(ob_timestamps, trade_time_int, side='left') - 1
            if pre_idx < 0:
                continue

            mid_pre = self.mid.iloc[pre_idx]

            # Measure impact at each horizon
            for h_ms in horizons_ms:
                target_time = trade_time + pd.Timedelta(milliseconds=h_ms)
                target_time_int = int(target_time.value)  # Convert to int64

                # Fast binary search for orderbook snapshot AFTER horizon
                post_idx = np.searchsorted(ob_timestamps, target_time_int, side='left')

                if post_idx < len(self.orderbook):
                    mid_post = self.mid.iloc[post_idx]

                    # Directional impact (positive = adverse to that side)
                    if trade['side'] == 'buy':  # Buyer hit ask, pushes price up
                        impact = mid_post - mid_pre
                        impact_results[h_ms]['buy'].append(impact)
                    else:  # Seller hit bid, pushes price down
                        impact = mid_pre - mid_post  # Flip sign so positive = adverse
                        impact_results[h_ms]['sell'].append(impact)

        log.info(f"  ✓ Collected impact measurements\n")

        # Calculate statistics per horizon
        log.info("  Impact by horizon:")
        c_AS_by_horizon = {}

        for h_ms in horizons_ms:
            if len(impact_results[h_ms]['buy']) > 0 and len(impact_results[h_ms]['sell']) > 0:
                # Use trimmed mean (exclude top/bottom 5% outliers, then mean)
                from scipy import stats
                c_AS_bid = stats.trim_mean(impact_results[h_ms]['sell'], proportiontocut=0.05)
                c_AS_ask = stats.trim_mean(impact_results[h_ms]['buy'], proportiontocut=0.05)

                # Also compute median for comparison
                median_bid = np.median(impact_results[h_ms]['sell'])
                median_ask = np.median(impact_results[h_ms]['buy'])

                c_AS_by_horizon[h_ms] = {'bid': c_AS_bid, 'ask': c_AS_ask}

                log.info(f"    {h_ms:5d}ms - bid: ${c_AS_bid:+.5f} ({c_AS_bid/self.mid.median()*10000:+.2f} bps), "
                        f"ask: ${c_AS_ask:+.5f} ({c_AS_ask/self.mid.median()*10000:+.2f} bps)")
                log.info(f"              median - bid: ${median_bid:+.5f}, ask: ${median_ask:+.5f}")

        # Select optimal horizons where impact has stabilized
        log.info("\n  Selecting optimal horizons...")
        horizons_sorted = sorted([h for h in horizons_ms if h in c_AS_by_horizon])

        if len(horizons_sorted) < 2:
            log.warning("  ⚠ Insufficient horizon data, using all available")
            selected_horizons = horizons_sorted
        else:
            # Calculate marginal impact between consecutive horizons
            impacts_bid = [c_AS_by_horizon[h]['bid'] for h in horizons_sorted]
            impacts_ask = [c_AS_by_horizon[h]['ask'] for h in horizons_sorted]

            marginal_bid = np.abs(np.diff(impacts_bid))
            marginal_ask = np.abs(np.diff(impacts_ask))

            # Select horizons where marginal impact is significant (>10% of total range)
            total_range_bid = max(impacts_bid) - min(impacts_bid) if len(impacts_bid) > 0 else 0
            total_range_ask = max(impacts_ask) - min(impacts_ask) if len(impacts_ask) > 0 else 0
            threshold_bid = 0.1 * abs(total_range_bid) if total_range_bid != 0 else 0.001
            threshold_ask = 0.1 * abs(total_range_ask) if total_range_ask != 0 else 0.001

            selected_horizons = [horizons_sorted[0]]  # Always include first
            for i in range(len(marginal_bid)):
                if marginal_bid[i] > threshold_bid or marginal_ask[i] > threshold_ask:
                    if horizons_sorted[i+1] not in selected_horizons:
                        selected_horizons.append(horizons_sorted[i+1])

            # Always include a medium-term horizon (around 1s)
            if 1000 in horizons_sorted and 1000 not in selected_horizons:
                selected_horizons.append(1000)

            selected_horizons = sorted(selected_horizons)

        log.info(f"  ✓ Selected horizons: {selected_horizons}")

        # Calculate final c_AS as the MAXIMUM impact across selected horizons
        # This is conservative: we want to protect against the full extent of the price move
        c_AS_bid_final = 0.0
        c_AS_ask_final = 0.0
        
        if selected_horizons:
            # Get the values for selected horizons
            bids = [c_AS_by_horizon[h]['bid'] for h in selected_horizons]
            asks = [c_AS_by_horizon[h]['ask'] for h in selected_horizons]
            
            # Use max to be conservative (cover the worst-case average impact)
            c_AS_bid_final = max(bids) if bids else 0.0
            c_AS_ask_final = max(asks) if asks else 0.0
            
            log.info("\n  Impact across horizons (using MAX for final parameters):")
            for h in selected_horizons:
                log.info(f"    {h:5d}ms: Bid=${c_AS_by_horizon[h]['bid']:.5f}, Ask=${c_AS_by_horizon[h]['ask']:.5f}")

        log.info(f"\n  Final Adverse Selection (Max):")
        log.info(f"    Bid: ${c_AS_bid_final:.5f} ({c_AS_bid_final/self.mid.median()*10000:.2f} bps)")
        log.info(f"    Ask: ${c_AS_ask_final:.5f} ({c_AS_ask_final/self.mid.median()*10000:.2f} bps)")
        log.info(f"    Asymmetry (ask/bid): {c_AS_ask_final/c_AS_bid_final if c_AS_bid_final != 0 else np.inf:.2f}x")

        # Store results
        self.c_AS_bid = c_AS_bid_final
        self.c_AS_ask = c_AS_ask_final
        self.selected_horizons = selected_horizons
        self.c_AS_by_horizon = c_AS_by_horizon

        log.info("\n✓ Phase 3 complete\n")


def main():
    """Main execution"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Avellaneda-Stoikov Spread Calculator')
    parser.add_argument('--symbol', type=str, default='PAXG',
                        help='Trading symbol (e.g., PAXG, ETHUSDT)')
    parser.add_argument('--window-hours', type=float, default=1.0,
                        help='Rolling window in hours for backtesting. Default: 1.0h')
    args = parser.parse_args()

    log.info("\n" + "="*70)
    log.info("AVELLANEDA-STOIKOV SPREAD CALCULATOR")
    log.info("Data-Driven Parameter Calibration")
    log.info(f"Symbol: {args.symbol}")
    log.info("="*70 + "\n")

    # Initialize
    config = SpreadConfig()
    calculator = SpreadCalculator(config, symbol=args.symbol)

    # Phase 0: Load data and extract market characteristics
    calculator.load_data()
    calculator.extract_market_characteristics()

    # Phase 1: Feature engineering
    calculator.calculate_volatility()
    calculator.calculate_microprice_alpha()

    # Phase 2: Fill probability estimation
    calculator.estimate_fill_probabilities()

    # Phase 3: Adverse selection measurement
    calculator.measure_adverse_selection()

    # Phase 4: Test backtest engine with default parameters
    log.info("="*70)
    log.info("PHASE 4: BACKTESTING ENGINE TEST")
    log.info("="*70)
    log.info("\nTesting backtest with default parameters...")

    # Test with reasonable defaults on small sample
    test_params = {
        'gamma': 1e-4,
        'tau': 2.0,
        'beta_alpha': calculator.beta_alpha_initial
    }

    log.info(f"  Test params: γ={test_params['gamma']:.2e}, τ={test_params['tau']:.1f}, β_α={test_params['beta_alpha']:.3f}")

    # Test on first 10K snapshots (fast test)
    test_slice = slice(0, 10000)
    test_metrics = calculator.backtest_strategy(test_params, data_slice=test_slice)

    log.info(f"\n  Test results (on {10000} snapshots):")
    log.info(f"    Sharpe: {test_metrics['sharpe']:.3f}")
    log.info(f"    PnL: ${test_metrics['pnl']:.2f}")
    log.info(f"    Max inventory: {test_metrics['max_inventory']:.2f}")
    log.info(f"    Max drawdown: ${test_metrics['max_drawdown']:.2f}")
    log.info(f"    Est. fills: {test_metrics['num_fills']:,}")

    log.info("\n✓ Phase 4 test complete\n")

    # Phase 5: Parameter optimization via grid search on PnL
    log.info("="*70)
    log.info("PHASE 5: PARAMETER OPTIMIZATION")
    log.info("="*70)
    log.info("\nOptimizing (gamma, tau, beta_alpha) via grid search on out-of-sample PnL...\n")

    # Select rolling window if configured
    if args.window_hours is not None:
        window_start = calculator.orderbook['timestamp'].max() - pd.Timedelta(hours=args.window_hours)
        window_mask = calculator.orderbook['timestamp'] >= window_start
        window_indices = calculator.orderbook[window_mask].index

        if len(window_indices) == 0:
            log.warning(f"  ⚠ Window of {args.window_hours}h is empty, using all data")
            window_start_idx = 0
            window_end_idx = len(calculator.orderbook)
        else:
            window_start_idx = window_indices[0]
            window_end_idx = window_indices[-1] + 1

        log.info(f"  Using rolling window: last {args.window_hours} hours")
        log.info(f"    Window: {calculator.orderbook['timestamp'].iloc[window_start_idx]} to {calculator.orderbook['timestamp'].iloc[window_end_idx-1]}")
        log.info(f"    Snapshots in window: {window_end_idx - window_start_idx:,}")
    else:
        window_start_idx = 0
        window_end_idx = len(calculator.orderbook)
        log.info(f"  Using full dataset: {window_end_idx:,} snapshots")

    # Split window: train / test
    window_size = window_end_idx - window_start_idx
    train_size = int(window_size * calculator.config.backtest_train_fraction)
    train_slice = slice(window_start_idx, window_start_idx + train_size)
    test_slice = slice(window_start_idx + train_size, window_end_idx)

    log.info(f"  Train: {train_size:,} snapshots, Test: {window_size - train_size:,} snapshots")

    # Display volatility stats for the training period
    train_volatility = calculator.sigma_t.iloc[train_slice]
    log.info("\n" + "="*70)
    log.info("VOLATILITY (σ) USED FOR OPTIMIZATION (TRAINING SLICE)")
    log.info("="*70)
    log.info(f"  Mean:   ${train_volatility.mean():.4f}/sqrt(s)")
    log.info(f"  Median: ${train_volatility.median():.4f}/sqrt(s)")
    log.info(f"  Std Dev: ${train_volatility.std():.4f}/sqrt(s)")
    log.info(f"  Min:    ${train_volatility.min():.4f}/sqrt(s)")
    log.info(f"  Max:    ${train_volatility.max():.4f}/sqrt(s)")
    log.info("="*70 + "\n")

    # Get market-calibrated baseline for gamma (now with corrected units)
    typical_spread_price = (calculator.orderbook['ask_price_0'] - calculator.orderbook['bid_price_0']).median()
    typical_mid = calculator.mid.median()
    typical_vol = np.sqrt((calculator.sigma_t**2).mean())
    tau_base = TAU_SECONDS
    # Corrected: no mid multiplier
    gamma_base = (typical_spread_price / 2) / (0.5 * typical_vol**2 * tau_base) if typical_vol > 0 else 1e-3

    # Define grid from top-level parameters
    gamma_grid = [gamma_base * m for m in GAMMA_GRID_MULTIPLIERS]
    tau_grid = TAU_GRID_SECONDS
    beta_alpha_grid = [calculator.beta_alpha_initial * m for m in BETA_ALPHA_GRID_MULTIPLIERS]

    log.info(f"\n  Grid search space:")
    log.info(f"    γ candidates: {[f'{g:.2e}' for g in gamma_grid]} (multipliers: {GAMMA_GRID_MULTIPLIERS})")
    log.info(f"    τ candidates: {tau_grid}")
    log.info(f"    β_α candidates: {[f'{b:.4f}' for b in beta_alpha_grid]} (multipliers: {BETA_ALPHA_GRID_MULTIPLIERS})")
    log.info(f"    Total combinations: {len(gamma_grid) * len(tau_grid) * len(beta_alpha_grid)}\n")

    best_sharpe = -np.inf
    best_params = None
    results = []

    log.info("  Running grid search on training data...")
    for gamma in gamma_grid:
        for tau in tau_grid:
            for beta_alpha in beta_alpha_grid:
                params = {'gamma': gamma, 'tau': tau, 'beta_alpha': beta_alpha}

                # Backtest on train set
                metrics = calculator.backtest_strategy(params, data_slice=train_slice)

                results.append({
                    'gamma': gamma,
                    'tau': tau,
                    'beta_alpha': beta_alpha,
                    'sharpe': metrics['sharpe'],
                    'pnl': metrics['pnl'],
                    'max_inventory': metrics['max_inventory']
                })

                # Track best by Sharpe with inventory constraint
                if metrics['max_inventory'] <= calculator.config.max_inventory and metrics['sharpe'] > best_sharpe:
                    best_sharpe = metrics['sharpe']
                    best_params = params.copy()

    log.info(f"  ✓ Grid search complete\n")

    # Display top 3 results
    results_df = pd.DataFrame(results).sort_values('sharpe', ascending=False)
    log.info("  Top 3 parameter sets (by Sharpe):")
    for idx, row in results_df.head(3).iterrows():
        log.info(f"    γ={row['gamma']:.2e}, τ={row['tau']:.1f}, β_α={row['beta_alpha']:.4f} → "
                f"Sharpe={row['sharpe']:.3f}, PnL=${row['pnl']:.2f}, MaxInv={row['max_inventory']:.2f}")

    if best_params is None:
        log.warning("\n  ⚠ No valid params found (all exceeded inventory), using baseline")
        best_params = {'gamma': gamma_base, 'tau': tau_base, 'beta_alpha': 0.0}

    log.info(f"\n  Selected parameters (best train Sharpe):")
    log.info(f"    γ (risk aversion): {best_params['gamma']:.2e}")
    log.info(f"    τ (mean-reversion): {best_params['tau']:.2f}s")
    log.info(f"    β_α (alpha weight): {best_params['beta_alpha']:.4f}")

    # Validate on test set
    log.info(f"\n  Validating on out-of-sample test set...")
    test_metrics = calculator.backtest_strategy(best_params, data_slice=test_slice)
    log.info(f"    Test Sharpe: {test_metrics['sharpe']:.3f}")
    log.info(f"    Test PnL: ${test_metrics['pnl']:.2f}")
    log.info(f"    Test Max Inventory: {test_metrics['max_inventory']:.2f}")

    # Store params
    calculator.config.gamma = best_params['gamma']
    calculator.config.tau = best_params['tau']
    calculator.config.beta_alpha = best_params['beta_alpha']

    log.info("\n✓ Phase 5 complete\n")

    #Phase 6: Calculate final spreads for entire dataset
    log.info("\n" + "="*70)
    log.info("PHASE 6: CALCULATE FINAL OPTIMAL SPREADS")
    log.info("="*70)
    log.info("\nCalculating optimal bid/ask spreads for full dataset...\n")

    optimal_params = {
        'gamma': calculator.config.gamma,
        'tau': calculator.config.tau,
        'beta_alpha': calculator.config.beta_alpha
    }

    # Calculate spreads (simplified - no inventory tracking for final output)
    spreads_data = []
    k_mid = (calculator.config.k_bid + calculator.config.k_ask) / 2

    log.info(f"  Processing {len(calculator.orderbook):,} snapshots...")

    for i in range(0, len(calculator.orderbook), max(1, len(calculator.orderbook) // 100)):  # Sample every 1% for speed
        if i % 50000 == 0:
            log.info(f"    Processed {i:,}/{len(calculator.orderbook):,}...")

        mid_i = calculator.mid.iloc[i]
        sigma_i = calculator.sigma_t.iloc[i]
        alpha_i = calculator.micro_deviation.iloc[i]

        # Reservation price (inventory = 0 for market-neutral)
        r = mid_i + optimal_params['beta_alpha'] * alpha_i

        # Half-spread - correct units (no mid_i multiplier on vol term)
        k_mid_price_based = k_mid * 10000 / mid_i
        phi_term1 = (1/optimal_params['gamma']) * np.log(1 + optimal_params['gamma'] / k_mid_price_based) if optimal_params['gamma'] > 0 and k_mid_price_based > 0 else 0.01
        phi_term2 = 0.5 * optimal_params['gamma'] * sigma_i**2 * optimal_params['tau']
        phi_term3 = (calculator.c_AS_bid + calculator.c_AS_ask) / 2
        phi_term4 = calculator.config.maker_fee_bps / 10000 * mid_i

        phi = max(phi_term1 + phi_term2 + phi_term3 + phi_term4, calculator.tick_size)

        # Asymmetric adjustments (difference from average adverse selection)
        c_AS_mid = (calculator.c_AS_bid + calculator.c_AS_ask) / 2
        Delta_bid = calculator.c_AS_bid - c_AS_mid
        Delta_ask = calculator.c_AS_ask - c_AS_mid

        # Final quotes
        bid_price = r - phi - Delta_bid
        ask_price = r + phi + Delta_ask

        # Convert to % from mid
        bid_spread_pct = (mid_i - bid_price) / mid_i * 100
        ask_spread_pct = (ask_price - mid_i) / mid_i * 100
        total_spread_bps = (ask_price - bid_price) / mid_i * 10000

        spreads_data.append({
            'timestamp': calculator.orderbook.iloc[i]['timestamp'],
            'unix_timestamp': calculator.orderbook.iloc[i]['unix_timestamp'],
            'mid_price': mid_i,
            'market_bid': calculator.orderbook.iloc[i]['bid_price_0'],
            'market_ask': calculator.orderbook.iloc[i]['ask_price_0'],
            'market_spread_bps': (calculator.orderbook.iloc[i]['ask_price_0'] - calculator.orderbook.iloc[i]['bid_price_0']) / mid_i * 10000,
            'volatility': sigma_i,
            'optimal_bid': bid_price,
            'optimal_ask': ask_price,
            'bid_spread_pct': bid_spread_pct,
            'ask_spread_pct': ask_spread_pct,
            'total_spread_bps': total_spread_bps,
            'half_spread': phi,
            'reservation_price': r
        })

    spreads_df = pd.DataFrame(spreads_data)

    # Save to CSV
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    spreads_df.to_csv(output_dir / "optimal_spreads.csv", index=False)

    log.info(f"\n  ✓ Saved optimal spreads to output/optimal_spreads.csv ({len(spreads_df):,} rows)")

    # Summary statistics
    log.info(f"\n  Summary Statistics:")
    log.info(f"    Mean optimal spread: {spreads_df['total_spread_bps'].mean():.2f} bps")
    log.info(f"    Median optimal spread: {spreads_df['total_spread_bps'].median():.2f} bps")
    log.info(f"    Mean market spread: {spreads_df['market_spread_bps'].mean():.2f} bps")
    log.info(f"    Bid spread from mid: {spreads_df['bid_spread_pct'].mean():.4f}%")
    log.info(f"    Ask spread from mid: {spreads_df['ask_spread_pct'].mean():.4f}%")

    log.info("\n✓ Phase 6 complete\n")

    # Display final (most recent) spreads for live trading
    log.info("="*70)
    log.info("RECOMMENDED SPREADS FOR CURRENT TIME (most recent period)")
    log.info("="*70)
    latest = spreads_df.iloc[-1]
    log.info(f"\nTimestamp: {latest['timestamp']}")
    log.info(f"Mid price: ${latest['mid_price']:.2f}")
    log.info(f"Market spread: {latest['market_spread_bps']:.2f} bps")
    log.info(f"\n  OPTIMAL QUOTES:")
    log.info(f"    Bid: ${latest['optimal_bid']:.4f} ({latest['bid_spread_pct']:.4f}% from mid)")
    log.info(f"    Ask: ${latest['optimal_ask']:.4f} ({latest['ask_spread_pct']:.4f}% from mid)")
    log.info(f"    Total spread: {latest['total_spread_bps']:.2f} bps")
    log.info(f"\n  Underlying factors:")
    log.info(f"    Volatility: {latest['volatility']:.6e}")
    log.info(f"    Half-spread (φ): ${latest['half_spread']:.4f}")
    log.info(f"    Reservation price: ${latest['reservation_price']:.4f}")

    log.info("\n" + "="*70)
    log.info("ALL PHASES COMPLETED SUCCESSFULLY!")
    log.info(f"Output: output/optimal_spreads.csv")
    log.info("="*70)

    # Phase 7: Save parameters to JSON
    log.info("\n" + "="*70)
    log.info("PHASE 7: SAVE PARAMETERS TO JSON")
    log.info("="*70)
    log.info("\nSaving calibrated parameters...\n")

    # Create params directory if it doesn't exist
    params_dir = Path("params")
    params_dir.mkdir(exist_ok=True)

    # Prepare parameters dictionary
    params_output = {
        # Market characteristics
        "symbol": calculator.symbol,
        "tick_size": float(calculator.config.tick_size),
        "median_tick_interval_ms": float(calculator.median_tick_interval * 1000),

        # Optimized strategy parameters
        "gamma": float(calculator.config.gamma),
        "tau": float(calculator.config.tau),
        "beta_alpha": float(calculator.config.beta_alpha),

        # Fill probability parameters
        "k_bid": float(calculator.config.k_bid),
        "k_ask": float(calculator.config.k_ask),
        "A_bid": float(calculator.config.A_bid),
        "A_ask": float(calculator.config.A_ask),

        # Adverse selection costs
        "c_AS_bid": float(calculator.c_AS_bid),
        "c_AS_ask": float(calculator.c_AS_ask),

        # Volatility parameters
        "latest_volatility": float(latest['volatility']),
        "mean_volatility_annual_pct": float(calculator.sigma_t.mean() * np.sqrt(365 * 24 * 3600 / calculator.median_tick_interval) * 100),

        # Trading configuration
        "maker_fee_bps": float(calculator.config.maker_fee_bps),
        "base_quote_size": float(calculator.config.base_quote_size),
        "max_inventory": float(calculator.config.max_inventory),

        # Recent spread recommendations
        "latest_optimal_bid": float(latest['optimal_bid']),
        "latest_optimal_ask": float(latest['optimal_ask']),
        "latest_mid_price": float(latest['mid_price']),
        "latest_total_spread_bps": float(latest['total_spread_bps']),
        "latest_bid_spread_pct": float(latest['bid_spread_pct']),
        "latest_ask_spread_pct": float(latest['ask_spread_pct']),
        "latest_timestamp": str(latest['timestamp']),

        # Performance metrics (test set)
        "test_sharpe": float(test_metrics['sharpe']),
        "test_pnl": float(test_metrics['pnl']),
        "test_max_inventory": float(test_metrics['max_inventory']),
        "test_max_drawdown": float(test_metrics['max_drawdown']),

        # Metadata
        "calibration_timestamp": pd.Timestamp.now().isoformat(),
        "data_duration_hours": float((calculator.orderbook['timestamp'].max() - calculator.orderbook['timestamp'].min()).total_seconds() / 3600),
        "num_snapshots": int(len(calculator.orderbook)),
        "num_trades": int(len(calculator.trades))
    }

    # Save to JSON
    output_file = params_dir / f"spread_parameters_{calculator.symbol}.json"
    with open(output_file, 'w') as f:
        json.dump(params_output, f, indent=2)

    log.info(f"  ✓ Saved parameters to {output_file}")
    log.info(f"\n  Parameters saved:")
    log.info(f"    Symbol: {calculator.symbol}")
    log.info(f"    Optimal gamma: {params_output['gamma']:.2e}")
    log.info(f"    Optimal tau: {params_output['tau']:.2f}s")
    log.info(f"    Optimal beta_alpha: {params_output['beta_alpha']:.4f}")
    log.info(f"    Fill intensity (bid): A={params_output['A_bid']:.2f}, k={params_output['k_bid']:.4f}")
    log.info(f"    Fill intensity (ask): A={params_output['A_ask']:.2f}, k={params_output['k_ask']:.4f}")
    log.info(f"    Adverse selection (bid): ${params_output['c_AS_bid']:.5f}")
    log.info(f"    Adverse selection (ask): ${params_output['c_AS_ask']:.5f}")
    log.info(f"    Latest volatility: {params_output['latest_volatility']:.6e}")
    log.info(f"    Latest bid spread pct: {params_output['latest_bid_spread_pct']:.4f}%")
    log.info(f"    Latest ask spread pct: {params_output['latest_ask_spread_pct']:.4f}%")

    log.info("\n" + "="*70)
    log.info("FINAL SPREAD FORMULA & PARAMETERS")
    log.info("="*70)
    log.info("\nThis is the model used for calculating quotes based on the latest data:")
    
    # Display parameters
    log.info("\n  Parameters Used:")
    log.info(f"    γ (Risk Aversion):   {params_output['gamma']:.4e}")
    log.info(f"    τ (Time Horizon):      {params_output['tau']:.4f} s")
    log.info(f"    β_α (Microprice Mult): {params_output['beta_alpha']:.4f}")
    log.info(f"    σ (Volatility):        {latest['volatility']:.4e}")
    log.info(f"    k_mid (Book Density):  {(params_output['k_bid'] + params_output['k_ask']) / 2:.4f} (1/bps)")
    log.info(f"    Adverse Selection:   Bid=${params_output['c_AS_bid']:.5f}, Ask=${params_output['c_AS_ask']:.5f}")
    log.info(f"    Maker Fee:           {params_output['maker_fee_bps']:.2f} bps")

    # Display formulas
    log.info("\n  Formulas:")
    log.info("    reservation_price = mid + (β_α * micro_price_deviation) - (inventory * γ * σ² * τ)")
    log.info("    k_price_based     = k_mid * mid / 10000")
    log.info("    phi_inventory     = (1/γ) * log(1 + γ / k_price_based)")
    log.info("    phi_adverse_sel   = 0.5 * γ * σ² * τ")
    log.info("    phi_adverse_cost  = (c_AS_bid + c_AS_ask) / 2")
    log.info("    phi_fee           = maker_fee_bps / 10000 * mid")
    log.info("    half_spread (φ)   = phi_inventory + phi_adverse_sel + phi_adverse_cost + phi_fee")
    log.info("")
    log.info("    delta_bid_as      = c_AS_bid - (c_AS_bid + c_AS_ask) / 2")
    log.info("    delta_ask_as      = c_AS_ask - (c_AS_bid + c_AS_ask) / 2")
    log.info("")
    log.info("    FINAL_BID = reservation_price - half_spread - delta_bid_as")
    log.info("    FINAL_ASK = reservation_price + half_spread + delta_ask_as")

    log.info("\n" + "="*70)
    log.info("INVENTORY SKEW ANALYSIS (LATEST PERIOD)")
    log.info("="*70)
    log.info("\nPredicted spreads at different inventory levels (based on latest data):")
    
    # Prepare parameters from the final output dictionary
    params = params_output
    mid_price = latest['mid_price']
    # Use the mean volatility for a more representative skew calculation
    sigma = calculator.sigma_t.mean()
    alpha_i = calculator.micro_deviation.iloc[-1] if not calculator.micro_deviation.empty else 0

    # Table header
    log.info(f"\n{'            Inventory Factor':>18} | {'Bid Spread (%)':>18} | {'Ask Spread (%)':>18}")
    log.info(f"{'-'*18} | {'-'*18} | {'-'*18}")

    inventory_factors = sorted([-5, -2, -1, 0, 1, 2, 5])

    for inv_factor in inventory_factors:
        # Re-calculate quotes using the same logic as the market maker
        gamma = params['gamma']
        tau = params['tau']
        k_mid = (params['k_bid'] + params['k_ask']) / 2
        c_as_bid = params['c_AS_bid']
        c_as_ask = params['c_AS_ask']
        tick_size = params['tick_size']
        maker_fee_bps = params['maker_fee_bps']
        beta_alpha = params['beta_alpha']

        k_mid_price_based = k_mid * 10000 / mid_price

        # Reservation price with inventory and alpha
        reservation_price = mid_price + (beta_alpha * alpha_i) - inv_factor * gamma * (sigma**2) * tau
        
        # Optimal half-spread (phi)
        phi_term1 = (1 / gamma) * np.log(1 + gamma / k_mid_price_based) if gamma > 0 and k_mid_price_based > 0 else 0.01
        phi_term2 = 0.5 * gamma * (sigma**2) * tau
        phi_term3 = (c_as_bid + c_as_ask) / 2
        phi_term4 = maker_fee_bps / 10000 * mid_price
        half_spread = max(phi_term1 + phi_term2 + phi_term3 + phi_term4, tick_size)

        # Asymmetric adjustments
        c_as_mid = (c_as_bid + c_as_ask) / 2
        delta_bid_as = c_as_bid - c_as_mid
        delta_ask_as = c_as_ask - c_as_mid

        # Final quotes
        final_bid_price = reservation_price - half_spread - delta_bid_as
        final_ask_price = reservation_price + half_spread + delta_ask_as

        # --- Sanity Check: Prevent spread crossing (mirroring live logic) ---
        final_bid_price = min(final_bid_price, mid_price - tick_size)
        final_ask_price = max(final_ask_price, mid_price + tick_size)
        if final_bid_price >= final_ask_price:
            final_bid_price = final_ask_price - tick_size

        # Convert to % from mid
        bid_spread_pct = (mid_price - final_bid_price) / mid_price * 100
        ask_spread_pct = (final_ask_price - mid_price) / mid_price * 100

        log.info(f"{inv_factor:>18} | {f'{bid_spread_pct:>.4f}%':>18} | {f'{ask_spread_pct:>.4f}%':>18}")

    log.info("\n✓ Phase 7 complete\n")


if __name__ == "__main__":
    main()
