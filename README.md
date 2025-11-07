# Funding Interval Arbitrage Trading System

Complete system for identifying and backtesting funding rate interval mismatch arbitrage opportunities across Binance and Bybit.

## 📚 Documentation

- **[BACKTEST_ARCHITECTURE.md](./BACKTEST_ARCHITECTURE.md)** - Complete technical design and system architecture
- **[backtest/README.md](./backtest/README.md)** - Backtest module documentation
- **[opportunity_analysis/README.md](./opportunity_analysis/README.md)** - Analysis module documentation
- **[data_collector/README.md](./data_collector/README.md)** - Data collection module documentation

## 🎯 System Overview

### What is Funding Interval Mismatch?

Different exchanges settle funding rates at different intervals:
- **Binance**: Every 8 hours (0:00, 8:00, 16:00 UTC)
- **Bybit**: Every hour (on the hour)

When these intervals mismatch, opportunities arise for arbitrage:
- One exchange is about to pay funding
- Another is about to receive funding
- We can exploit this spread by positioning accordingly

### System Architecture

```
┌─────────────────────────────────────────────────┐
│        Funding Interval Arbitrage System         │
└─────────────────────────────────────────────────┘

Phase 1: OPPORTUNITY ANALYSIS
  └─ Identify funding interval mismatches
  └─ Calculate potential profits
  └─ Output: trading_opportunities.csv

Phase 2: BACKTEST SYSTEM
  ├─ Load identified opportunities
  ├─ Fetch historical K-line data
  ├─ Calculate entry/exit prices (VWAP)
  ├─ Simulate trades
  ├─ Calculate P&L and metrics
  └─ Output: performance reports, charts
```

## 🏗️ Project Structure

```
funding_interval_arb/
├── opportunity_analysis/              # Phase 1: Identify opportunities
│   ├── main.py                        # Mismatch detection engine
│   ├── interval_analyzer.py          # Interval analysis
│   ├── stats_analyzer.py             # Statistics generation
│   ├── visualizer.py                 # Visualizations
│   ├── config.py
│   ├── __init__.py
│   └── README.md
│
├── backtest/                          # Phase 2: Backtest system
│   ├── run_backtest.py               # Entry point
│   ├── backtest_main.py              # Core orchestration
│   ├── backtest_config.py            # Configuration
│   ├── opportunity_loader.py         # Load opportunities
│   ├── kline_fetcher.py              # Fetch K-line data
│   ├── vwap_calculator.py            # Calculate VWAP
│   ├── vwap_integrator.py            # Integrate VWAP
│   ├── backtest_engine.py            # Trade execution
│   ├── pnl_calculator.py             # P&L calculation
│   ├── backtest_analyzer.py          # Performance analysis
│   ├── backtest_visualizer.py        # Reporting/charts
│   ├── config/
│   │   └── default_backtest_config.json
│   ├── __init__.py
│   └── README.md
│
├── data_collector/                    # Data collection utilities
│   ├── binance_client.py             # Binance API client
│   ├── bybit_client.py               # Bybit API client
│   ├── utils.py                      # Utilities
│   ├── config.py
│   ├── __init__.py
│   └── README.md
│
├── README.md (this file)
└── BACKTEST_ARCHITECTURE.md           # Technical details
```

## 🚀 Quick Start

### Step 1: Analyze Opportunities

Identify funding interval mismatches:

```bash
cd opportunity_analysis
python main.py --end_date 2025-11-05 --duration 90
```

Generates: `funding_rate_timeline_*.csv` files

### Step 2: Configure Backtest

Edit `backtest/config/default_backtest_config.json`:

```json
{
  "analysis": {
    "run_analysis_first": false,
    "start_date": "2025-08-07",
    "end_date": "2025-11-05"
  },
  "trading": {
    "initial_capital": 100000,
    "vwap_window_minutes": 5
  },
  "fees": {
    "maker_fee": 0.0002,
    "taker_fee": 0.0004
  }
}
```

### Step 3: Run Backtest

```bash
cd backtest
python run_backtest.py
```

### Step 4: View Results

Check results in: `/home/james/research_output/funding_interval_arb/backtest_results/`

```
backtest_results/BACKTEST_YYYYMMDD_HHMMSS/
├── trades.csv                  # Detailed trades
├── equity_curve.csv            # Equity progression
├── performance_report.txt      # Metrics and analysis
├── symbol_stats.csv            # Per-symbol breakdown
└── pnl_chart.png               # P&L and drawdown chart
```

## 💹 How It Works

### Phase 1: Opportunity Analysis

The analysis module:
1. Fetches funding rates from Binance and Bybit
2. Identifies when funding intervals mismatch
3. Calculates potential funding fee spreads
4. Outputs tradable opportunities

### Phase 2: Backtest Execution

For each identified opportunity:

1. **Fetch Data** - Get 1-minute K-lines around the signal time
2. **Calculate Prices**
   - Entry VWAP: 5 minutes before signal
   - Exit VWAP: 5 minutes after signal
3. **Determine Direction**
   - If exchange A pays and B receives: go short A, long B
   - Profit from funding spread + price movements
4. **Execute Trade**
   - Buy/sell at Entry VWAP
   - Close at Exit VWAP
   - Calculate fees and P&L
5. **Track Performance** - Cumulative returns, metrics, risk

### Trade Economics

```
Gross P&L = position_size × [
  (exit_price - entry_price) / entry_price +
  funding_rate_received
]

Net P&L = Gross P&L - trading_fees

Example:
  Position size: $50,000
  Price profit: $100 (0.2%)
  Funding income: $160 (0.32%)
  Trading fees: $40
  ────────────────
  Net P&L: $220 (0.44%)
```

## ⚙️ Configuration

### Essential Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `initial_capital` | 100,000 | Trading capital in USDT |
| `vwap_window_minutes` | 5 | Window for VWAP calculation |
| `entry_buffer_pct` | 0.0005 | Entry slippage (0.05%) |
| `exit_buffer_pct` | 0.0005 | Exit slippage (0.05%) |
| `taker_fee` | 0.0004 | Taker fee (0.04%) |
| `maker_fee` | 0.0002 | Maker fee (0.02%) |

### Dynamic Capital Allocation

When multiple symbols are tradable at the same time:
- Total capital is divided equally among all tradable symbols
- Each symbol gets: `K = initial_capital / n_tradable_symbols`
- This ensures exposure is balanced

## 📊 Output & Reporting

### Performance Metrics

- **Total Return %** - Cumulative profit/loss
- **Sharpe Ratio** - Risk-adjusted returns
- **Sortino Ratio** - Downside risk-adjusted returns
- **Maximum Drawdown** - Peak-to-trough decline
- **Win Rate** - % of profitable trades
- **Per-Symbol Stats** - Breakdown by trading pair
- **Monthly Analysis** - Month-by-month P&L

### Generated Files

- `trades.csv` - All executed trades with P&L breakdown
- `equity_curve.csv` - Daily equity values
- `performance_report.txt` - Summary metrics
- `symbol_stats.csv` - Per-symbol analysis
- `pnl_chart.png` - P&L and drawdown visualization

## 🔄 Workflow Diagram

```
START
  │
  ├─→ [Analysis] Identify opportunities
  │     └─ Output: funding_rate_timeline_*.csv
  │
  ├─→ [Backtest Setup]
  │     ├─ Load opportunities
  │     ├─ Fetch K-line data (with caching)
  │     └─ Calculate VWAP entry/exit prices
  │
  ├─→ [Execution]
  │     ├─ For each tradable opportunity:
  │     │   ├─ Determine trade direction
  │     │   ├─ Calculate P&L
  │     │   ├─ Update equity
  │     │   └─ Record trade
  │
  ├─→ [Analysis]
  │     ├─ Calculate metrics
  │     ├─ Generate statistics
  │     └─ Create visualizations
  │
  └─→ [Output]
        ├─ Save trades.csv
        ├─ Save equity_curve.csv
        ├─ Generate pnl_chart.png
        └─ Print performance report
```

## 🔧 Key Features

✅ **Intelligent K-line Caching**
- Caches downloaded K-lines to minimize API calls
- Smart incremental updates for overlapping periods

✅ **Parallel Data Fetching**
- asyncio + semaphore for efficient concurrent requests
- Respects exchange rate limits

✅ **Timestamp Standardization**
- Consistent millisecond integers throughout system
- Handles various timestamp formats automatically

✅ **Listing Time Awareness**
- Avoids fetching data before symbol launch
- Prevents API 400 errors for invalid periods

✅ **Dynamic Capital Allocation**
- Automatically balances capital among tradable symbols
- Adapts to market conditions

✅ **Comprehensive Reporting**
- Detailed trade records
- Equity curve tracking
- Performance metrics and visualizations

## 🐛 Troubleshooting

### Common Issues

**"Gap after cache (0.0 days)"**
- Normal behavior - system ignores gaps < 2 minutes
- Due to timestamp precision at day boundaries

**"Binance API 错误: 400"**
- Symbol delisted or unavailable during that period
- System logs warning and uses available cached data
- Check symbol/date range validity

**Memory issues**
- Reduce `vwap_window_minutes` for smaller K-line periods
- Limit symbols via `symbol_whitelist` config
- Process fewer symbols at a time

**Slow execution**
- K-line fetching is normal for first run (~10-30 min for 500 symbols)
- Subsequent runs use cache and are much faster
- Can parallelize with multiple processes

### Logs & Debugging

Enable detailed logging in `backtest_main.py`:

```python
logging.basicConfig(level=logging.DEBUG)
```

Check execution logs for detailed information about:
- API calls and responses
- K-line fetching progress
- VWAP calculation details
- Trade execution flow

## 📋 Advanced Configuration

### Conservative Strategy

For risk-averse trading:

```json
{
  "initial_capital": 50000,
  "vwap_window_minutes": 10,
  "entry_buffer_pct": 0.001,
  "exit_buffer_pct": 0.001,
  "taker_fee": 0.0005
}
```

### Aggressive Strategy

For higher return targeting:

```json
{
  "initial_capital": 500000,
  "vwap_window_minutes": 3,
  "entry_buffer_pct": 0.0001,
  "exit_buffer_pct": 0.0001,
  "taker_fee": 0.0003
}
```

## 📈 Understanding Results

### Equity Curve

- Shows cumulative capital progression
- Flat/rising = profitable strategy
- Declining = losses exceed gains
- Drawdown = peak-to-trough decline

### P&L Breakdown

Each trade shows:
- Entry/exit prices per exchange
- Price spread profit/loss
- Funding fee income
- Trading fees
- Net P&L

### Symbol Statistics

- Win rate per symbol
- Average P&L per trade
- Total trades
- Sharpe ratio
- Best/worst trades

## 🔄 System Integration

The system is designed to be modular:

```
Analysis Module (opportunity_analysis/)
        ↓ (produces opportunities)
        ↓
Configuration (backtest/config/*.json)
        ↓
Backtest Module (backtest/)
        ├─ Loads opportunities
        ├─ Fetches data
        ├─ Executes trades
        └─ Generates reports
```

Each component can be used independently:
- Run analysis without backtest
- Run backtest with pre-existing opportunities
- Customize data fetching
- Extend analysis logic

## 📞 Support

For detailed technical information:
1. See [BACKTEST_ARCHITECTURE.md](./BACKTEST_ARCHITECTURE.md)
2. Check module-specific README files
3. Review example configurations
4. Check execution logs for errors

## 🎓 Learning Resources

- **BACKTEST_ARCHITECTURE.md** - Complete technical design
- **backtest/README.md** - Backtest-specific details
- **opportunity_analysis/README.md** - Analysis methodology
- **data_collector/README.md** - API integration details

## 📊 Example Results

```
Backtest Results Summary
═══════════════════════════════════════════════════════════

Total Trades:          245
Winning Trades:        183 (74.7%)
Losing Trades:         62 (25.3%)

Performance:
  Total Return:        $12,450 (12.45%)
  Sharpe Ratio:        1.45
  Sortino Ratio:       2.31
  Max Drawdown:        -3.2%

Best Trade:            +$685 (0.68%)
Worst Trade:           -$145 (0.14%)
Avg Trade:             +$50.8 (0.05%)

Monthly Breakdown:
  August:              +$3,200
  September:           +$5,100
  October:             +$3,800
  November (partial):  +$350
```

---

## Project Information

**System**: Funding Interval Arbitrage Trading  
**Version**: 2.0  
**Status**: ✅ Production Ready  
**Last Updated**: 2025-11-07  

**Components**:
- Opportunity Analysis ✅
- Backtest Framework ✅
- Data Collection ✅
- Performance Reporting ✅

**Technologies**:
- Python 3.8+
- asyncio for concurrent requests
- pandas for data processing
- matplotlib for visualization
- aiohttp for async HTTP
