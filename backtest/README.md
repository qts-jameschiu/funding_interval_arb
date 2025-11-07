# Backtest Module

Backtesting framework for funding rate interval mismatch arbitrage strategies.

## Overview

This module takes identified tradable opportunities and simulates trading execution to evaluate profitability and risk metrics.

### Core Components

- **backtest_main.py** - Orchestration and workflow
- **backtest_config.py** - Configuration management
- **opportunity_loader.py** - Load tradable opportunities
- **kline_fetcher.py** - Fetch and cache K-line data
- **vwap_calculator.py** - Calculate VWAP from K-lines
- **vwap_integrator.py** - Integrate VWAP into opportunities
- **backtest_engine.py** - Trade execution logic
- **pnl_calculator.py** - P&L calculation
- **backtest_analyzer.py** - Performance metrics
- **backtest_visualizer.py** - Charts and reports

## Quick Start

```bash
cd backtest
python run_backtest.py
```

Results: `/home/james/research_output/funding_interval_arb/backtest_results/`

## Configuration

Edit `config/default_backtest_config.json`:

```json
{
  "analysis": {
    "run_analysis_first": false,
    "start_date": "2025-08-07",
    "end_date": "2025-11-05"
  },
  "trading": {
    "initial_capital": 100000,
    "vwap_window_minutes": 5,
    "entry_buffer_pct": 0.0005,
    "exit_buffer_pct": 0.0005
  },
  "fees": {
    "maker_fee": 0.0002,
    "taker_fee": 0.0004
  }
}
```

## Workflow

1. **Load Config** - Read backtest_config.json
2. **Run Analysis** (optional) - Generate opportunities if needed
3. **Load Opportunities** - Filter tradable=True entries
4. **Fetch K-lines** - Get historical data for all symbols
5. **Calculate VWAP** - Entry and exit prices from K-lines
6. **Execute Backtest** - Simulate trades and calculate P&L
7. **Analyze Performance** - Generate metrics and reports
8. **Visualize Results** - Create charts and export data

## Output Files

```
backtest_results/BACKTEST_YYYYMMDD_HHMMSS/
├── trades.csv                 # Detailed trade records
├── equity_curve.csv           # Cumulative equity
├── performance_report.txt     # Performance metrics
├── symbol_stats.csv           # Per-symbol analysis
└── pnl_chart.png              # P&L and drawdown chart
```

## Trade Logic

For each tradable opportunity:

1. **Determine Direction** - Based on funding rates and exchange flags
2. **Entry Price** - VWAP from 5 minutes before signal
3. **Exit Price** - VWAP from 5 minutes after signal
4. **Calculate P&L** - Price spread + funding fees - trading fees
5. **Update Equity** - Track cumulative returns

## P&L Calculation

```
gross_P&L = position_size × [
  (exit_price - entry_price) / entry_price +
  funding_rate
]

net_P&L = gross_P&L - trading_fees
```

## Performance Metrics

- Total Return %
- Sharpe Ratio
- Sortino Ratio
- Maximum Drawdown %
- Win Rate %
- Per-symbol breakdown
- Monthly performance

## Key Features

✅ Intelligent K-line caching
✅ Parallel API calls
✅ Timestamp standardization
✅ Listing time awareness
✅ Dynamic capital allocation
✅ Comprehensive reporting

## Troubleshooting

**"gap after cache (0.0 days)"**
- Normal - ignores gaps < 2 minutes

**"Binance API 错误: 400"**
- Symbol delisted - uses cached data

**Memory issues**
- Reduce vwap_window_minutes or symbol count

## Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `run_analysis_first` | bool | false | Run analysis if needed |
| `start_date` | string | 2025-08-07 | Backtest start date |
| `end_date` | string | 2025-11-05 | Backtest end date |
| `initial_capital` | float | 100000 | Trading capital (USDT) |
| `vwap_window_minutes` | int | 5 | VWAP window size (min) |
| `entry_buffer_pct` | float | 0.0005 | Entry slippage (0.05%) |
| `exit_buffer_pct` | float | 0.0005 | Exit slippage (0.05%) |
| `maker_fee` | float | 0.0002 | Maker fee (0.02%) |
| `taker_fee` | float | 0.0004 | Taker fee (0.04%) |

## Advanced Configuration

### Conservative Strategy
```json
{
  "vwap_window_minutes": 10,
  "entry_buffer_pct": 0.001,
  "exit_buffer_pct": 0.001,
  "initial_capital": 50000
}
```

### Aggressive Strategy
```json
{
  "vwap_window_minutes": 3,
  "entry_buffer_pct": 0.0001,
  "exit_buffer_pct": 0.0001,
  "initial_capital": 500000
}
```

---

**Module**: Backtest System  
**Version**: 2.0  
**Status**: ✅ Production Ready

