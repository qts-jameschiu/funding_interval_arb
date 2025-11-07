"""
Backtest System for Funding Interval Arbitrage
回測系统 - 基于 VWAP 的配对交易回測

模組結构：
- backtest_config: 設定管理
- backtest_main: 主程序入口
- opportunity_loader: 交易機會加載
- kline_fetcher: K 线取得和快取
- vwap_calculator: VWAP 計算
- backtest_engine: 回測執行引擎
- backtest_analyzer: 性能分析
- backtest_visualizer: 可視化和報告生成
"""

from .backtest_config import BacktestConfig

__all__ = ['BacktestConfig']
__version__ = '0.1.0'

