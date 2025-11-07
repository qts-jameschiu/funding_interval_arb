"""
回測分析器
計算績效指標 (Sharpe、Sortino、回撤等)
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class BacktestAnalyzer:
    """回測分析器"""
    
    @staticmethod
    def calculate_metrics(trades: List, equity_curve: List) -> Dict:
        """
        計算績效指標
        
        Args:
            trades: 交易記錄列表
            equity_curve: 權益曲線列表
        
        Returns:
            績效指標字典
        """
        if not trades:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'total_pnl': 0,
                'avg_pnl': 0,
                'max_win': 0,
                'max_loss': 0,
                'sharpe_ratio': 0,
                'max_drawdown': 0
            }
        
        # 基本統計
        pnl_values = [t.net_pnl for t in trades]
        total_pnl = sum(pnl_values)
        
        winning_trades = sum(1 for p in pnl_values if p > 0)
        losing_trades = sum(1 for p in pnl_values if p < 0)
        win_rate = winning_trades / len(trades) if trades else 0
        
        max_win = max(pnl_values) if pnl_values else 0
        max_loss = min(pnl_values) if pnl_values else 0
        
        # Sharpe Ratio (簡化計算)
        returns = np.array(pnl_values)
        sharpe_ratio = 0
        if len(returns) > 1 and np.std(returns) > 0:
            sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252)
        
        # 最大回撤
        max_drawdown = BacktestAnalyzer._calculate_max_drawdown(equity_curve)
        
        return {
            'total_trades': len(trades),
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate * 100,
            'total_pnl': total_pnl,
            'avg_pnl': total_pnl / len(trades) if trades else 0,
            'max_win': max_win,
            'max_loss': max_loss,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'profit_factor': BacktestAnalyzer._calculate_profit_factor(pnl_values)
        }
    
    @staticmethod
    def _calculate_max_drawdown(equity_curve: List) -> float:
        """計算最大回撤"""
        if not equity_curve:
            return 0
        
        portfolio_values = [e.get('portfolio_value', 0) for e in equity_curve]
        
        if not portfolio_values:
            return 0
        
        running_max = np.maximum.accumulate(portfolio_values)
        drawdown = (np.array(portfolio_values) - running_max) / running_max
        max_drawdown = np.min(drawdown) if len(drawdown) > 0 else 0
        
        return abs(max_drawdown) * 100
    
    @staticmethod
    def _calculate_profit_factor(pnl_values: List) -> float:
        """計算獲利因子"""
        wins = sum(p for p in pnl_values if p > 0)
        losses = abs(sum(p for p in pnl_values if p < 0))
        
        if losses == 0:
            return float('inf') if wins > 0 else 0
        
        return wins / losses if losses > 0 else 0
    
    @staticmethod
    def generate_report(backtest_result: Dict) -> str:
        """生成績效報告"""
        metrics = backtest_result.get('metrics', {})
        
        report = []
        report.append("=" * 60)
        report.append("回測績效報告")
        report.append("=" * 60)
        report.append("")
        
        report.append(f"總交易數:        {metrics.get('total_trades', 0)}")
        report.append(f"獲利交易:        {metrics.get('winning_trades', 0)}")
        report.append(f"虧損交易:        {metrics.get('losing_trades', 0)}")
        report.append(f"勝率:            {metrics.get('win_rate', 0):.2f}%")
        report.append("")
        
        report.append(f"總 P&L:          ${metrics.get('total_pnl', 0):,.2f}")
        report.append(f"平均 P&L:        ${metrics.get('avg_pnl', 0):,.2f}")
        report.append(f"最大盈利:        ${metrics.get('max_win', 0):,.2f}")
        report.append(f"最大虧損:        ${metrics.get('max_loss', 0):,.2f}")
        report.append("")
        
        report.append(f"Sharpe Ratio:    {metrics.get('sharpe_ratio', 0):.2f}")
        report.append(f"最大回撤:        {metrics.get('max_drawdown', 0):.2f}%")
        report.append(f"獲利因子:        {metrics.get('profit_factor', 0):.2f}")
        report.append("")
        report.append("=" * 60)
        
        return "\n".join(report)

