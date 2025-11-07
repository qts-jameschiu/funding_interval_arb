"""
回測可視化器
生成績效報告和圖表
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
import numpy as np

logger = logging.getLogger(__name__)


class BacktestVisualizer:
    """回測可視化器"""
    
    def __init__(self, output_dir: str):
        """初始化可視化器"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_text_report(
        self,
        metrics: Dict,
        trades: List,
        equity_curve: List
    ) -> str:
        """生成文本報告"""
        report = []
        report.append("=" * 80)
        report.append("回測績效報告".center(80))
        report.append("=" * 80)
        report.append("")
        
        # 交易統計
        report.append("【交易統計】")
        report.append(f"  總交易數:         {metrics.get('total_trades', 0)}")
        report.append(f"  獲利交易:         {metrics.get('winning_trades', 0)}")
        report.append(f"  虧損交易:         {metrics.get('losing_trades', 0)}")
        report.append(f"  勝率:             {metrics.get('win_rate', 0):.2f}%")
        report.append("")
        
        # 損益統計
        report.append("【損益統計】")
        report.append(f"  總 P&L:          ${metrics.get('total_pnl', 0):,.2f}")
        report.append(f"  平均 P&L:        ${metrics.get('avg_pnl', 0):,.2f}")
        report.append(f"  最大盈利:        ${metrics.get('max_win', 0):,.2f}")
        report.append(f"  最大虧損:        ${metrics.get('max_loss', 0):,.2f}")
        report.append("")
        
        # 風險指標
        report.append("【風險指標】")
        report.append(f"  Sharpe Ratio:    {metrics.get('sharpe_ratio', 0):.2f}")
        report.append(f"  最大回撤:        {metrics.get('max_drawdown', 0):.2f}%")
        report.append(f"  獲利因子:        {metrics.get('profit_factor', 0):.2f}")
        report.append("")
        
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def save_trades_csv(self, trades: List) -> Path:
        """保存交易記錄為 CSV"""
        try:
            data = []
            for trade in trades:
                data.append({
                    'timestamp': trade.timestamp,
                    'symbol': trade.symbol,
                    'K': trade.K,
                    'direction': trade.direction,
                    'price_pnl': trade.price_pnl,
                    'funding_pnl': trade.funding_pnl,
                    'fees': trade.total_fees,
                    'net_pnl': trade.net_pnl,
                    'pnl_pct': trade.pnl_pct
                })
            
            df = pd.DataFrame(data)
            csv_path = self.output_dir / 'trades.csv'
            df.to_csv(csv_path, index=False)
            logger.info(f"交易記錄已保存: {csv_path}")
            return csv_path
            
        except Exception as e:
            logger.error(f"保存交易記錄失敗: {e}")
            return None
    
    def save_equity_curve_csv(self, equity_curve: List) -> Path:
        """保存權益曲線為 CSV"""
        try:
            df = pd.DataFrame(equity_curve)
            csv_path = self.output_dir / 'equity_curve.csv'
            df.to_csv(csv_path, index=False)
            logger.info(f"權益曲線已保存: {csv_path}")
            return csv_path
            
        except Exception as e:
            logger.error(f"保存權益曲線失敗: {e}")
            return None
    
    def save_report(self, report_text: str) -> Path:
        """保存文本報告"""
        try:
            report_path = self.output_dir / 'backtest_report.txt'
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_text)
            logger.info(f"報告已保存: {report_path}")
            return report_path
            
        except Exception as e:
            logger.error(f"保存報告失敗: {e}")
            return None
    
    def generate_pnl_chart(self, equity_curve: List, metrics: Dict) -> Optional[Path]:
        """
        生成 P&L 曲線圖 + 回撤圖表 + 統計數據
        - 上: P&L 曲線 (佔 60%)
        - 中: 回撤圖表 (佔 25%)
        - 下: 統計數據 (佔 15%)
        
        Args:
            equity_curve: 權益曲線數據
            metrics: 績效指標
        
        Returns:
            圖表路徑或 None
        """
        try:
            if not equity_curve:
                logger.warning("權益曲線為空，跳過圖表生成")
                return None
            
            # 轉換為 DataFrame
            df = pd.DataFrame(equity_curve)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # 計算回撤百分比
            cumulative_max = df['portfolio_value'].expanding().max()
            drawdown_pct = ((df['portfolio_value'] - cumulative_max) / cumulative_max) * 100
            
            # 計算 P&L 百分比
            pnl_pct = (df['cumulative_pnl'] / 100000) * 100
            
            # 創建組合圖表
            fig = plt.figure(figsize=(16, 12))
            gs = fig.add_gridspec(4, 1, height_ratios=[3, 1.2, 0.8, 0.3], hspace=0.35)
            
            # ===== 第一部分：P&L 曲線 (60%) =====
            ax_pnl = fig.add_subplot(gs[0])
            
            # 繪製 P&L 曲線（百分比）
            ax_pnl.plot(df['timestamp'], pnl_pct, 
                       linewidth=2.5, color='#2E86AB', label='Cumulative P&L')
            ax_pnl.fill_between(df['timestamp'], 0, pnl_pct, 
                               where=(pnl_pct >= 0), 
                               alpha=0.2, color='#06A77D', label='Profit')
            ax_pnl.fill_between(df['timestamp'], 0, pnl_pct, 
                               where=(pnl_pct < 0), 
                               alpha=0.2, color='#D62828', label='Loss')
            
            # 設置 P&L 圖格式
            ax_pnl.set_ylabel('Cumulative P&L (%)', fontsize=12, fontweight='bold')
            ax_pnl.set_title('Backtest Performance - Cumulative P&L & Drawdown Analysis', 
                            fontsize=14, fontweight='bold', pad=20)
            ax_pnl.grid(True, alpha=0.3, linestyle='--')
            ax_pnl.legend(loc='upper left', fontsize=10)
            
            # 格式化 y 軸為百分比
            def pct_formatter(x, p):
                return f'{x:.1f}%'
            ax_pnl.yaxis.set_major_formatter(FuncFormatter(pct_formatter))
            
            # 格式化 x 軸日期
            ax_pnl.xaxis.set_major_locator(mdates.AutoDateLocator())
            ax_pnl.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            plt.setp(ax_pnl.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
            # ===== 第二部分：回撤圖表 (25%) =====
            ax_dd = fig.add_subplot(gs[1])
            
            # 繪製回撤柱狀圖
            colors = ['#D62828' if x < 0 else '#06A77D' for x in drawdown_pct]
            ax_dd.bar(df['timestamp'], drawdown_pct, color=colors, alpha=0.7, width=0.008)
            ax_dd.axhline(y=0, color='black', linestyle='-', linewidth=1)
            
            # 設置回撤圖格式
            ax_dd.set_ylabel('Drawdown (%)', fontsize=11, fontweight='bold')
            ax_dd.grid(True, alpha=0.3, linestyle='--', axis='y')
            
            # 格式化 y 軸
            ax_dd.yaxis.set_major_formatter(FuncFormatter(pct_formatter))
            
            # 隱藏 x 軸標籤
            ax_dd.set_xticklabels([])
            
            # ===== 第三部分：統計數據 (15%) =====
            ax_stats = fig.add_subplot(gs[2:])
            ax_stats.axis('off')
            
            # 準備統計文本（使用英文避免亂碼）
            total_trades = metrics.get('total_trades', 0)
            winning_trades = metrics.get('winning_trades', 0)
            losing_trades = metrics.get('losing_trades', 0)
            win_rate = metrics.get('win_rate', 0)
            
            final_pnl = df['cumulative_pnl'].iloc[-1] if len(df) > 0 else 0
            avg_pnl = metrics.get('avg_pnl', 0)
            max_win = metrics.get('max_win', 0)
            max_loss = metrics.get('max_loss', 0)
            
            final_return_pct = pnl_pct.iloc[-1] if len(pnl_pct) > 0 else 0
            sharpe_ratio = metrics.get('sharpe_ratio', 0)
            max_drawdown = metrics.get('max_drawdown', 0)
            profit_factor = metrics.get('profit_factor', 0)
            
            # 統計數據：用英文避免亂碼
            stats_text = (
                f"Trade Statistics\n"
                f"  Total Trades: {total_trades:>3d}  |  "
                f"Win Trades: {winning_trades:>3d}  |  "
                f"Loss Trades: {losing_trades:>3d}  |  "
                f"Win Rate: {win_rate:>5.2f}%\n"
                f"\n"
                f"P&L Summary\n"
                f"  Cumulative P&L: ${final_pnl:>9,.0f}  |  "
                f"Avg P&L/Trade: ${avg_pnl:>8,.0f}  |  "
                f"Max Win: ${max_win:>8,.0f}  |  "
                f"Max Loss: ${max_loss:>9,.0f}\n"
                f"\n"
                f"Risk Metrics\n"
                f"  Return Rate: {final_return_pct:>5.2f}%  |  "
                f"Sharpe Ratio: {sharpe_ratio:>5.2f}  |  "
                f"Max Drawdown: {max_drawdown:>5.2f}%  |  "
                f"Profit Factor: {profit_factor:>5.2f}"
            )
            
            # 在圖表下方添加統計文本
            ax_stats.text(0.05, 0.5, stats_text, 
                         fontsize=10, 
                         family='monospace',
                         bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.2),
                         verticalalignment='center')
            
            # 保存圖表
            chart_path = self.output_dir / 'pnl_chart.png'
            plt.tight_layout()
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"P&L 圖表已生成: {chart_path}")
            return chart_path
            
        except Exception as e:
            logger.error(f"生成 P&L 圖表失敗: {e}", exc_info=True)
            return None
    
    def generate_drawdown_chart(self, equity_curve: List) -> Optional[Path]:
        """
        生成回撤圖表
        
        Args:
            equity_curve: 權益曲線數據
        
        Returns:
            圖表路徑或 None
        """
        try:
            if not equity_curve:
                logger.warning("權益曲線為空，跳過回撤圖表生成")
                return None
            
            df = pd.DataFrame(equity_curve)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # 計算回撤
            cumulative_max = df['portfolio_value'].expanding().max()
            drawdown = df['portfolio_value'] - cumulative_max
            drawdown_pct = (drawdown / cumulative_max) * 100
            
            # 創建圖表
            fig, ax = plt.subplots(figsize=(16, 8))
            
            # 繪製回撤
            colors = ['#D62828' if x < 0 else '#06A77D' for x in drawdown_pct]
            ax.bar(df['timestamp'], drawdown_pct, color=colors, alpha=0.7, width=0.01)
            ax.axhline(y=0, color='black', linestyle='-', linewidth=1)
            
            # 設置格式
            ax.set_ylabel('Drawdown (%)', fontsize=12, fontweight='bold')
            ax.set_title('回撤 - Drawdown Chart', fontsize=14, fontweight='bold', pad=20)
            ax.grid(True, alpha=0.3, linestyle='--')
            
            # 格式化 x 軸
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
            # 保存圖表
            chart_path = self.output_dir / 'drawdown_chart.png'
            plt.tight_layout()
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"回撤圖表已生成: {chart_path}")
            return chart_path
            
        except Exception as e:
            logger.error(f"生成回撤圖表失敗: {e}", exc_info=True)
            return None
    
    def generate_all_reports(
        self,
        metrics: Dict,
        trades: List,
        equity_curve: List
    ) -> Dict:
        """生成所有報告和 CSV 檔案及圖表"""
        results = {}
        
        # 生成文本報告
        report_text = self.generate_text_report(metrics, trades, equity_curve)
        results['report_txt'] = self.save_report(report_text)
        
        # 保存 CSV
        results['trades_csv'] = self.save_trades_csv(trades)
        results['equity_csv'] = self.save_equity_curve_csv(equity_curve)
        
        # 生成合併圖表（P&L + Drawdown + Stats）
        results['pnl_chart'] = self.generate_pnl_chart(equity_curve, metrics)
        
        # 註：回撤圖表已集成到 pnl_chart 中，不再單獨生成
        
        logger.info(f"所有報告已生成到: {self.output_dir}")
        
        return results

