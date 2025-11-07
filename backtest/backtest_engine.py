"""
回測引擎
執行完整的回測流程和權益曲線計算
"""

import pandas as pd
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime

from backtest.backtest_config import BacktestConfig
from backtest.trade_direction import TradeDirectionDeterminer
from backtest.pnl_calculator import PnLCalculator

logger = logging.getLogger(__name__)


class TradeRecord:
    """交易記錄"""
    
    def __init__(self, opportunity: Dict, pnl_result: Dict):
        self.timestamp = opportunity.get('timestamp')
        self.symbol = opportunity.get('symbol')
        self.K = opportunity.get('K', 0)
        self.direction = pnl_result.get('direction')
        self.price_pnl = pnl_result.get('price_pnl', 0)
        self.funding_pnl = pnl_result.get('funding_pnl', 0)
        self.entry_fee = pnl_result.get('entry_fee', 0)
        self.exit_fee = pnl_result.get('exit_fee', 0)
        self.total_fees = pnl_result.get('total_fees', 0)
        self.net_pnl = pnl_result.get('net_pnl', 0)
        self.pnl_pct = pnl_result.get('pnl_pct', 0)


class BacktestEngine:
    """回測引擎"""
    
    def __init__(self, initial_capital: float, config: BacktestConfig):
        """初始化回測引擎"""
        self.initial_capital = initial_capital
        self.config = config
        self.trades: List[TradeRecord] = []
        self.cumulative_pnl = 0.0
        self.portfolio_value = initial_capital
        self.equity_curve = []
    
    def execute_trade(
        self,
        opportunity: Dict,
        pnl_result: Dict
    ) -> Optional[TradeRecord]:
        """
        執行單筆交易
        
        Args:
            opportunity: 機會資訊
            pnl_result: P&L 計算結果
        
        Returns:
            交易記錄或 None
        """
        try:
            # 驗證機會
            if not opportunity.get('vwap_valid', False):
                return None
            
            if opportunity.get('K', 0) <= 0:
                return None
            
            # 建立交易記錄
            trade = TradeRecord(opportunity, pnl_result)
            
            # 更新權益
            self.cumulative_pnl += trade.net_pnl
            self.portfolio_value = self.initial_capital + self.cumulative_pnl
            
            return trade
            
        except Exception as e:
            logger.error(f"交易執行失敗: {e}")
            return None
    
    def run_backtest(
        self,
        opportunities: List[Dict],
        klines_dict: Dict
    ) -> Dict:
        """
        運行完整回測
        
        Args:
            opportunities: 機會列表
            klines_dict: K 線資料字典
        
        Returns:
            回測結果
        """
        logger.info(f"開始回測: {len(opportunities)} 個機會")
        
        failed_trades = 0
        
        for i, opportunity in enumerate(opportunities):
            try:
                # 檢查 VWAP 有效性
                if not opportunity.get('vwap_valid', False):
                    logger.debug(f"機會 {i} VWAP 無效，跳過")
                    failed_trades += 1
                    continue
                
                # 檢查 VWAP 值不為 None
                if any(v is None for v in [
                    opportunity.get('vwap_entry_binance'),
                    opportunity.get('vwap_exit_binance'),
                    opportunity.get('vwap_entry_bybit'),
                    opportunity.get('vwap_exit_bybit')
                ]):
                    logger.debug(f"機會 {i} VWAP 值為 None，跳過")
                    failed_trades += 1
                    continue
                
                # 判斷交易方向
                direction, recv, pay = TradeDirectionDeterminer.determine_direction(
                    bybit_pay=opportunity.get('bybit_pay', False),
                    binance_pay=opportunity.get('binance_pay', False),
                    bybit_rate=opportunity.get('bybit_rate', 0),
                    binance_rate=opportunity.get('binance_rate', 0)
                )
                
                # 計算 P&L
                pnl_result = PnLCalculator.calculate_pnl(
                    direction=direction,
                    vwap_entry_bn=opportunity.get('vwap_entry_binance'),
                    vwap_exit_bn=opportunity.get('vwap_exit_binance'),
                    vwap_entry_by=opportunity.get('vwap_entry_bybit'),
                    vwap_exit_by=opportunity.get('vwap_exit_bybit'),
                    binance_rate=opportunity.get('binance_rate', 0),
                    bybit_rate=opportunity.get('bybit_rate', 0),
                    position_size=opportunity.get('K', 0) / 2,
                    config=self.config
                )
                pnl_result['direction'] = direction
                
                # 執行交易
                trade = self.execute_trade(opportunity, pnl_result)
                
                if trade:
                    self.trades.append(trade)
                    # 記錄權益曲線
                    self.equity_curve.append({
                        'timestamp': opportunity.get('timestamp'),
                        'cumulative_pnl': self.cumulative_pnl,
                        'portfolio_value': self.portfolio_value,
                        'trades_count': len(self.trades)
                    })
                else:
                    failed_trades += 1
                    
            except Exception as e:
                logger.warning(f"機會 {i} 失敗: {e}")
                failed_trades += 1
        
        logger.info(f"回測完成: {len(self.trades)} 成功, {failed_trades} 失敗")
        
        return {
            'trades': self.trades,
            'equity_curve': self.equity_curve,
            'final_pnl': self.cumulative_pnl,
            'final_portfolio_value': self.portfolio_value,
            'total_trades': len(self.trades),
            'failed_trades': failed_trades,
            'return_pct': (self.cumulative_pnl / self.initial_capital * 100) if self.initial_capital > 0 else 0
        }
    
    def get_results(self) -> Dict:
        """取得回測結果"""
        return {
            'trades': self.trades,
            'cumulative_pnl': self.cumulative_pnl,
            'portfolio_value': self.portfolio_value,
            'equity_curve': self.equity_curve,
            'total_trades': len(self.trades)
        }

