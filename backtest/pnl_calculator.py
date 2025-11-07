"""
P&L (Profit and Loss) Calculator
計算交易的損益，包括價差利潤、資金費收入和手續費
"""

from typing import Dict, Tuple
from enum import Enum
from backtest.trade_direction import TradeDirection, TradeDirectionDeterminer
from backtest.backtest_config import BacktestConfig


class PnLCalculator:
    """P&L 計算器"""
    
    @staticmethod
    def calculate_pnl(
        direction: TradeDirection,
        vwap_entry_bn: float,
        vwap_exit_bn: float,
        vwap_entry_by: float,
        vwap_exit_by: float,
        binance_rate: float,
        bybit_rate: float,
        position_size: float,
        config: BacktestConfig
    ) -> Dict[str, float]:
        """
        計算總 P&L
        
        Args:
            direction: 交易方向
            vwap_entry_bn: Binance 入場 VWAP
            vwap_exit_bn: Binance 出場 VWAP
            vwap_entry_by: Bybit 入場 VWAP
            vwap_exit_by: Bybit 出場 VWAP
            binance_rate: Binance funding rate (可能為正或負)
            bybit_rate: Bybit funding rate
            position_size: 單侧頭寸大小 (總資金的一半)
            config: 回測設定
        
        Returns:
            {
                'price_pnl': 價差利潤,
                'funding_pnl': 資金費收入,
                'entry_fee': 入場手續費,
                'exit_fee': 出場手續費,
                'total_fees': 總手續費,
                'net_pnl': 淨 P&L,
                'pnl_pct': P&L 百分比
            }
        """
        
        # 計算價差利潤
        price_pnl = PnLCalculator.calculate_price_pnl(
            direction,
            vwap_entry_bn, vwap_exit_bn,
            vwap_entry_by, vwap_exit_by,
            position_size
        )
        
        # 計算資金費收入
        funding_pnl = PnLCalculator.calculate_funding_pnl(
            direction,
            binance_rate,
            bybit_rate,
            position_size
        )
        
        # 計算手續費
        entry_fee, exit_fee = PnLCalculator.calculate_fees(
            position_size,
            config
        )
        total_fees = entry_fee + exit_fee
        
        # 計算淨 P&L
        net_pnl = price_pnl + funding_pnl - total_fees
        
        # P&L 百分比 (相對於總資金 = 2 * position_size)
        total_capital = 2 * position_size
        pnl_pct = (net_pnl / total_capital) * 100 if total_capital > 0 else 0
        
        return {
            'price_pnl': price_pnl,
            'funding_pnl': funding_pnl,
            'entry_fee': entry_fee,
            'exit_fee': exit_fee,
            'total_fees': total_fees,
            'net_pnl': net_pnl,
            'pnl_pct': pnl_pct,
        }
    
    @staticmethod
    def calculate_price_pnl(
        direction: TradeDirection,
        vwap_entry_bn: float,
        vwap_exit_bn: float,
        vwap_entry_by: float,
        vwap_exit_by: float,
        position_size: float
    ) -> float:
        """
        計算價差利潤
        
        公式（按方向）：
          LONG_BYBIT_SHORT_BINANCE:
            = position_size × [(bybit_exit - bybit_entry) / bybit_entry - (binance_exit - binance_entry) / binance_entry]
          
          SHORT_BYBIT_LONG_BINANCE:
            = position_size × [-(bybit_exit - bybit_entry) / bybit_entry + (binance_exit - binance_entry) / binance_entry]
          
          LONG_BINANCE_SHORT_BYBIT:
            = position_size × [(binance_exit - binance_entry) / binance_entry - (bybit_exit - bybit_entry) / bybit_entry]
          
          SHORT_BINANCE_LONG_BYBIT:
            = position_size × [-(binance_exit - binance_entry) / binance_entry + (bybit_exit - bybit_entry) / bybit_entry]
        """
        
        if direction == TradeDirection.LONG_BYBIT_SHORT_BINANCE:
            # 做多 Bybit，做空 Binance
            bybit_long_pnl = (vwap_exit_by - vwap_entry_by) / vwap_entry_by
            binance_short_pnl = -(vwap_exit_bn - vwap_entry_bn) / vwap_entry_bn
            return position_size * (bybit_long_pnl + binance_short_pnl)
        
        elif direction == TradeDirection.SHORT_BYBIT_LONG_BINANCE:
            # 做空 Bybit，做多 Binance
            bybit_short_pnl = -(vwap_exit_by - vwap_entry_by) / vwap_entry_by
            binance_long_pnl = (vwap_exit_bn - vwap_entry_bn) / vwap_entry_bn
            return position_size * (bybit_short_pnl + binance_long_pnl)
        
        elif direction == TradeDirection.LONG_BINANCE_SHORT_BYBIT:
            # 做多 Binance，做空 Bybit
            binance_long_pnl = (vwap_exit_bn - vwap_entry_bn) / vwap_entry_bn
            bybit_short_pnl = -(vwap_exit_by - vwap_entry_by) / vwap_entry_by
            return position_size * (binance_long_pnl + bybit_short_pnl)
        
        elif direction == TradeDirection.SHORT_BINANCE_LONG_BYBIT:
            # 做空 Binance，做多 Bybit
            binance_short_pnl = -(vwap_exit_bn - vwap_entry_bn) / vwap_entry_bn
            bybit_long_pnl = (vwap_exit_by - vwap_entry_by) / vwap_entry_by
            return position_size * (binance_short_pnl + bybit_long_pnl)
        
        else:
            raise ValueError(f"Unknown direction: {direction}")
    
    @staticmethod
    def calculate_funding_pnl(
        direction: TradeDirection,
        binance_rate: float,
        bybit_rate: float,
        position_size: float
    ) -> float:
        """
        計算資金費收入
        
        根據方向，决定从哪個交易所收取 funding：
          LONG_BYBIT_SHORT_BINANCE: 从 Bybit 的做多头收取 (abs(bybit_rate))
          SHORT_BYBIT_LONG_BINANCE: 从 Binance 的做多头收取 (abs(binance_rate))
          LONG_BINANCE_SHORT_BYBIT: 从 Binance 的做多头收取 (abs(binance_rate))
          SHORT_BINANCE_LONG_BYBIT: 从 Bybit 的做多头收取 (abs(bybit_rate))
        """
        
        if direction == TradeDirection.LONG_BYBIT_SHORT_BINANCE:
            # 做多 Bybit，接收其 funding (无论正負，我们都接收)
            return position_size * abs(bybit_rate)
        
        elif direction == TradeDirection.SHORT_BYBIT_LONG_BINANCE:
            # 做多 Binance，接收其 funding
            return position_size * abs(binance_rate)
        
        elif direction == TradeDirection.LONG_BINANCE_SHORT_BYBIT:
            # 做多 Binance，接收其 funding
            return position_size * abs(binance_rate)
        
        elif direction == TradeDirection.SHORT_BINANCE_LONG_BYBIT:
            # 做多 Bybit，接收其 funding
            return position_size * abs(bybit_rate)
        
        else:
            raise ValueError(f"Unknown direction: {direction}")
    
    @staticmethod
    def calculate_fees(
        position_size: float,
        config: BacktestConfig
    ) -> Tuple[float, float]:
        """
        計算手續費
        
        Args:
            position_size: 單侧頭寸大小
            config: 設定（包含 taker_fee）
        
        Returns:
            (entry_fee, exit_fee)
            
        說明：
          - 入場：Taker 手續費（市价成交），双侧都要支付
          - 出場：Taker 手續費（市价成交），双侧都要支付
          - 總費用 = position_size × (taker_fee × 2) + position_size × (taker_fee × 2)
            = position_size × taker_fee × 4
        """
        
        # 入場費用：双侧都用 Taker 費率
        entry_fee = position_size * config.taker_fee * 2
        
        # 出場費用：双侧都用 Taker 費率
        exit_fee = position_size * config.taker_fee * 2
        
        return entry_fee, exit_fee

