"""
Trade Direction Determination
根據 funding pay flags 和 funding rate 判斷交易方向

四種交易方向：
1. LONG_BYBIT_SHORT_BINANCE: 做多 Bybit，做空 Binance
2. SHORT_BYBIT_LONG_BINANCE: 做空 Bybit，做多 Binance
3. LONG_BINANCE_SHORT_BYBIT: 做多 Binance，做空 Bybit
4. SHORT_BINANCE_LONG_BYBIT: 做空 Binance，做多 Bybit
"""

from typing import Tuple, Optional
from enum import Enum


class TradeDirection(Enum):
    """交易方向枚举"""
    LONG_BYBIT_SHORT_BINANCE = "long_bybit_short_binance"
    SHORT_BYBIT_LONG_BINANCE = "short_bybit_long_binance"
    LONG_BINANCE_SHORT_BYBIT = "long_binance_short_bybit"
    SHORT_BINANCE_LONG_BYBIT = "short_binance_long_bybit"


class TradeDirectionDeterminer:
    """交易方向判斷器"""
    
    @staticmethod
    def determine_direction(
        bybit_pay: bool,
        binance_pay: bool,
        bybit_rate: float,
        binance_rate: float
    ) -> Tuple[TradeDirection, str, str]:
        """
        根據 pay flags 和 funding rate 判斷交易方向
        
        邏輯：
          Case 1: bybit_pay=True && binance_pay=False
            根據 bybit_rate 的符號：
              if bybit_rate > 0: SHORT_BYBIT_LONG_BINANCE (Bybit 多头支付，避免支付)
              if bybit_rate < 0: LONG_BYBIT_SHORT_BINANCE (Bybit 多头接收，接收funding)
          
          Case 2: binance_pay=True && bybit_pay=False
            根據 binance_rate 的符號：
              if binance_rate > 0: SHORT_BINANCE_LONG_BYBIT (Binance 多头支付，避免支付)
              if binance_rate < 0: LONG_BINANCE_SHORT_BYBIT (Binance 多头接收，接收funding)
        
        Args:
            bybit_pay: Bybit 是否支付 funding
            binance_pay: Binance 是否支付 funding
            bybit_rate: Bybit funding rate (正=多头支付，負=多头接收)
            binance_rate: Binance funding rate
        
        Returns:
            (TradeDirection, receiving_exchange, paying_exchange)
            receiving_exchange: 接收 funding 的交易所
            paying_exchange: 我们规避支付 funding 的交易所
        """
        
        # Case 1: bybit_pay=True && binance_pay=False
        if bybit_pay and not binance_pay:
            if bybit_rate < 0:
                # Bybit 多头接收 funding
                return (
                    TradeDirection.LONG_BYBIT_SHORT_BINANCE,
                    "bybit",
                    "binance"
                )
            else:  # bybit_rate > 0 或 == 0
                # Bybit 多头支付 funding，我们做空避免支付，Binance 接收 funding
                return (
                    TradeDirection.SHORT_BYBIT_LONG_BINANCE,
                    "binance",
                    "bybit"
                )
        
        # Case 2: binance_pay=True && bybit_pay=False
        elif binance_pay and not bybit_pay:
            if binance_rate < 0:
                # Binance 多头接收 funding
                return (
                    TradeDirection.LONG_BINANCE_SHORT_BYBIT,
                    "binance",
                    "bybit"
                )
            else:  # binance_rate > 0 或 == 0
                # Binance 多头支付 funding，我们做空避免支付，Bybit 接收 funding
                return (
                    TradeDirection.SHORT_BINANCE_LONG_BYBIT,
                    "bybit",
                    "binance"
                )
        
        else:
            raise ValueError(
                f"Invalid pay flags: bybit_pay={bybit_pay}, binance_pay={binance_pay}. "
                "Exactly one must be True."
            )
    
    @staticmethod
    def validate_direction(direction: TradeDirection) -> bool:
        """驗證交易方向有效"""
        return isinstance(direction, TradeDirection)
    
    @staticmethod
    def get_direction_description(direction: TradeDirection) -> str:
        """取得交易方向描述"""
        descriptions = {
            TradeDirection.LONG_BYBIT_SHORT_BINANCE: (
                "Long Bybit (多) + Short Binance (空)\n"
                "做多 Bybit 接收 funding，做空 Binance 规避支付"
            ),
            TradeDirection.SHORT_BYBIT_LONG_BINANCE: (
                "Short Bybit (空) + Long Binance (多)\n"
                "做空 Bybit 规避支付，做多 Binance 接收 funding"
            ),
            TradeDirection.LONG_BINANCE_SHORT_BYBIT: (
                "Long Binance (多) + Short Bybit (空)\n"
                "做多 Binance 接收 funding，做空 Bybit 规避支付"
            ),
            TradeDirection.SHORT_BINANCE_LONG_BYBIT: (
                "Short Binance (空) + Long Bybit (多)\n"
                "做空 Binance 规避支付，做多 Bybit 接收 funding"
            ),
        }
        return descriptions.get(direction, "Unknown direction")

