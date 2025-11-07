"""
VWAP (Volume Weighted Average Price) 計算器
根據時間窗口計算成交量加權平均價格
"""

import pandas as pd
import numpy as np
from typing import Optional, Tuple, Dict
import logging

logger = logging.getLogger(__name__)


class VWAPCalculator:
    """VWAP 計算器"""
    
    @staticmethod
    def calculate_vwap(
        klines_df: pd.DataFrame,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        min_required_candles: int = 3
    ) -> Optional[float]:
        """
        計算指定時間窗口的 VWAP
        
        Args:
            klines_df: K 線資料框 (必須包含 'timestamp', 'high', 'low', 'close', 'volume')
            start_time: 窗口開始時間
            end_time: 窗口結束時間
            min_required_candles: 最少需要的蠟燭數量
        
        Returns:
            VWAP 值或 None (若資料不足)
            
        VWAP 公式:
            典型價格 = (high + low + close) / 3
            VWAP = Σ(典型價格 × 交易量) / Σ(交易量)
        """
        try:
            # 篩選時間窗口
            mask = (klines_df['timestamp'] >= start_time) & (klines_df['timestamp'] <= end_time)
            window_data = klines_df[mask].copy()
            
            if len(window_data) < min_required_candles:
                logger.debug(
                    f"資料不足: {len(window_data)} < {min_required_candles} "
                    f"({start_time} ~ {end_time})"
                )
                return None
            
            # 計算典型價格
            window_data['typical_price'] = (
                (window_data['high'] + window_data['low'] + window_data['close']) / 3
            )
            
            # 計算 VWAP
            numerator = (window_data['typical_price'] * window_data['volume']).sum()
            denominator = window_data['volume'].sum()
            
            if denominator == 0:
                logger.warning(f"交易量為 0 ({start_time} ~ {end_time})")
                return None
            
            vwap = numerator / denominator
            
            return vwap
            
        except Exception as e:
            logger.error(f"VWAP 計算失敗: {e}")
            return None
    
    @staticmethod
    def calculate_entry_exit_vwap(
        opportunity: Dict,
        klines_dict: Dict[str, pd.DataFrame],
        vwap_window_minutes: int
    ) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], bool]:
        """
        計算入場和出場 VWAP
        
        Args:
            opportunity: 機會物件，包含 timestamp, symbol 等
            klines_dict: K 線字典 {exchange: dataframe}
            vwap_window_minutes: VWAP 窗口 (分鐘)
        
        Returns:
            (vwap_entry_bn, vwap_entry_by, vwap_exit_bn, vwap_exit_by, is_valid)
        """
        try:
            # 獲取時間戳 (轉為毫秒)
            # 支持字典和 Opportunity 對象
            if isinstance(opportunity, dict):
                timestamp_ms = opportunity.get('timestamp')
            else:
                timestamp_ms = getattr(opportunity, 'timestamp', None)
            
            if timestamp_ms is None:
                return None, None, None, None, False
            
            # 轉換為 pd.Timestamp
            timestamp = pd.Timestamp(timestamp_ms, unit='ms')
            
            # 計算窗口
            entry_start = timestamp - pd.Timedelta(minutes=vwap_window_minutes)
            entry_end = timestamp
            exit_start = timestamp
            exit_end = timestamp + pd.Timedelta(minutes=vwap_window_minutes)
            
            results = {}
            
            # 對每個交易所計算 VWAP
            for exchange in ['binance', 'bybit']:
                if exchange not in klines_dict or klines_dict[exchange] is None:
                    logger.warning(f"缺少 {exchange} 的 K 線資料")
                    return None, None, None, None, False
                
                klines = klines_dict[exchange]
                
                # 確保 timestamp 列是 Timestamp 型態
                if not isinstance(klines['timestamp'].dtype, type(pd.Timestamp)):
                    klines['timestamp'] = pd.to_datetime(klines['timestamp'])
                
                # 計算入場 VWAP
                entry_vwap = VWAPCalculator.calculate_vwap(
                    klines, entry_start, entry_end
                )
                
                # 計算出場 VWAP
                exit_vwap = VWAPCalculator.calculate_vwap(
                    klines, exit_start, exit_end
                )
                
                results[exchange] = {
                    'entry_vwap': entry_vwap,
                    'exit_vwap': exit_vwap
                }
            
            # 提取結果
            vwap_entry_bn = results['binance']['entry_vwap']
            vwap_exit_bn = results['binance']['exit_vwap']
            vwap_entry_by = results['bybit']['entry_vwap']
            vwap_exit_by = results['bybit']['exit_vwap']
            
            # 驗證所有 VWAP 都有效
            is_valid = all([
                vwap_entry_bn is not None,
                vwap_exit_bn is not None,
                vwap_entry_by is not None,
                vwap_exit_by is not None
            ])
            
            return vwap_entry_bn, vwap_entry_by, vwap_exit_bn, vwap_exit_by, is_valid
            
        except Exception as e:
            logger.error(f"計算入場/出場 VWAP 失敗: {e}")
            return None, None, None, None, False
    
    @staticmethod
    def validate_vwap(
        vwap: float,
        klines_df: pd.DataFrame,
        low: float = None,
        high: float = None
    ) -> bool:
        """
        驗證 VWAP 的有效性
        
        Args:
            vwap: VWAP 值
            klines_df: K 線資料框
            low: 期望的最低價 (可選)
            high: 期望的最高價 (可選)
        
        Returns:
            True 若 VWAP 有效
        """
        # 檢查是否為 NaN
        if vwap is None or np.isnan(vwap):
            return False
        
        # 檢查是否為正數
        if vwap <= 0:
            return False
        
        # 如果提供了價格範圍，檢查 VWAP 是否在範圍內
        if low is not None and high is not None:
            if vwap < low or vwap > high:
                logger.warning(f"VWAP {vwap} 超出範圍 [{low}, {high}]")
                return False
        
        return True

