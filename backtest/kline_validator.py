"""
K 線驗證器
驗證 K 線資料的完整性、連續性和異常值
"""

import pandas as pd
import logging
from typing import Tuple, Dict, List

logger = logging.getLogger(__name__)


class KlineValidator:
    """K 線驗證類"""
    
    COVERAGE_THRESHOLD = 0.95  # 95% 覆蓋率
    MAX_GAP_MINUTES = 5  # 最大 5 分鐘缺口
    VOLATILITY_THRESHOLD = 0.1  # 10% 波動閾值
    
    @staticmethod
    def validate_completeness(
        klines_df: pd.DataFrame,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp
    ) -> Tuple[bool, Dict]:
        """
        驗證 K 線完整性
        
        Returns:
            (is_valid, validation_result_dict)
        """
        try:
            # 計算期望記錄數
            time_diff = end_time - start_time
            expected_candles = int(time_diff.total_seconds() / 60)
            
            # 篩選時間範圍
            mask = (klines_df['timestamp'] >= start_time) & (klines_df['timestamp'] <= end_time)
            window_data = klines_df[mask]
            
            actual_candles = len(window_data)
            coverage = actual_candles / expected_candles if expected_candles > 0 else 0
            
            result = {
                'is_valid': coverage >= KlineValidator.COVERAGE_THRESHOLD,
                'coverage_pct': coverage * 100,
                'expected_candles': expected_candles,
                'actual_candles': actual_candles,
                'gaps': [],
                'anomalies': []
            }
            
            # 檢查時間缺口
            if len(window_data) > 1:
                time_diffs = window_data['timestamp'].diff()
                gaps = time_diffs[time_diffs > pd.Timedelta(minutes=KlineValidator.MAX_GAP_MINUTES)]
                result['gaps'] = len(gaps)
                if len(gaps) > 0:
                    result['is_valid'] = False
            
            # 檢查異常值
            anomalies = []
            if (window_data['volume'] <= 0).any():
                anomalies.append('volume_zero')
            if (window_data['close'] <= 0).any():
                anomalies.append('price_zero')
            
            result['anomalies'] = anomalies
            if len(anomalies) > 0:
                result['is_valid'] = False
            
            return result['is_valid'], result
            
        except Exception as e:
            logger.error(f"驗證失敗: {e}")
            return False, {'error': str(e)}

