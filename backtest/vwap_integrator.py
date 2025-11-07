"""
VWAP 集成器
批量計算所有機會的 VWAP
"""

import pandas as pd
import logging
from typing import List, Dict, Tuple

from backtest.vwap_calculator import VWAPCalculator

logger = logging.getLogger(__name__)


class VWAPIntegrator:
    """VWAP 集成器"""
    
    @staticmethod
    def calculate_vwaps_for_all_opportunities(
        opportunities: List[Dict],
        klines_dict: Dict[str, Dict[str, pd.DataFrame]],
        vwap_window_minutes: int
    ) -> Tuple[List[Dict], Dict]:
        """
        為所有機會計算 VWAP
        
        Args:
            opportunities: 機會列表 (字典)
            klines_dict: K 線資料字典 {symbol: {exchange: df}}
            vwap_window_minutes: VWAP 窗口 (分鐘)
        
        Returns:
            (更新後的機會列表, 統計資訊)
        """
        logger.info(f"開始為 {len(opportunities)} 個機會計算 VWAP")
        
        updated_opportunities = []
        success_count = 0
        failure_count = 0
        failure_reasons = {}
        
        for i, opp in enumerate(opportunities):
            try:
                # 轉換為字典 (如果是 Opportunity 對象)
                if hasattr(opp, 'to_dict'):
                    opp_dict = opp.to_dict()
                else:
                    opp_dict = opp if isinstance(opp, dict) else dict(opp)
                
                # 獲取該機會的 symbol
                symbol = opp_dict.get('symbol')
                if not symbol:
                    raise ValueError(f"機會 {i} 沒有 symbol")
                
                # 為該 symbol 提取 K 線資料 {exchange: df}
                symbol_klines = klines_dict.get(symbol, {})
                if not symbol_klines:
                    raise ValueError(f"找不到 {symbol} 的 K 線資料")
                
                # 計算 VWAP
                vwap_entry_bn, vwap_entry_by, vwap_exit_bn, vwap_exit_by, is_valid = \
                    VWAPCalculator.calculate_entry_exit_vwap(
                        opp_dict, symbol_klines, vwap_window_minutes
                    )
                
                # 更新機會字典
                opp_dict['vwap_entry_binance'] = vwap_entry_bn
                opp_dict['vwap_entry_bybit'] = vwap_entry_by
                opp_dict['vwap_exit_binance'] = vwap_exit_bn
                opp_dict['vwap_exit_bybit'] = vwap_exit_by
                opp_dict['vwap_valid'] = is_valid
                
                if is_valid:
                    success_count += 1
                else:
                    failure_count += 1
                    reason = "VWAP 計算無效"
                    failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
                
                updated_opportunities.append(opp_dict)
                
            except Exception as e:
                logger.warning(f"機會 {i} VWAP 計算失敗: {e}")
                failure_count += 1
                reason = str(e)
                failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
                
                # 轉換為字典並標記為無效
                if hasattr(opp, 'to_dict'):
                    opp_dict = opp.to_dict()
                else:
                    opp_dict = opp if isinstance(opp, dict) else dict(opp)
                
                opp_dict['vwap_valid'] = False
                updated_opportunities.append(opp_dict)
        
        stats = {
            'total': len(opportunities),
            'success': success_count,
            'failure': failure_count,
            'success_rate': (success_count / len(opportunities) * 100) if opportunities else 0,
            'failure_reasons': failure_reasons
        }
        
        logger.info(f"VWAP 計算完成: {success_count}/{len(opportunities)} 成功")
        
        return updated_opportunities, stats

