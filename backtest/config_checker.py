"""
設定檢查和時間覆蓋驗證
確保回測時間範圍被既有分析資料覆蓋
"""

from pathlib import Path
from datetime import datetime
import logging
from typing import Tuple, Optional

from backtest.backtest_config import BacktestConfig

logger = logging.getLogger(__name__)


class ConfigChecker:
    """設定檢查器"""
    
    def __init__(self, data_dir: str = "/home/james/research_output/funding_interval_arb/existence_analysis/data"):
        """
        初始化檢查器
        
        Args:
            data_dir: 既有分析資料目錄
        """
        self.data_dir = Path(data_dir)
    
    def get_existing_analysis_date_range(self) -> Optional[Tuple[datetime, datetime]]:
        """
        從既有分析資料推斷時間範圍
        
        Returns:
            (start_date, end_date) 或 None 若無資料
        """
        if not self.data_dir.exists():
            logger.warning(f"分析資料目錄不存在: {self.data_dir}")
            return None
        
        csv_files = list(self.data_dir.glob("funding_rate_timeline_*.csv"))
        
        if not csv_files:
            logger.warning("未找到既有分析資料檔案")
            return None
        
        # 讀取第一個檔案以取得時間範圍
        import pandas as pd
        
        dates = []
        for csv_file in csv_files[:3]:  # 只檢查前 3 個檔案以加快速度
            try:
                df = pd.read_csv(csv_file)
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    dates.append(df['timestamp'].min())
                    dates.append(df['timestamp'].max())
            except Exception as e:
                logger.warning(f"讀取 {csv_file} 失敗: {e}")
                continue
        
        if not dates:
            logger.warning("無法從分析資料中提取時間資訊")
            return None
        
        start_date = min(dates)
        end_date = max(dates)
        
        logger.info(f"既有分析資料時間範圍: {start_date.date()} ~ {end_date.date()}")
        
        return start_date, end_date
    
    def check_time_coverage(self, config: BacktestConfig) -> Tuple[bool, str]:
        """
        檢查設定的時間範圍是否被既有分析覆蓋
        
        Returns:
            (is_covered, message)
        """
        existing_range = self.get_existing_analysis_date_range()
        
        if existing_range is None:
            return False, "未找到既有分析資料，需要運行分析"
        
        existing_start, existing_end = existing_range
        
        # 解析設定的時間範圍
        config_start = datetime.strptime(config.start_date, "%Y-%m-%d")
        config_end = datetime.strptime(config.end_date, "%Y-%m-%d")
        
        # 檢查覆蓋
        if config_start >= existing_start and config_end <= existing_end:
            return True, f"時間範圍已覆蓋 ({config_start.date()} ~ {config_end.date()})"
        else:
            msg = (
                f"時間範圍未完全覆蓋。\n"
                f"  需要: {config_start.date()} ~ {config_end.date()}\n"
                f"  已有: {existing_start.date()} ~ {existing_end.date()}"
            )
            return False, msg
    
    def should_run_analysis(self, config: BacktestConfig) -> Tuple[bool, str]:
        """
        判斷是否需要運行分析
        
        Returns:
            (should_run, reason)
        """
        # 如果使用者明確設定 run_analysis_first=False，檢查覆蓋
        if not config.run_analysis_first:
            is_covered, msg = self.check_time_coverage(config)
            if is_covered:
                return False, "時間範圍已覆蓋，無需運行分析"
            else:
                return True, f"時間範圍未覆蓋，需要運行分析: {msg}"
        
        # 如果使用者明確設定 run_analysis_first=True
        return True, "使用者設定強制運行分析"
    
    def validate_config_consistency(self, config: BacktestConfig) -> Tuple[bool, list]:
        """
        驗證設定的一致性
        
        Returns:
            (is_valid, list_of_issues)
        """
        issues = []
        
        # 檢查初始資金
        if config.initial_capital <= 0:
            issues.append("初始資金必須大於 0")
        
        if config.initial_capital > 1_000_000:
            issues.append(f"初始資金過高: ${config.initial_capital:,.2f}")
        
        # 檢查 VWAP 窗口
        if config.vwap_window_minutes <= 0:
            issues.append("VWAP 窗口必須大於 0")
        
        if config.vwap_window_minutes > 120:
            issues.append(f"VWAP 窗口過大: {config.vwap_window_minutes} 分鐘")
        
        # 檢查手續費
        if config.maker_fee < 0 or config.maker_fee > 0.01:
            issues.append(f"Maker 手續費異常: {config.maker_fee * 100:.4f}%")
        
        if config.taker_fee < 0 or config.taker_fee > 0.01:
            issues.append(f"Taker 手續費異常: {config.taker_fee * 100:.4f}%")
        
        # 檢查日期
        start = datetime.strptime(config.start_date, "%Y-%m-%d")
        end = datetime.strptime(config.end_date, "%Y-%m-%d")
        
        if (end - start).days < 1:
            issues.append("時間範圍太短 (至少 1 天)")
        
        if (end - start).days > 365:
            issues.append(f"時間範圍過長: {(end - start).days} 天")
        
        return len(issues) == 0, issues

