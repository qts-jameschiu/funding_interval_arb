"""
Opportunity Loader
从分析結果 CSV 加載交易機會並進行驗證和分組
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from datetime import datetime
import logging

from backtest.backtest_config import BacktestConfig


logger = logging.getLogger(__name__)


class Opportunity:
    """交易機會資料結构"""
    
    def __init__(self, row: pd.Series, K: float = 0, n_tradable_at_time: int = 1):
        """
        初始化機會
        
        Args:
            row: CSV 行資料
            K: 分配的資金
            n_tradable_at_time: 该時間点 tradable 的 symbol 個數
        """
        # 解析時間戳
        ts = pd.to_datetime(row['timestamp'])
        self.timestamp = int(ts.timestamp() * 1000)  # 轉為毫秒
        self.timestamp_str = str(row['timestamp'])
        self.symbol = row['symbol']
        
        # 資金信息
        self.K = K
        self.n_tradable_at_time = n_tradable_at_time
        
        # Funding rate 信息
        self.binance_interval = row.get('binance_interval', '')
        self.bybit_interval = row.get('bybit_interval', '')
        self.mismatch_type = row.get('mismatch_type', '')
        self.binance_pay = bool(row.get('binance_pay', False))
        self.bybit_pay = bool(row.get('bybit_pay', False))
        self.tradable = bool(row.get('tradable', False))
        
        # Funding rates（正負代表方向）
        # 注意：CSV 中的欄位是 'binance_rate' 和 'bybit_rate'（不是 binance_funding_rate 等）
        self.binance_rate = float(row.get('binance_rate', row.get('binance_funding_rate', 0)))
        self.bybit_rate = float(row.get('bybit_rate', row.get('bybit_funding_rate', 0)))
        self.rate_diff = float(row.get('rate_diff', 0))
        
        # 持续時間
        self.duration_hours = float(row.get('duration_hours', 0))
        
        # VWAP 相关（后续填充）
        self.vwap_entry_binance = None
        self.vwap_entry_bybit = None
        self.vwap_exit_binance = None
        self.vwap_exit_bybit = None
        self.vwap_valid = False
        
        # Volume 信息
        self.entry_volume_bn = 0
        self.exit_volume_bn = 0
        self.entry_volume_by = 0
        self.exit_volume_by = 0
    
    def to_dict(self) -> Dict:
        """轉換為字典"""
        return {
            'timestamp': self.timestamp,
            'timestamp_str': self.timestamp_str,
            'symbol': self.symbol,
            'K': self.K,
            'n_tradable_at_time': self.n_tradable_at_time,
            'binance_interval': self.binance_interval,
            'bybit_interval': self.bybit_interval,
            'mismatch_type': self.mismatch_type,
            'binance_pay': self.binance_pay,
            'bybit_pay': self.bybit_pay,
            'tradable': self.tradable,
            'binance_rate': self.binance_rate,
            'bybit_rate': self.bybit_rate,
            'rate_diff': self.rate_diff,
            'duration_hours': self.duration_hours,
            'vwap_entry_binance': self.vwap_entry_binance,
            'vwap_entry_bybit': self.vwap_entry_bybit,
            'vwap_exit_binance': self.vwap_exit_binance,
            'vwap_exit_bybit': self.vwap_exit_bybit,
            'vwap_valid': self.vwap_valid,
        }


class OpportunityLoader:
    """交易機會加載器"""
    
    def __init__(self, data_dir: str = "/home/james/research_output/funding_interval_arb/existence_analysis/data"):
        """
        初始化加載器
        
        Args:
            data_dir: 分析資料目錄
        """
        self.data_dir = Path(data_dir)
        self.opportunities: List[Opportunity] = []
        self.grouped_by_timestamp: Dict[int, List[Opportunity]] = defaultdict(list)
        self.symbol_list: List[str] = []
    
    def load_tradable_opportunities(self, config: BacktestConfig) -> List[Opportunity]:
        """
        加載 tradable 機會
        
        Args:
            config: 設定物件
            
        Returns:
            機會列表
        """
        if not self.data_dir.exists():
            raise FileNotFoundError(f"資料目錄不存在: {self.data_dir}")
        
        # 查找所有 funding_rate_timeline_*.csv 檔案
        csv_files = sorted(self.data_dir.glob("funding_rate_timeline_*.csv"))
        logger.info(f"找到 {len(csv_files)} 個資料檔案")
        
        if not csv_files:
            raise FileNotFoundError(f"未找到 funding_rate_timeline_*.csv 檔案 in {self.data_dir}")
        
        # 取得時間範圍
        start_ms, end_ms = config.get_time_range()
        start_date = datetime.fromtimestamp(start_ms / 1000)
        end_date = datetime.fromtimestamp(end_ms / 1000)
        
        logger.info(f"篩選時間範圍: {start_date} ~ {end_date}")
        
        all_data = []
        
        # 讀取所有 CSV
        for csv_file in csv_files:
            try:
                symbol = csv_file.stem.replace("funding_rate_timeline_", "")
                logger.debug(f"讀取 {symbol}...")
                
                df = pd.read_csv(csv_file)
                df['symbol'] = symbol
                all_data.append(df)
                
            except Exception as e:
                logger.warning(f"讀取檔案 {csv_file} 失败: {e}")
                continue
        
        if not all_data:
            raise ValueError("无法讀取任何資料檔案")
        
        # 合並所有資料
        df_all = pd.concat(all_data, ignore_index=True)
        logger.info(f"總行數: {len(df_all)}")
        
        # 轉換時間戳列 (處理兩種列名)
        if 'datetime' in df_all.columns:
            df_all['timestamp'] = pd.to_datetime(df_all['datetime'])
        else:
            df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
        
        # 篩選條件
        df_tradable = df_all[df_all['tradable'] == True].copy()
        logger.info(f"Tradable 機會: {len(df_tradable)}")
        
        # 篩選時間範圍
        df_tradable = df_tradable[
            (df_tradable['timestamp'] >= start_date) &
            (df_tradable['timestamp'] <= end_date)
        ]
        logger.info(f"時間範圍内的 Tradable 機會: {len(df_tradable)}")
        
        # 按 symbol 收集
        symbols_in_range = df_tradable['symbol'].unique()
        self.symbol_list = sorted(list(symbols_in_range))
        logger.info(f"涉及的 symbols: {len(self.symbol_list)} 個")
        
        # 应用 symbol 過濾
        if not config.config["symbols"]["include_all"]:
            whitelist = config.config["symbols"]["symbol_whitelist"]
            df_tradable = df_tradable[df_tradable['symbol'].isin(whitelist)]
            logger.info(f"应用 whitelist，剩余: {len(df_tradable)}")
        
        exclude = config.config["symbols"]["exclude_symbols"]
        if exclude:
            df_tradable = df_tradable[~df_tradable['symbol'].isin(exclude)]
            logger.info(f"排除 symbols，剩余: {len(df_tradable)}")
        
        # 按時間戳分組統計
        timestamp_counts = df_tradable.groupby('timestamp').size()
        logger.info(f"不同時間戳: {len(timestamp_counts)} 個")
        logger.info(f"單時間点最多 symbols: {timestamp_counts.max()}")
        logger.info(f"單時間点最少 symbols: {timestamp_counts.min()}")
        
        # 创建 Opportunity 物件
        opportunities_list = []
        
        for timestamp, group in df_tradable.groupby('timestamp'):
            n_tradable = len(group)
            K_per_symbol = config.initial_capital / n_tradable
            
            for _, row in group.iterrows():
                opp = Opportunity(row, K=K_per_symbol, n_tradable_at_time=n_tradable)
                opportunities_list.append(opp)
                
                # 按時間戳分組
                ts_ms = int(timestamp.timestamp() * 1000)
                self.grouped_by_timestamp[ts_ms].append(opp)
        
        self.opportunities = opportunities_list
        logger.info(f"创建 {len(opportunities_list)} 個 Opportunity 物件")
        
        return opportunities_list
    
    def get_opportunities_by_timestamp(self) -> Dict[int, List[Opportunity]]:
        """取得按時間戳分組的機會"""
        return dict(self.grouped_by_timestamp)
    
    def get_unique_symbols(self) -> List[str]:
        """取得涉及的 symbols"""
        return self.symbol_list
    
    def get_summary(self) -> Dict:
        """取得統計摘要"""
        if not self.opportunities:
            return {
                'total_opportunities': 0,
                'unique_symbols': 0,
                'unique_timestamps': 0,
                'total_capital_allocated': 0,
                'avg_capital_per_opp': 0,
            }
        
        df_opps = pd.DataFrame([opp.to_dict() for opp in self.opportunities])
        
        total_capital = sum(opp.K for opp in self.opportunities)
        
        return {
            'total_opportunities': len(self.opportunities),
            'unique_symbols': len(self.symbol_list),
            'unique_timestamps': len(self.grouped_by_timestamp),
            'total_capital_allocated': total_capital,
            'avg_capital_per_opp': total_capital / len(self.opportunities) if self.opportunities else 0,
            'min_capital': df_opps['K'].min() if len(df_opps) > 0 else 0,
            'max_capital': df_opps['K'].max() if len(df_opps) > 0 else 0,
            'total_duration_hours': df_opps['duration_hours'].sum() if len(df_opps) > 0 else 0,
            'avg_duration_hours': df_opps['duration_hours'].mean() if len(df_opps) > 0 else 0,
        }
    
    def validate_opportunity(self, opp: Opportunity) -> Tuple[bool, str]:
        """
        驗證單個機會
        
        Returns:
            (is_valid, reason)
        """
        # 檢查必需字段
        if not opp.symbol:
            return False, "symbol 缺失"
        
        if opp.timestamp <= 0:
            return False, "timestamp 無效"
        
        if not (opp.binance_pay or opp.bybit_pay):
            return False, "pay flags 都為 False"
        
        if opp.binance_pay and opp.bybit_pay:
            return False, "pay flags 都為 True"
        
        if opp.K <= 0:
            return False, "K 不合法"
        
        if opp.duration_hours <= 0:
            return False, "duration_hours 不合法"
        
        return True, "OK"
    
    def filter_valid_opportunities(self) -> List[Opportunity]:
        """篩選有效的機會"""
        valid_opps = []
        
        for opp in self.opportunities:
            is_valid, reason = self.validate_opportunity(opp)
            if is_valid:
                valid_opps.append(opp)
            else:
                logger.warning(f"Invalid opportunity: {opp.symbol} @ {opp.timestamp_str}: {reason}")
        
        logger.info(f"有效機會: {len(valid_opps)}/{len(self.opportunities)}")
        return valid_opps


def print_summary(loader: OpportunityLoader):
    """打印摘要"""
    summary = loader.get_summary()
    
    print("\n" + "=" * 60)
    print("Opportunity Loading Summary")
    print("=" * 60)
    print(f"Total Opportunities: {summary['total_opportunities']}")
    print(f"Unique Symbols: {summary['unique_symbols']}")
    print(f"Unique Timestamps: {summary['unique_timestamps']}")
    print(f"Total Capital Allocated: ${summary['total_capital_allocated']:,.2f}")
    print(f"Avg Capital per Opportunity: ${summary['avg_capital_per_opp']:,.2f}")
    print(f"Capital Range: ${summary['min_capital']:,.2f} - ${summary['max_capital']:,.2f}")
    print(f"Avg Duration: {summary['avg_duration_hours']:.2f} hours")
    print("=" * 60 + "\n")

