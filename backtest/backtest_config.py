"""
Backtest Configuration Management
支持 JSON 設定加載、驗證和參數管理
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple, Optional


class BacktestConfig:
    """回測設定類 - 負責加載、驗證和管理設定參數"""
    
    # 預設設定
    DEFAULT_CONFIG = {
        "analysis": {
            "run_analysis_first": False,
            "start_date": "2025-08-07",
            "end_date": "2025-11-05"
        },
        "trading": {
            "initial_capital": 100000,
            "vwap_window_minutes": 5,
            "entry_buffer_pct": 0.0005,
            "exit_buffer_pct": 0.0005
        },
        "fees": {
            "maker_fee": 0.0002,
            "taker_fee": 0.0004
        },
        "symbols": {
            "include_all": True,
            "symbol_whitelist": [],
            "exclude_symbols": []
        },
        "output": {
            "output_dir": "/home/james/research_output/funding_interval_arb/backtest_results",
            "save_detailed_trades": True,
            "save_equity_curve": True,
            "generate_plots": True
        }
    }
    
    # 必需的設定项
    REQUIRED_FIELDS = [
        "analysis.run_analysis_first",
        "analysis.start_date",
        "analysis.end_date",
        "trading.initial_capital",
        "trading.vwap_window_minutes",
        "fees.maker_fee",
        "fees.taker_fee",
    ]
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        初始化設定
        
        Args:
            config_dict: 設定字典，若 None 则使用預設設定
        """
        if config_dict is None:
            self.config = self.DEFAULT_CONFIG.copy()
        else:
            # 深度合並設定和預設值
            self.config = self._deep_merge(self.DEFAULT_CONFIG, config_dict)
        
        self._validate()
    
    @staticmethod
    def _deep_merge(default: Dict, override: Dict) -> Dict:
        """深度合並字典"""
        result = default.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = BacktestConfig._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    @classmethod
    def load_from_json(cls, filepath: str) -> "BacktestConfig":
        """
        从 JSON 檔案加載設定
        
        Args:
            filepath: JSON 設定檔案路徑
            
        Returns:
            BacktestConfig 实例
            
        Raises:
            FileNotFoundError: 檔案不存在
            json.JSONDecodeError: JSON 格式錯誤
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"設定檔案不存在: {filepath}")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                config_dict = json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"設定檔案 JSON 格式錯誤: {e}", e.doc, e.pos)
        
        return cls(config_dict)
    
    def _validate(self):
        """驗證設定的有效性"""
        # 檢查必需字段
        for field in self.REQUIRED_FIELDS:
            parts = field.split('.')
            value = self.config
            try:
                for part in parts:
                    value = value[part]
            except KeyError:
                raise ValueError(f"缺少必需設定项: {field}")
        
        # 驗證資料型態
        if not isinstance(self.config["trading"]["initial_capital"], (int, float)):
            raise TypeError("initial_capital 必须是數字")
        
        if self.config["trading"]["initial_capital"] <= 0:
            raise ValueError("initial_capital 必须大于 0")
        
        if not isinstance(self.config["trading"]["vwap_window_minutes"], int):
            raise TypeError("vwap_window_minutes 必须是整數")
        
        if self.config["trading"]["vwap_window_minutes"] <= 0:
            raise ValueError("vwap_window_minutes 必须大于 0")
        
        # 驗證時間範圍
        try:
            start_date = datetime.strptime(
                self.config["analysis"]["start_date"], "%Y-%m-%d"
            )
            end_date = datetime.strptime(
                self.config["analysis"]["end_date"], "%Y-%m-%d"
            )
        except ValueError as e:
            raise ValueError(f"日期格式錯誤，应為 YYYY-MM-DD: {e}")
        
        if start_date >= end_date:
            raise ValueError(
                f"start_date ({self.config['analysis']['start_date']}) "
                f"必须小于 end_date ({self.config['analysis']['end_date']})"
            )
        
        # 驗證手續費
        for fee_type in ["maker_fee", "taker_fee"]:
            fee = self.config["fees"][fee_type]
            if not isinstance(fee, (int, float)):
                raise TypeError(f"{fee_type} 必须是數字")
            if fee < 0 or fee > 1:
                raise ValueError(f"{fee_type} 必须在 [0, 1] 範圍内")
    
    def get_time_range(self) -> Tuple[int, int]:
        """
        取得時間範圍（毫秒時間戳）
        
        Returns:
            (start_time_ms, end_time_ms) 元组
        """
        start_date = datetime.strptime(
            self.config["analysis"]["start_date"], "%Y-%m-%d"
        )
        end_date = datetime.strptime(
            self.config["analysis"]["end_date"], "%Y-%m-%d"
        )
        
        start_ms = int(start_date.timestamp() * 1000)
        # 对于 end_date，取该天的最后一秒
        end_ms = int((end_date.timestamp() + 86400) * 1000) - 1
        
        return start_ms, end_ms
    
    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典"""
        return json.loads(json.dumps(self.config))
    
    def __str__(self) -> str:
        """設定摘要"""
        summary = []
        summary.append("=" * 60)
        summary.append("Backtest Configuration Summary")
        summary.append("=" * 60)
        
        # 分析設定
        summary.append("\n📊 Analysis Settings:")
        summary.append(f"  Run analysis first: {self.config['analysis']['run_analysis_first']}")
        summary.append(f"  Date range: {self.config['analysis']['start_date']} ~ {self.config['analysis']['end_date']}")
        summary.append(f"  Duration: {self.config['analysis']['duration_days']} days")
        
        # 交易設定
        summary.append("\n💹 Trading Settings:")
        summary.append(f"  Initial capital: ${self.config['trading']['initial_capital']:,.2f}")
        summary.append(f"  VWAP window: {self.config['trading']['vwap_window_minutes']} minutes")
        summary.append(f"  Entry buffer: {self.config['trading']['entry_buffer_pct']*100:.4f}%")
        summary.append(f"  Exit buffer: {self.config['trading']['exit_buffer_pct']*100:.4f}%")
        
        # 手續費
        summary.append("\n💰 Fees:")
        summary.append(f"  Maker fee: {self.config['fees']['maker_fee']*100:.4f}%")
        summary.append(f"  Taker fee: {self.config['fees']['taker_fee']*100:.4f}%")
        
        # Symbol 設定
        summary.append("\n📝 Symbol Settings:")
        summary.append(f"  Include all: {self.config['symbols']['include_all']}")
        if self.config['symbols']['symbol_whitelist']:
            summary.append(f"  Whitelist: {', '.join(self.config['symbols']['symbol_whitelist'])}")
        if self.config['symbols']['exclude_symbols']:
            summary.append(f"  Exclude: {', '.join(self.config['symbols']['exclude_symbols'])}")
        
        # 输出設定
        summary.append("\n📁 Output Settings:")
        summary.append(f"  Output dir: {self.config['output']['output_dir']}")
        summary.append(f"  Save trades: {self.config['output']['save_detailed_trades']}")
        summary.append(f"  Save equity: {self.config['output']['save_equity_curve']}")
        summary.append(f"  Generate plots: {self.config['output']['generate_plots']}")
        
        summary.append("=" * 60)
        return "\n".join(summary)
    
    # 便捷属性访问
    @property
    def run_analysis_first(self) -> bool:
        return self.config["analysis"]["run_analysis_first"]
    
    @property
    def start_date(self) -> str:
        return self.config["analysis"]["start_date"]
    
    @property
    def end_date(self) -> str:
        return self.config["analysis"]["end_date"]
    
    @property
    def analysis_duration_days(self) -> int:
        """根據 start_date 和 end_date 自動計算天數"""
        start_date = datetime.strptime(
            self.config["analysis"]["start_date"], "%Y-%m-%d"
        )
        end_date = datetime.strptime(
            self.config["analysis"]["end_date"], "%Y-%m-%d"
        )
        delta = end_date - start_date
        return delta.days
    
    @property
    def initial_capital(self) -> float:
        return self.config["trading"]["initial_capital"]
    
    @property
    def vwap_window_minutes(self) -> int:
        return self.config["trading"]["vwap_window_minutes"]
    
    @property
    def entry_buffer_pct(self) -> float:
        return self.config["trading"]["entry_buffer_pct"]
    
    @property
    def exit_buffer_pct(self) -> float:
        return self.config["trading"]["exit_buffer_pct"]
    
    @property
    def maker_fee(self) -> float:
        return self.config["fees"]["maker_fee"]
    
    @property
    def taker_fee(self) -> float:
        return self.config["fees"]["taker_fee"]
    
    @property
    def output_dir(self) -> str:
        return self.config["output"]["output_dir"]

