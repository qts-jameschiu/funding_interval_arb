"""
Backtest Main Entry Point
主程序协调和流程管理
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

from backtest.backtest_config import BacktestConfig


# 設定日志
def setup_logging(debug: bool = False) -> logging.Logger:
    """初始化日志系統"""
    log_level = logging.DEBUG if debug else logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            # 可选：添加檔案处理器
        ]
    )
    
    return logging.getLogger(__name__)


def parse_arguments():
    """解析命令行參數"""
    parser = argparse.ArgumentParser(
        description="Funding Interval Arbitrage - Backtest System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用預設設定运行
  python -m backtest.backtest_main
  
  # 使用自定义設定
  python -m backtest.backtest_main --config /path/to/config.json
  
  # 强制运行分析
  python -m backtest.backtest_main --force-analysis
  
  # 调試模式
  python -m backtest.backtest_main --debug
        """
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='設定檔案路徑 (預設: backtest/config/default_backtest_config.json)'
    )
    
    parser.add_argument(
        '--force-analysis',
        action='store_true',
        help='强制运行存在性分析'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='启用调試模式'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='試运行模式 (仅加載設定，不執行回測)'
    )
    
    return parser.parse_args()


def get_default_config_path() -> str:
    """取得預設設定檔案路徑"""
    backtest_dir = Path(__file__).parent
    return str(backtest_dir / "config" / "default_backtest_config.json")


def load_config(config_path: Optional[str] = None) -> BacktestConfig:
    """加載設定檔案"""
    if config_path is None:
        config_path = get_default_config_path()
    
    config = BacktestConfig.load_from_json(config_path)
    return config


def check_time_coverage(config: BacktestConfig, logger: logging.Logger) -> bool:
    """
    檢查設定時間是否被已有分析覆蓋
    
    Returns:
        True 如果覆蓋，False 需要运行分析
    """
    from pathlib import Path
    
    # 查找既有分析資料
    output_base = Path("/home/james/research_output/funding_interval_arb/existence_analysis/data")
    
    if not output_base.exists():
        logger.warning(f"分析資料目錄不存在: {output_base}")
        logger.warning("强制设置 run_analysis_first=true")
        return False
    
    # 查找 funding_rate_timeline_*.csv 檔案
    csv_files = list(output_base.glob("funding_rate_timeline_*.csv"))
    
    if not csv_files:
        logger.warning("未找到既有分析資料")
        logger.warning("强制设置 run_analysis_first=true")
        return False
    
    logger.info(f"找到 {len(csv_files)} 個既有分析資料檔案")
    
    # 简單檢查: 只要有資料檔案就假设覆蓋了
    # 更复杂的檢查可以解析檔案查看日期範圍
    return True


def run_analysis_if_needed(config: BacktestConfig, logger: logging.Logger) -> bool:
    """
    如果需要，运行存在性分析
    
    Returns:
        True 如果分析成功，False 分析失败
    """
    import subprocess
    
    if not config.run_analysis_first:
        logger.info("跳过分析 (run_analysis_first=false)")
        return True
    
    logger.info("=" * 60)
    logger.info("Running Opportunity Analysis...")
    logger.info("=" * 60)
    
    # 執行分析脚本
    analysis_script = Path(__file__).parent.parent / "opportunity_analysis" / "main.py"
    
    if not analysis_script.exists():
        logger.error(f"分析脚本不存在: {analysis_script}")
        return False
    
    try:
        # 使用 Python 模組运行
        result = subprocess.run(
            [sys.executable, "-m", "opportunity_analysis.main"],
            cwd=str(Path(__file__).parent.parent),
            capture_output=False,
            timeout=3600  # 1 小時超时
        )
        
        if result.returncode != 0:
            logger.error("分析執行失败")
            return False
        
        logger.info("分析完成")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("分析執行超时")
        return False
    except Exception as e:
        logger.error(f"分析執行異常: {e}")
        return False


def print_summary(config: BacktestConfig, logger: logging.Logger):
    """打印設定摘要"""
    logger.info("\n" + str(config))


def main():
    """主程序"""
    # 解析參數
    args = parse_arguments()
    
    # 初始化日志
    logger = setup_logging(debug=args.debug)
    
    logger.info("=" * 60)
    logger.info("Funding Interval Arbitrage - Backtest System")
    logger.info("=" * 60)
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Step 1: 加載設定
        logger.info("\n[Step 1] Loading Configuration...")
        config = load_config(args.config)
        
        if args.force_analysis:
            logger.info("强制设置 run_analysis_first=true")
            config.config["analysis"]["run_analysis_first"] = True
        
        # Step 2: 檢查時間覆蓋
        logger.info("\n[Step 2] Checking Time Coverage...")
        is_covered = check_time_coverage(config, logger)
        
        if not is_covered:
            logger.warning("時間範圍未被覆蓋，强制运行分析")
            config.config["analysis"]["run_analysis_first"] = True
        
        # Step 3: 打印設定摘要
        print_summary(config, logger)
        
        # Step 4: 如果是試运行模式，提前退出
        if args.dry_run:
            logger.info("\n✓ Dry-run 完成")
            logger.info("=" * 60)
            return 0
        
        # Step 5: 如果需要，运行分析
        if config.run_analysis_first:
            logger.info("\n[Step 3] Running Analysis (if needed)...")
            if not run_analysis_if_needed(config, logger):
                logger.error("分析失败")
                return 1
        
        logger.info("\n" + "=" * 60)
        logger.info("Configuration Loaded Successfully")
        logger.info("Ready for backtest execution")
        logger.info("=" * 60)
        
        # 返回設定供后续使用（实际會继承到更多模組）
        return 0
        
    except KeyboardInterrupt:
        logger.info("\nBacktest interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

