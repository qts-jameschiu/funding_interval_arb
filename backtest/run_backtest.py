#!/usr/bin/env python
"""
完整回測執行腳本
集成所有模組來運行完整的回測流程
"""

import sys
import asyncio
import logging
import subprocess
from pathlib import Path
from datetime import datetime

# 確保可以導入 backtest 模塊
# run_backtest.py 路徑: /home/james/research/funding_interval_arb/backtest/run_backtest.py
# 需要的根目錄: /home/james/research/funding_interval_arb
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backtest.backtest_config import BacktestConfig
from backtest.opportunity_loader import OpportunityLoader
from backtest.vwap_calculator import VWAPCalculator
from backtest.vwap_integrator import VWAPIntegrator
from backtest.backtest_engine import BacktestEngine
from backtest.backtest_analyzer import BacktestAnalyzer
from backtest.backtest_visualizer import BacktestVisualizer
from backtest.kline_fetcher import KlineFetcher


# 設定日誌
def setup_logging(debug: bool = False) -> logging.Logger:
    """初始化日誌系統"""
    log_level = logging.DEBUG if debug else logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )
    
    return logging.getLogger(__name__)


def run_analysis_if_needed(config: BacktestConfig, logger: logging.Logger) -> bool:
    """
    如果需要，運行 analysis 以產生新的交易機會資料
    
    Args:
        config: 回測配置
        logger: 日誌記錄器
    
    Returns:
        成功返回 True，失敗返回 False
    """
    if not config.run_analysis_first:
        logger.info("Analysis 已禁用 (run_analysis_first=false)")
        return True
    
    logger.info("\n[Step 0/6] 運行 Analysis 以產生交易機會資料...")
    logger.info(f"  時間範圍: {config.start_date} ~ {config.end_date}")
    logger.info(f"  分析天數: {config.analysis_duration_days}")
    
    try:
        # 轉換配置的日期格式為 YYYY-MM-DD
        end_date = config.end_date.split(' ')[0]  # 從 "YYYY-MM-DD HH:MM:SS" 提取日期
        duration = config.analysis_duration_days
        
        # 調用 analysis/main.py
        # Note: analysis/main.py 支持 --end_date 和 --duration 參數
        cmd = [
            sys.executable,
            str(Path(__file__).parent.parent / "opportunity_analysis" / "main.py"),
            "--end_date", end_date,
            "--duration", str(duration),
            "--regen_data", "false"
        ]
        
        logger.info(f"  執行命令: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            cwd=str(Path(__file__).parent.parent),
            capture_output=True,
            text=True,
            timeout=3600  # 1 小時超時
        )
        
        if result.returncode != 0:
            logger.error(f"Analysis 執行失敗: {result.stderr}")
            return False
        
        logger.info(f"✓ Analysis 完成")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("Analysis 執行超時 (>1 小時)")
        return False
    except Exception as e:
        logger.error(f"Analysis 執行異常: {e}", exc_info=True)
        return False


def run_complete_backtest(config: BacktestConfig, logger: logging.Logger):
    """
    運行完整的回測流程
    
    Args:
        config: 回測配置
        logger: 日誌記錄器
    """
    logger.info("=" * 80)
    logger.info("【開始完整回測流程】")
    logger.info("=" * 80)
    
    start_time = datetime.now()
    
    try:
        # Step 0: 如果需要，運行 Analysis
        if not run_analysis_if_needed(config, logger):
            logger.error("Analysis 失敗，無法繼續回測")
            return False
        
        # Step 1: 加載機會
        logger.info("\n[Step 1/6] 加載交易機會...")
        loader = OpportunityLoader()
        opportunities = loader.load_tradable_opportunities(config)
        
        if not opportunities:
            logger.error("未找到任何交易機會")
            return False
        
        logger.info(f"✓ 已加載 {len(opportunities)} 個可交易機會")
        
        # Step 2: 非同步獲取 K 線
        logger.info("\n[Step 2/6] 非同步獲取 K 線資料...")
        fetcher = KlineFetcher()
        
        # 提取所有 symbols
        tradable_symbols = []
        for opp_obj in opportunities:
            opp = opp_obj.to_dict() if hasattr(opp_obj, 'to_dict') else opp_obj
            symbol = opp.get('symbol')
            if symbol and symbol not in tradable_symbols:
                tradable_symbols.append(symbol)
        
        if not tradable_symbols:
            logger.warning("未找到任何可交易的 symbols")
            tradable_symbols = []
        
        logger.info(f"準備獲取 {len(tradable_symbols)} 個 symbols 的 K 線...")
        
        # 非同步獲取 K 線
        if tradable_symbols:
            klines_dict = asyncio.run(fetcher.fetch_all_klines(tradable_symbols, config))
            logger.info(f"✓ 已獲取 K 線資料")
            
            # 詳細診斷
            if not klines_dict:
                logger.error("❌ klines_dict 為空！K-line 數據未被加載")
                logger.error(f"   tradable_symbols 數: {len(tradable_symbols)}")
                logger.error(f"   快取目錄: {fetcher.cache_dir}")
            else:
                logger.info(f"✓ klines_dict 包含 {len(klines_dict)} 個 symbols")
                # 查看每個 symbol 的數據
                for symbol in list(klines_dict.keys())[:5]:
                    exchanges = klines_dict[symbol]
                    logger.info(f"  {symbol}: {len(exchanges)} 個交易所")
                    for exchange, df in exchanges.items():
                        if df is not None:
                            logger.info(f"    {exchange}: {len(df)} 行")
                        else:
                            logger.warning(f"    {exchange}: None")
        else:
            logger.warning("無可交易的 symbols，跳過 K 線獲取")
            klines_dict = {}
        
        # Step 3: 計算 VWAP
        logger.info("\n[Step 3/6] 計算 VWAP...")
        if klines_dict:
            # 確保 opportunities 是字典列表
            opportunities_dicts = []
            for opp in opportunities:
                if hasattr(opp, 'to_dict'):
                    opportunities_dicts.append(opp.to_dict())
                else:
                    opportunities_dicts.append(opp)
            
            # 轉換 klines_dict 格式: {symbol: {exchange: df}} -> 針對每個 symbol/opportunity
            # VWAP 計算器期望 {exchange: df} 的格式
            def get_symbol_klines(symbol: str, klines_dict: dict) -> dict:
                """為特定 symbol 提取 K 線資料"""
                if symbol in klines_dict:
                    return klines_dict[symbol]  # 直接返回 {exchange: df}
                return {}
            
            # 檢查機會中的 symbols 是否在 klines_dict 中
            opportunity_symbols = set(opp.get('symbol') for opp in opportunities_dicts if 'symbol' in opp)
            missing_symbols = opportunity_symbols - set(klines_dict.keys())
            if missing_symbols:
                logger.warning(f"⚠️  {len(missing_symbols)} 個機會的 symbols 在 klines_dict 中找不到")
                logger.warning(f"   缺失 symbols: {missing_symbols}")
            
            updated_opps, stats = VWAPIntegrator.calculate_vwaps_for_all_opportunities(
                opportunities_dicts,
                klines_dict,
                config.vwap_window_minutes
            )
            logger.info(f"✓ VWAP 計算完成")
            logger.info(f"  成功: {stats['success']}/{stats['total']} ({stats['success_rate']:.1f}%)")
            opportunities = updated_opps
        else:
            logger.warning("無 K 線資料，跳過 VWAP 計算")
        
        # Step 4: 執行回測
        logger.info("\n[Step 4/6] 執行回測引擎...")
        engine = BacktestEngine(config.initial_capital, config)
        result = engine.run_backtest(opportunities, klines_dict)
        
        logger.info(f"✓ 回測執行完成")
        logger.info(f"  執行交易: {result['total_trades']} 筆")
        logger.info(f"  失敗交易: {result['failed_trades']} 筆")
        logger.info(f"  累積 P&L: ${result['final_pnl']:,.2f}")
        logger.info(f"  最終資產值: ${result['final_portfolio_value']:,.2f}")
        logger.info(f"  回報率: {result['return_pct']:.2f}%")
        
        # Step 5: 分析和生成報告
        logger.info("\n[Step 5/6] 分析績效和生成報告...")
        metrics = BacktestAnalyzer.calculate_metrics(
            result.get('trades', []),
            result.get('equity_curve', [])
        )
        
        logger.info(f"✓ 績效分析完成")
        logger.info(f"  總交易數: {metrics['total_trades']}")
        logger.info(f"  勝率: {metrics['win_rate']:.2f}%")
        logger.info(f"  Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
        logger.info(f"  最大回撤: {metrics['max_drawdown']:.2f}%")
        
        # 生成報告
        output_dir = config.output_dir
        visualizer = BacktestVisualizer(output_dir)
        
        report_files = visualizer.generate_all_reports(metrics, result['trades'], result['equity_curve'])
        logger.info(f"\n✓ 報告已生成到: {output_dir}")
        logger.info(f"  - 績效報告: {report_files.get('report_txt')}")
        logger.info(f"  - 交易記錄: {report_files.get('trades_csv')}")
        logger.info(f"  - 權益曲線: {report_files.get('equity_csv')}")
        logger.info(f"  - 綜合分析圖: {report_files.get('pnl_chart')} (P&L + Drawdown + Stats)")
        
        # 總結
        elapsed = datetime.now() - start_time
        logger.info("\n" + "=" * 80)
        logger.info("【回測完成！】")
        logger.info("=" * 80)
        logger.info(f"執行時間: {elapsed.total_seconds():.2f} 秒")
        logger.info(f"累積 P&L: ${result['final_pnl']:,.2f}")
        logger.info(f"勝率: {metrics['win_rate']:.2f}%")
        logger.info(f"Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"回測執行失敗: {e}", exc_info=True)
        return False


def main():
    """主函數"""
    import argparse
    
    parser = argparse.ArgumentParser(description="完整回測執行腳本")
    parser.add_argument('--config', type=str, default=None, help='設定檔案路徑')
    parser.add_argument('--debug', action='store_true', help='調試模式')
    
    args = parser.parse_args()
    
    # 初始化日誌
    logger = setup_logging(debug=args.debug)
    
    try:
        # 加載配置
        if args.config:
            config = BacktestConfig.load_from_json(args.config)
        else:
            config = BacktestConfig()
        
        logger.info(f"使用配置:")
        logger.info(f"  時間範圍: {config.start_date} ~ {config.end_date}")
        logger.info(f"  初始資金: ${config.initial_capital:,.0f}")
        logger.info(f"  VWAP 窗口: {config.vwap_window_minutes} 分鐘")
        
        # 運行回測
        success = run_complete_backtest(config, logger)
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.info("\n回測被用戶中斷")
        return 130
    except Exception as e:
        logger.error(f"致命錯誤: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

