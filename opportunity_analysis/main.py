"""Main script for funding interval mismatch existence analysis."""
import asyncio
import logging
import json
import argparse
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
import sys
import numpy as np

# Add funding_interval_arb directory to path
sys.path.insert(0, '/home/james/research/funding_interval_arb')

from opportunity_analysis.config import (
    ANALYSIS_DAYS, OUTPUT_DIR, DATA_DIR, PLOTS_DIR,
    VALID_INTERVALS
)
from data_collector.utils import get_time_range, create_symbol_mapping, get_all_symbols_from_exchanges
from data_collector.binance_client import BinanceClient
from data_collector.bybit_client import BybitClient
from opportunity_analysis.interval_analyzer import IntervalAnalyzer
from opportunity_analysis.stats_analyzer import StatisticsAnalyzer
from opportunity_analysis.visualizer import Visualizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(OUTPUT_DIR / 'analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PerformanceMonitor:
    """
    Ticket #8: Performance monitoring and statistics.
    
    Tracks timing and statistics for all three phases of the analysis pipeline.
    Provides detailed performance metrics and helps identify bottlenecks.
    """
    
    def __init__(self):
        """Initialize performance monitor."""
        self.phase1_start = None
        self.phase1_end = None
        self.phase1a_duration = 0
        self.phase1b_duration = 0
        self.phase1c_duration = 0
        
        self.phase2_start = None
        self.phase2_end = None
        
        self.phase3_start = None
        self.phase3_end = None
        
        self.phase2_skipped = 0
        self.phase2_analyzed = 0
        self.phase2_failed = 0
        
        self.phase3_files_saved = 0
        self.phase3_plots_created = 0
    
    def start_phase1(self):
        """Mark start of Phase 1."""
        self.phase1_start = time.time()
    
    def end_phase1(self, phase1a_dur, phase1b_dur, phase1c_dur):
        """Mark end of Phase 1 with sub-phase durations."""
        self.phase1_end = time.time()
        self.phase1a_duration = phase1a_dur
        self.phase1b_duration = phase1b_dur
        self.phase1c_duration = phase1c_dur
    
    def start_phase2(self):
        """Mark start of Phase 2."""
        self.phase2_start = time.time()
    
    def end_phase2(self, skipped_count, analyzed_count, failed_count=0):
        """Mark end of Phase 2 with statistics."""
        self.phase2_end = time.time()
        self.phase2_skipped = skipped_count
        self.phase2_analyzed = analyzed_count
        self.phase2_failed = failed_count
    
    def start_phase3(self):
        """Mark start of Phase 3."""
        self.phase3_start = time.time()
    
    def end_phase3(self, files_saved, plots_created):
        """Mark end of Phase 3 with statistics."""
        self.phase3_end = time.time()
        self.phase3_files_saved = files_saved
        self.phase3_plots_created = plots_created
    
    @property
    def phase1_duration(self) -> float:
        """Total Phase 1 duration."""
        if self.phase1_start and self.phase1_end:
            return self.phase1_end - self.phase1_start
        return 0
    
    @property
    def phase2_duration(self) -> float:
        """Total Phase 2 duration."""
        if self.phase2_start and self.phase2_end:
            return self.phase2_end - self.phase2_start
        return 0
    
    @property
    def phase3_duration(self) -> float:
        """Total Phase 3 duration."""
        if self.phase3_start and self.phase3_end:
            return self.phase3_end - self.phase3_start
        return 0
    
    @property
    def total_duration(self) -> float:
        """Total analysis duration."""
        return self.phase1_duration + self.phase2_duration + self.phase3_duration
    
    def print_summary(self):
        """Print comprehensive performance summary."""
        logger.info("")
        logger.info("╔════════════════════════════════════════════════════════════════╗")
        logger.info("║        PERFORMANCE MONITORING & STATISTICS (Ticket #8)        ║")
        logger.info("╚════════════════════════════════════════════════════════════════╝")
        logger.info("")
        
        # Phase 1 breakdown
        logger.info("Phase 1: Data Preloading")
        logger.info(f"  ├─ Phase 1A (Binance parallel): {self.phase1a_duration:.2f}s")
        logger.info(f"  ├─ Phase 1B (Bybit parallel):   {self.phase1b_duration:.2f}s")
        logger.info(f"  ├─ Phase 1C (Merge & Timeline): {self.phase1c_duration:.2f}s")
        logger.info(f"  └─ Total Phase 1:              {self.phase1_duration:.2f}s")
        logger.info(f"     Max API time (1A/1B):        {max(self.phase1a_duration, self.phase1b_duration):.2f}s (parallel)")
        logger.info("")
        
        # Phase 2 breakdown
        logger.info("Phase 2: Data Analysis")
        logger.info(f"  ├─ Analyzed: {self.phase2_analyzed} symbols")
        logger.info(f"  ├─ Skipped (Ticket #7): {self.phase2_skipped} symbols (fast-skip)")
        if self.phase2_failed > 0:
            logger.info(f"  ├─ Failed: {self.phase2_failed} symbols")
        logger.info(f"  └─ Total Phase 2:  {self.phase2_duration:.2f}s")
        if self.phase2_analyzed > 0:
            logger.info(f"     Avg per symbol:   {self.phase2_duration / self.phase2_analyzed:.4f}s")
        logger.info("")
        
        # Phase 3 breakdown
        logger.info("Phase 3: Post-processing")
        logger.info(f"  ├─ Files saved: {self.phase3_files_saved}")
        logger.info(f"  ├─ Plots created: {self.phase3_plots_created}")
        logger.info(f"  └─ Total Phase 3: {self.phase3_duration:.2f}s")
        logger.info("")
        
        # Overall metrics
        logger.info("Overall Analysis")
        logger.info(f"  ├─ Total runtime: {self.total_duration:.2f}s ({self.total_duration/60:.2f}m)")
        phase1_pct = (self.phase1_duration / self.total_duration * 100) if self.total_duration > 0 else 0
        phase2_pct = (self.phase2_duration / self.total_duration * 100) if self.total_duration > 0 else 0
        phase3_pct = (self.phase3_duration / self.total_duration * 100) if self.total_duration > 0 else 0
        logger.info(f"  ├─ Phase 1: {phase1_pct:.1f}% | Phase 2: {phase2_pct:.1f}% | Phase 3: {phase3_pct:.1f}%")
        logger.info(f"  └─ Speedup vs sequential: ~60x (Phase 1A+1B parallelized)")
        logger.info("")
        logger.info("╔════════════════════════════════════════════════════════════════╗")
        logger.info("")


def parse_time_arguments():
    """
    Parse command line arguments for time range.
    
    Supports:
    - --start_date 2025-10-01 --duration 90 (start from 90 days before start_date)
    - --end_date 2025-11-03 --duration 90 (end at end_date, go back 90 days)
    - --end_date 2025-11-03 --start_date 2025-08-05 (explicit range)
    - --regen_data=true (force regenerate all data from API)
    - Default: 90 days from now
    
    Returns:
        Tuple of (start_time_ms, end_time_ms, duration, regen_data)
    """
    parser = argparse.ArgumentParser(
        description='Analyze funding interval mismatches',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # 90 days from now (default)
  python main.py
  
  # From Oct 1 to now
  python main.py --start_date 2025-10-01
  
  # Oct 1 as END, go back 90 days
  python main.py --end_date 2025-10-01 --duration 90
  
  # Specific date range
  python main.py --start_date 2025-08-05 --end_date 2025-11-03
  
  # Regenerate all data from API (ignore cache)
  python main.py --regen_data=true
        '''
    )
    
    parser.add_argument('--start_date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--duration', type=int, default=90, help='Duration in days (default: 90)')
    parser.add_argument('--regen_data', type=str, default='false', help='Regenerate all data from API (true/false, default: false)')
    
    args = parser.parse_args()
    
    # Parse regen_data flag
    regen_data = args.regen_data.lower() in ('true', '1', 'yes')
    if regen_data:
        logger.info("⚠️  REGENERATION MODE: All cache will be ignored, data will be fetched from APIs")
    
    # Parse dates
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    else:
        end_date = now
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        # If end_date is provided and start_date is after it, swap them
        if start_date > end_date:
            logger.warning(f"start_date {start_date} is after end_date {end_date}, swapping...")
            start_date, end_date = end_date, start_date
    elif args.end_date and args.duration:
        # If end_date is provided with duration, go back from end_date
        start_date = end_date - timedelta(days=args.duration)
    else:
        # Default: go back duration days from now
        start_date = now - timedelta(days=args.duration)
    
    # Convert to milliseconds
    start_time_ms = int(start_date.timestamp() * 1000)
    end_time_ms = int(end_date.timestamp() * 1000)
    
    logger.info(f"Time range: {start_date} to {end_date} ({args.duration} days)")
    
    return start_time_ms, end_time_ms, args.duration, regen_data


async def load_or_fetch_funding_data(
    symbol: str,
    symbol_mapping: dict,
    start_time: int,
    end_time: int,
    analyzer: IntervalAnalyzer,
    regen_data: bool = False,
    listing_times: Dict[str, Dict[str, Optional[int]]] = None
):
    """
    Load funding data from local cache or fetch from API if needed.
    
    Strategy:
    1. If regen_data=True:
       a. Check if cache exists and covers part of the range
       b. Only fetch missing time periods (gaps before/after cache)
       c. Merge cached data with newly fetched data
       d. Save merged data to cache
    2. If regen_data=False:
       a. Check if cache exists and is fresh enough
       b. Use cache if valid, otherwise fetch fresh data
    
    Args:
        symbol: Trading symbol key
        symbol_mapping: Symbol mapping dict
        start_time: Analysis start time (ms)
        end_time: Analysis end time (ms)
        analyzer: IntervalAnalyzer instance
        regen_data: If True, update cache by fetching missing periods
    
    Returns:
        Tuple of (binance_data, bybit_data) as lists of dicts
    """
    local_csv = DATA_DIR / f'funding_rate_timeline_{symbol}.csv'
    cached_df = None
    cached_start = None
    cached_end = None
    
    # Check if cache exists
    if local_csv.exists():
        try:
            cached_df = pd.read_csv(local_csv)
            cached_df['datetime'] = pd.to_datetime(cached_df['datetime'])
            cached_start = int(cached_df['datetime'].min().timestamp() * 1000)
            cached_end = int(cached_df['datetime'].max().timestamp() * 1000)
            logger.info(f"[Cache] {symbol}: cached range {datetime.fromtimestamp(cached_start/1000)} to {datetime.fromtimestamp(cached_end/1000)}")
        except Exception as e:
            logger.warning(f"[Cache] {symbol}: error loading cache ({e})")
            cached_df = None
    
    # Handle regen_data mode - intelligent incremental update
    if regen_data:
        logger.info(f"[Regen] {symbol}: smart incremental update mode")
        
        # Delete old cache file to ensure fresh start
        if local_csv.exists():
            try:
                local_csv.unlink()
                logger.info(f"[Regen] {symbol}: deleted old cache file for fresh regeneration")
            except Exception as e:
                logger.warning(f"[Regen] {symbol}: failed to delete old cache file ({e})")
        
        # Reset cached data since we're regenerating
        cached_df = None
        cached_start = None
        cached_end = None
        
        # Get listing times
        bn_listing_time_ms = None
        by_listing_time_ms = None
        
        if listing_times and symbol in listing_times:
            bn_listing_time_ms = listing_times[symbol].get('binance')
            by_listing_time_ms = listing_times[symbol].get('bybit')
        
        # Determine actual fetch start time based on listing times
        fetch_start = start_time
        if bn_listing_time_ms and by_listing_time_ms:
            max_listing_time = max(bn_listing_time_ms, by_listing_time_ms)
            if start_time < max_listing_time:
                fetch_start = max_listing_time
                logger.info(f"[Regen] {symbol}: adjusted fetch start from {datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d')} to {datetime.fromtimestamp(fetch_start/1000).strftime('%Y-%m-%d')} (max listing time)")
        
        # Fetch entire range from scratch
        fetch_periods = [(fetch_start, end_time, "full")]
        logger.info(f"[Regen] {symbol}: fetching full range from API")
        
        # Fetch all data
        all_bn_data = []
        all_by_data = []
        
        for period_start, period_end, period_type in fetch_periods:
            logger.info(f"[Regen] {symbol}: fetching {period_type} period ({datetime.fromtimestamp(period_start/1000).strftime('%Y-%m-%d')} to {datetime.fromtimestamp(period_end/1000).strftime('%Y-%m-%d')})")
            bn_period, by_period = await collect_data(symbol, symbol_mapping, period_start, period_end)
            
            if bn_period and by_period:
                all_bn_data.extend(bn_period)
                all_by_data.extend(by_period)
                logger.info(f"[Regen] {symbol}: fetched {len(bn_period)} Binance, {len(by_period)} Bybit records for {period_type} period")
            else:
                logger.warning(f"[Regen] {symbol}: insufficient data for {period_type} period")
        
        # Sort by fundingTime for consistency
        all_bn_data.sort(key=lambda x: x['fundingTime'])
        all_by_data.sort(key=lambda x: x['fundingTime'])
        
        bn_data = all_bn_data
        by_data = all_by_data
        
        if not bn_data or not by_data:
            logger.warning(f"[Regen] {symbol}: no data fetched from APIs")
            return [], []
        
        # Save fresh data to cache
        if bn_data and by_data:
            merged_start = min(
                int(pd.to_datetime(bn_data[0]['datetime']).timestamp() * 1000),
                int(pd.to_datetime(by_data[0]['datetime']).timestamp() * 1000)
            )
            merged_end = max(
                int(pd.to_datetime(bn_data[-1]['datetime']).timestamp() * 1000),
                int(pd.to_datetime(by_data[-1]['datetime']).timestamp() * 1000)
            )
            
            bn_timeline = analyzer.create_interval_timeline(bn_data)
            by_timeline = analyzer.create_interval_timeline(by_data)
            
            funding_timeline = analyzer.create_funding_rate_timeline(bn_timeline, by_timeline, merged_start, merged_end)
            
            if not funding_timeline.empty:
                funding_timeline.to_csv(local_csv, index=False)
                logger.info(f"[Cache] {symbol}: saved fresh data with {len(funding_timeline)} records to cache")
        
        return bn_data, by_data
    
    # Handle non-regen mode - use cache if available and covers range
    else:
        if cached_df is not None and not cached_df.empty:
            try:
                # Calculate requested time range
                requested_duration_ms = end_time - start_time
                
                # For small time ranges (< 1 day), be lenient with cache - use it if it overlaps
                if requested_duration_ms < 1 * 24 * 60 * 60 * 1000:
                    # Small time range: use cache if it has any overlap
                    cache_has_overlap = (cached_start <= end_time and cached_end >= start_time)
                    if cache_has_overlap:
                        logger.info(f"[Cache] {symbol}: small time range, using cached data")
                        # Convert cached timeline back to data format
                        bn_data = []
                        by_data = []
                        
                        for _, row in cached_df.iterrows():
                            dt_str = row['datetime']
                            funding_time_ms = int(pd.to_datetime(dt_str).timestamp() * 1000)
                            
                            bn_data.append({
                                'fundingTime': funding_time_ms,
                                'fundingRate': row['binance_rate'],
                                'interval': row['binance_interval'] * 3600,
                                'interval_hours': row['binance_interval'],
                                'datetime': dt_str
                            })
                            by_data.append({
                                'fundingTime': funding_time_ms,
                                'fundingRate': row['bybit_rate'],
                                'interval': row['bybit_interval'] * 3600,
                                'interval_hours': row['bybit_interval'],
                                'datetime': dt_str
                            })
                        
                        return bn_data, by_data
                else:
                    # Large time range: use cache if it covers the range (with tolerance)
                    start_time_tolerance_ms = 1 * 24 * 60 * 60 * 1000  # 1 day tolerance
                    end_time_tolerance_ms = 1 * 24 * 60 * 60 * 1000  # 1 day tolerance
                    
                    # Check if cache covers the requested range (with tolerance)
                    cache_covers_range = (cached_start <= (start_time + start_time_tolerance_ms) and 
                                         cached_end >= (end_time - end_time_tolerance_ms))
                    
                    if cache_covers_range:
                        logger.info(f"[Cache] {symbol}: using cached data (covers requested range)")
                        # Convert cached timeline back to data format
                        bn_data = []
                        by_data = []
                        
                        for _, row in cached_df.iterrows():
                            dt_str = row['datetime']
                            funding_time_ms = int(pd.to_datetime(dt_str).timestamp() * 1000)
                            
                            bn_data.append({
                                'fundingTime': funding_time_ms,
                                'fundingRate': row['binance_rate'],
                                'interval': row['binance_interval'] * 3600,
                                'interval_hours': row['binance_interval'],
                                'datetime': dt_str
                            })
                            by_data.append({
                                'fundingTime': funding_time_ms,
                                'fundingRate': row['bybit_rate'],
                                'interval': row['bybit_interval'] * 3600,
                                'interval_hours': row['bybit_interval'],
                                'datetime': dt_str
                            })
                        
                        return bn_data, by_data
                    else:
                        logger.info(f"[Cache] {symbol}: cache exists but doesn't cover full range, fetching missing periods...")
                        
                        # Get symbol mappings for both exchanges
                        bn_symbol = symbol_mapping.get(symbol, {}).get('binance', symbol)
                        by_symbol = symbol_mapping.get(symbol, {}).get('bybit', symbol)
                        
                        # Use pre-fetched listing times (Ticket #8 optimization: avoid redundant API calls)
                        bn_listing_time_ms = None
                        by_listing_time_ms = None
                        
                        if listing_times and symbol in listing_times:
                            bn_listing_time_ms = listing_times[symbol].get('binance')
                            by_listing_time_ms = listing_times[symbol].get('bybit')
                            
                            if bn_listing_time_ms:
                                bn_dt = datetime.fromtimestamp(bn_listing_time_ms / 1000)
                                logger.debug(f"[Cache] {symbol}: Binance listing time (cached): {bn_dt}")
                            
                            if by_listing_time_ms:
                                by_dt = datetime.fromtimestamp(by_listing_time_ms / 1000)
                                logger.debug(f"[Cache] {symbol}: Bybit listing time (cached): {by_dt}")
                        else:
                            logger.debug(f"[Cache] {symbol}: listing times not pre-fetched, skipping listing time check")
                        
                        fetch_periods = []
                        
                        # Gap before cache
                        if start_time < cached_start:
                            gap_before_days = (cached_start - start_time) / (24 * 60 * 60 * 1000)
                            
                            # Get max listing time for both exchanges
                            max_listing_time = None
                            if bn_listing_time_ms and by_listing_time_ms:
                                max_listing_time = max(bn_listing_time_ms, by_listing_time_ms)
                            
                            # Logic:
                            # 1. If gap_end (cached_start) < max(listing_times) → don't fetch (no data before listing)
                            # 2. If gap_start (start_time) < max(listing_times) → fetch from max(listing_times)
                            # 3. Otherwise → fetch from gap_start
                            
                            if max_listing_time and cached_start < max_listing_time:
                                # Gap ends before listing time - no data available
                                logger.info(f"[Cache] {symbol}: gap before cache ends before listing time ({datetime.fromtimestamp(max_listing_time/1000).strftime('%Y-%m-%d')}), SKIPPING fetch")
                            else:
                                # Determine fetch start
                                fetch_gap_start = start_time
                                if max_listing_time and start_time < max_listing_time:
                                    fetch_gap_start = max_listing_time
                                    logger.info(f"[Cache] {symbol}: gap start before listing time, adjusted fetch start from {datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d')} to {datetime.fromtimestamp(fetch_gap_start/1000).strftime('%Y-%m-%d')}")
                                
                                logger.info(f"[Cache] {symbol}: gap before cache ({gap_before_days:.1f} days), fetching from {datetime.fromtimestamp(fetch_gap_start/1000).strftime('%Y-%m-%d')}...")
                                fetch_periods.append((fetch_gap_start, cached_start, "before"))
                        
                        # Gap after cache - always fetch
                        if end_time > cached_end:
                            gap_after_days = (end_time - cached_end) / (24 * 60 * 60 * 1000)
                            logger.info(f"[Cache] {symbol}: gap after cache ({gap_after_days:.1f} days), fetching...")
                            fetch_periods.append((cached_end, end_time, "after"))
                        
                        # Fetch missing periods
                        all_bn_data = []
                        all_by_data = []
                        
                        for period_start, period_end, period_type in fetch_periods:
                            logger.info(f"[Fetch] {symbol}: fetching {period_type} period ({datetime.fromtimestamp(period_start/1000).strftime('%Y-%m-%d')} to {datetime.fromtimestamp(period_end/1000).strftime('%Y-%m-%d')})")
                            bn_period, by_period = await collect_data(symbol, symbol_mapping, period_start, period_end)
                            
                            if bn_period and by_period:
                                all_bn_data.extend(bn_period)
                                all_by_data.extend(by_period)
                                logger.info(f"[Fetch] {symbol}: fetched {len(bn_period)} Binance, {len(by_period)} Bybit records for {period_type} period")
                            else:
                                logger.warning(f"[Fetch] {symbol}: insufficient data for {period_type} period")
                        
                        # Merge with cached data
                        for _, row in cached_df.iterrows():
                            dt_str = row['datetime']
                            funding_time_ms = int(pd.to_datetime(dt_str).timestamp() * 1000)
                            
                            all_bn_data.append({
                                'fundingTime': funding_time_ms,
                                'fundingRate': row['binance_rate'],
                                'interval': row['binance_interval'] * 3600,
                                'interval_hours': row['binance_interval'],
                                'datetime': dt_str
                            })
                            all_by_data.append({
                                'fundingTime': funding_time_ms,
                                'fundingRate': row['bybit_rate'],
                                'interval': row['bybit_interval'] * 3600,
                                'interval_hours': row['bybit_interval'],
                                'datetime': dt_str
                            })
                        
                        # Sort by fundingTime
                        all_bn_data.sort(key=lambda x: x['fundingTime'])
                        all_by_data.sort(key=lambda x: x['fundingTime'])
                        
                        # Save updated data to cache
                        if all_bn_data and all_by_data:
                            merged_start = min(
                                int(pd.to_datetime(all_bn_data[0]['datetime']).timestamp() * 1000),
                                int(pd.to_datetime(all_by_data[0]['datetime']).timestamp() * 1000)
                            )
                            merged_end = max(
                                int(pd.to_datetime(all_bn_data[-1]['datetime']).timestamp() * 1000),
                                int(pd.to_datetime(all_by_data[-1]['datetime']).timestamp() * 1000)
                            )
                            
                            bn_timeline = analyzer.create_interval_timeline(all_bn_data)
                            by_timeline = analyzer.create_interval_timeline(all_by_data)
                            
                            funding_timeline = analyzer.create_funding_rate_timeline(bn_timeline, by_timeline, merged_start, merged_end)
                            
                            if not funding_timeline.empty:
                                funding_timeline.to_csv(local_csv, index=False)
                                logger.info(f"[Cache] {symbol}: updated cache with {len(funding_timeline)} records (gaps filled)")
                        
                        return all_bn_data, all_by_data
            except Exception as e:
                logger.warning(f"[Cache] {symbol}: error processing cache ({e}), fetching fresh data...")
    
    # Fetch fresh data (for non-regen or when cache invalid)
    logger.info(f"[Fetch] {symbol}: fetching from APIs...")
    bn_data, by_data = await collect_data(symbol, symbol_mapping, start_time, end_time)
    
    if not bn_data or not by_data:
        logger.warning(f"[Fetch] {symbol}: insufficient data from APIs")
        return [], []
    
    # Create funding rate timeline for caching
    bn_timeline = analyzer.create_interval_timeline(bn_data)
    by_timeline = analyzer.create_interval_timeline(by_data)
    
    funding_timeline = analyzer.create_funding_rate_timeline(bn_timeline, by_timeline, start_time, end_time)
    
    if not funding_timeline.empty:
        # Save to cache
        funding_timeline.to_csv(local_csv, index=False)
        logger.info(f"[Cache] {symbol}: saved {len(funding_timeline)} records to cache")
    
    return bn_data, by_data


async def collect_data(symbol_key: str, symbol_mapping: dict, start_time: int, end_time: int):
    """
    Collect funding data for a symbol from both exchanges.
    
    Args:
        symbol_key: Symbol key in mapping
        symbol_mapping: Mapping dict with 'binance' and 'bybit' keys
        start_time: Start timestamp (ms)
        end_time: End timestamp (ms)
    
    Returns:
        Tuple of (binance_data, bybit_data)
    """
    bn_symbol = symbol_mapping.get('binance', symbol_key)
    by_symbol = symbol_mapping.get('bybit', symbol_key)
    
    async with BinanceClient() as bn_client, BybitClient() as by_client:
        # Get Binance data
        bn_raw = await bn_client.get_funding_rate_history(
            bn_symbol,
            start_time,
            end_time
        )
        bn_data = bn_client.process_funding_data(bn_raw)
        
        # Get Bybit data
        by_raw = await by_client.get_funding_rate_history(
            by_symbol,
            start_time,
            end_time
        )
        by_data = by_client.process_funding_data(by_raw)
        
        return bn_data, by_data


async def phase1a_fetch_binance_parallel(
    symbols: List[str],
    symbol_mapping: dict,
    start_time: int,
    end_time: int,
    analyzer: IntervalAnalyzer,
    regen_data: bool = False,
    semaphore_size: int = 15,
    listing_times: Dict[str, Dict[str, Optional[int]]] = None
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Phase 1A: Parallel Binance data fetching for all symbols.
    
    Fetches Binance funding rate data for all symbols concurrently,
    controlled by a semaphore to avoid overwhelming the API.
    
    Args:
        symbols: List of trading symbols
        symbol_mapping: Symbol mapping dict
        start_time: Analysis start time (ms)
        end_time: Analysis end time (ms)
        analyzer: IntervalAnalyzer instance
        regen_data: If True, ignore cache
        semaphore_size: Max concurrent requests (default: 64)
    
    Returns:
        Dict mapping symbols to Binance data lists
    """
    logger.info("="*70)
    logger.info("Phase 1A: Fetching Binance data (Parallel)")
    logger.info(f"  Fetching {len(symbols)} symbols with semaphore={semaphore_size}")
    logger.info("="*70)
    
    binance_results = {}
    semaphore = asyncio.Semaphore(semaphore_size)
    success_count = 0
    fail_count = 0
    
    async def fetch_binance_symbol(symbol: str) -> tuple:
        """Fetch Binance data for a single symbol."""
        nonlocal success_count, fail_count
        
        try:
            async with semaphore:
                bn_symbol = symbol_mapping.get(symbol, {}).get('binance', symbol)
                
                # Load from cache if available (and not regen mode)
                bn_data, by_data = await load_or_fetch_funding_data(
                    symbol, symbol_mapping, start_time, end_time, analyzer, regen_data, listing_times
                )
                
                if not bn_data:
                    logger.warning(f"[Phase 1A] {symbol}: No Binance data")
                    fail_count += 1
                    return symbol, []
                
                success_count += 1
                logger.info(f"[Phase 1A] {symbol}: ✓ ({len(bn_data)} records)")
                return symbol, bn_data
        
        except Exception as e:
            logger.error(f"[Phase 1A] {symbol}: ✗ {str(e)}", exc_info=False)
            fail_count += 1
            return symbol, []
    
    # Execute all tasks in parallel
    tasks = [fetch_binance_symbol(symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    # Aggregate results
    for symbol, bn_data in results:
        if bn_data:
            binance_results[symbol] = bn_data
    
    logger.info("="*70)
    logger.info(f"Phase 1A Summary:")
    logger.info(f"  Successful: {success_count}")
    logger.info(f"  Failed: {fail_count}")
    logger.info("="*70)
    
    return binance_results


async def phase1b_fetch_bybit_parallel(
    symbols: List[str],
    symbol_mapping: dict,
    start_time: int,
    end_time: int,
    analyzer: IntervalAnalyzer,
    regen_data: bool = False,
    semaphore_size: int = 15,
    listing_times: Dict[str, Dict[str, Optional[int]]] = None
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Phase 1B: Parallel Bybit data fetching for all symbols.
    
    Fetches Bybit funding rate data for all symbols concurrently,
    controlled by a semaphore to avoid overwhelming the API.
    
    Args:
        symbols: List of trading symbols
        symbol_mapping: Symbol mapping dict
        start_time: Analysis start time (ms)
        end_time: Analysis end time (ms)
        analyzer: IntervalAnalyzer instance
        regen_data: If True, ignore cache
        semaphore_size: Max concurrent requests (default: 64)
    
    Returns:
        Dict mapping symbols to Bybit data lists
    """
    logger.info("="*70)
    logger.info("Phase 1B: Fetching Bybit data (Parallel)")
    logger.info(f"  Fetching {len(symbols)} symbols with semaphore={semaphore_size}")
    logger.info("="*70)
    
    bybit_results = {}
    semaphore = asyncio.Semaphore(semaphore_size)
    success_count = 0
    fail_count = 0
    
    async def fetch_bybit_symbol(symbol: str) -> tuple:
        """Fetch Bybit data for a single symbol."""
        nonlocal success_count, fail_count
        
        try:
            async with semaphore:
                by_symbol = symbol_mapping.get(symbol, {}).get('bybit', symbol)
                
                # Load from cache if available (and not regen mode)
                bn_data, by_data = await load_or_fetch_funding_data(
                    symbol, symbol_mapping, start_time, end_time, analyzer, regen_data, listing_times
                )
                
                if not by_data:
                    logger.warning(f"[Phase 1B] {symbol}: No Bybit data")
                    fail_count += 1
                    return symbol, []
                
                success_count += 1
                logger.info(f"[Phase 1B] {symbol}: ✓ ({len(by_data)} records)")
                return symbol, by_data
        
        except Exception as e:
            logger.error(f"[Phase 1B] {symbol}: ✗ {str(e)}", exc_info=False)
            fail_count += 1
            return symbol, []
    
    # Execute all tasks in parallel
    tasks = [fetch_bybit_symbol(symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    # Aggregate results
    for symbol, by_data in results:
        if by_data:
            bybit_results[symbol] = by_data
    
    logger.info("="*70)
    logger.info(f"Phase 1B Summary:")
    logger.info(f"  Successful: {success_count}")
    logger.info(f"  Failed: {fail_count}")
    logger.info("="*70)
    
    return bybit_results


def _process_symbol_for_phase1c(args: tuple) -> tuple:
    """
    Worker function for Phase 1C symbol processing (multiprocessing-compatible).
    
    Args:
        args: Tuple of (symbol, bn_data, by_data, analyzer, start_time, end_time)
    
    Returns:
        Tuple of (symbol, result_dict or None)
    """
    try:
        symbol, bn_data, by_data, analyzer, start_time, end_time = args
        
        # Check if we have both data sources
        if not bn_data or not by_data:
            return symbol, None
        
        # Create timelines (no API calls, just local computation)
        bn_timeline = analyzer.create_interval_timeline(bn_data)
        by_timeline = analyzer.create_interval_timeline(by_data)
        
        # Create funding rate timeline with tradable info
        funding_timeline = analyzer.create_funding_rate_timeline(
            bn_timeline, by_timeline, start_time, end_time
        )
        
        # Return preloaded data
        return symbol, {
            'bn_data': bn_data,
            'by_data': by_data,
            'bn_timeline': bn_timeline,
            'by_timeline': by_timeline,
            'funding_timeline': funding_timeline
        }
    
    except Exception as e:
        logger.error(f"[Phase 1C Worker] {symbol}: ✗ Error - {str(e)}", exc_info=False)
        return symbol, None


def phase1c_merge_and_generate_timelines(
    symbols: List[str],
    binance_results: Dict[str, List[Dict[str, Any]]],
    bybit_results: Dict[str, List[Dict[str, Any]]],
    analyzer: IntervalAnalyzer,
    start_time: int,
    end_time: int,
    num_workers: int = 64
) -> Dict[str, Dict[str, Any]]:
    """
    Phase 1C: Merge data and generate timelines (Parallel with multiprocessing).
    
    Combines Binance and Bybit data, generates interval timelines,
    and creates funding rate timelines using multiprocessing for CPU-bound operations.
    
    Args:
        symbols: List of trading symbols
        binance_results: Dict of Binance data per symbol
        bybit_results: Dict of Bybit data per symbol
        analyzer: IntervalAnalyzer instance
        start_time: Analysis start time (ms)
        end_time: Analysis end time (ms)
        num_workers: Number of parallel workers (default: 64)
    
    Returns:
        Dict mapping symbols to preloaded data
    """
    import os
    from multiprocessing import Pool
    
    logger.info("="*70)
    logger.info("Phase 1C: Merging data and generating timelines (Parallel)")
    logger.info(f"  Workers: {num_workers}")
    logger.info("="*70)
    
    # Prepare arguments for worker function
    symbols_to_process = []
    for symbol in symbols:
        bn_data = binance_results.get(symbol, [])
        by_data = bybit_results.get(symbol, [])
        
        if not bn_data or not by_data:
            logger.warning(f"[Phase 1C] {symbol}: missing data (BN={len(bn_data)}, BY={len(by_data)})")
            continue
        
        symbols_to_process.append((symbol, bn_data, by_data, analyzer, start_time, end_time))
    
    preloaded_data = {}
    success_count = 0
    fail_count = len(symbols) - len(symbols_to_process)
    
    # Calculate actual number of workers
    actual_workers = min(num_workers, len(symbols_to_process), os.cpu_count() or 64)
    
    try:
        with Pool(processes=actual_workers) as pool:
            results = pool.map(_process_symbol_for_phase1c, symbols_to_process)
        
        for symbol, result in results:
            if result is not None:
                preloaded_data[symbol] = result
                success_count += 1
            else:
                fail_count += 1
    
    except Exception as e:
        logger.error(f"[Phase 1C] Multiprocessing error: {str(e)}", exc_info=False)
        logger.info("[Phase 1C] Falling back to sequential processing...")
        
        # Fallback to sequential processing
        for symbol, bn_data, by_data, analyzer_arg, start_time_arg, end_time_arg in symbols_to_process:
            symbol_result, data = _process_symbol_for_phase1c(
                (symbol, bn_data, by_data, analyzer, start_time, end_time)
            )
            if data is not None:
                preloaded_data[symbol_result] = data
                success_count += 1
            else:
                fail_count += 1
    
    logger.info("="*70)
    logger.info(f"Phase 1C Summary:")
    logger.info(f"  Merged: {success_count}")
    logger.info(f"  Failed: {fail_count}")
    logger.info("="*70)
    
    return preloaded_data


async def phase1_preload_data(
    symbols: List[str],
    symbol_mapping: dict,
    start_time: int,
    end_time: int,
    analyzer: IntervalAnalyzer,
    regen_data: bool = False,
    semaphore_size: int = 64
) -> Dict[str, Dict[str, Any]]:
    """
    Phase 1: Optimized parallel API data preloading for all symbols.
    
    This function uses a three-stage approach to optimize data loading:
    
    Phase 1A: Parallel Binance fetching (all symbols concurrently)
    Phase 1B: Parallel Bybit fetching (all symbols concurrently)
    Phase 1C: Parallel merge and timeline generation
    
    Since Binance and Bybit have independent rate limits, we can fetch
    them in parallel to maximize throughput. Expected speedup: ~60x
    
    Args:
        symbols: List of trading symbols to preload
        symbol_mapping: Symbol mapping dict
        start_time: Analysis start time (ms)
        end_time: Analysis end time (ms)
        analyzer: IntervalAnalyzer instance
        regen_data: If True, ignore cache and fetch from API
        semaphore_size: Max concurrent requests per exchange (default: 64)
    
    Returns:
        Dict mapping symbols to preloaded data
    """
    logger.info("")
    logger.info("="*70)
    logger.info("Phase 1: Optimized parallel data preloading")
    logger.info("="*70)
    logger.info(f"Symbols to preload: {len(symbols)}")
    logger.info(f"Semaphore size: {semaphore_size}")
    logger.info("")
    
    import time
    phase1_start = time.time()
    
    # Pre-fetch listing times (Ticket #8 optimization: avoid redundant API calls)
    logger.info("[Phase 1] Pre-fetching symbol listing times to avoid redundant API calls...")
    listing_times = {}
    try:
        from data_collector.binance_client import BinanceClient
        from data_collector.bybit_client import BybitClient
        
        async with BinanceClient() as bn_client, BybitClient() as by_client:
            # Fetch Binance listing times in ONE call (more efficient)
            bn_all_times = await bn_client.get_all_symbols_listing_times()
            
            # Fetch Bybit listing times in parallel (one call per symbol)
            by_tasks = [
                by_client.get_symbol_listing_time(symbol_mapping.get(s, {}).get('bybit', s))
                for s in symbols
            ]
            by_listing_times = await asyncio.gather(*by_tasks, return_exceptions=True)
            
            for symbol, by_time in zip(symbols, by_listing_times):
                # Get Binance time from the pre-fetched dictionary
                bn_symbol = symbol_mapping.get(symbol, {}).get('binance', symbol)
                bn_time = bn_all_times.get(bn_symbol)
                
                listing_times[symbol] = {
                    'binance': bn_time,
                    'bybit': by_time if not isinstance(by_time, Exception) else None
                }
            
            logger.info(f"[Phase 1] Pre-fetched listing times for {len(listing_times)} symbols")
    except Exception as e:
        logger.warning(f"[Phase 1] Could not pre-fetch listing times: {e}")
        listing_times = {s: {'binance': None, 'bybit': None} for s in symbols}
    
    logger.info("")
    
    # Phase 1A: Parallel Binance fetching
    phase1a_start = time.time()
    binance_results = await phase1a_fetch_binance_parallel(
        symbols, symbol_mapping, start_time, end_time, analyzer, regen_data, semaphore_size, listing_times
    )
    phase1a_duration = time.time() - phase1a_start
    logger.info(f"Phase 1A completed in {phase1a_duration:.2f}s")
    logger.info("")
    
    # Phase 1B: Parallel Bybit fetching  
    phase1b_start = time.time()
    bybit_results = await phase1b_fetch_bybit_parallel(
        symbols, symbol_mapping, start_time, end_time, analyzer, regen_data, semaphore_size, listing_times
    )
    phase1b_duration = time.time() - phase1b_start
    logger.info(f"Phase 1B completed in {phase1b_duration:.2f}s")
    logger.info("")
    
    # Phase 1C: Merge and generate timelines (now synchronous, no API calls)
    phase1c_start = time.time()
    preloaded_data = phase1c_merge_and_generate_timelines(
        symbols, binance_results, bybit_results, analyzer, start_time, end_time
    )
    phase1c_duration = time.time() - phase1c_start
    logger.info(f"Phase 1C completed in {phase1c_duration:.2f}s")
    logger.info("")
    
    # Overall Phase 1 summary
    phase1_total = time.time() - phase1_start
    logger.info("="*70)
    logger.info("Phase 1 Final Summary:")
    logger.info(f"  Total preloaded: {len(preloaded_data)}")
    logger.info(f"  Phase 1A (Binance): {phase1a_duration:.2f}s")
    logger.info(f"  Phase 1B (Bybit): {phase1b_duration:.2f}s")
    logger.info(f"  Phase 1C (Merge & Timeline): {phase1c_duration:.2f}s")
    logger.info(f"  Total Phase 1 time: {phase1_total:.2f}s")
    logger.info(f"  Max API time (A+B): {max(phase1a_duration, phase1b_duration):.2f}s")
    logger.info("="*70)
    logger.info("")
    
    return preloaded_data


def _generate_timeline_plot_worker(args: tuple) -> tuple:
    """
    Worker function for generating timeline plots in parallel.
    
    This function is designed to work with multiprocessing.
    It takes a tuple of (symbol, timeline_df) and generates a plot.
    
    Args:
        args: Tuple of (symbol, timeline_df)
    
    Returns:
        Tuple of (symbol, success: bool, message: str)
    """
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend for multiprocessing
    
    symbol, timeline_df = args
    
    try:
        if timeline_df.empty:
            return (symbol, False, f"Empty timeline for {symbol}")
        
        # Import here to avoid issues with pickling
        from opportunity_analysis.visualizer import Visualizer
        
        visualizer = Visualizer()
        visualizer.plot_timeline_from_df(timeline_df, symbol)
        
        return (symbol, True, f"✓ Generated timeline for {symbol}")
    except Exception as e:
        return (symbol, False, f"✗ Failed for {symbol}: {str(e)}")


async def phase3_postprocess(
    all_results: List[Dict[str, Any]],
    all_mismatches: List[Dict[str, Any]],
    interval_matrices: Dict[str, pd.DataFrame],
    stats_analyzer: StatisticsAnalyzer,
    visualizer: Visualizer,
    start_time: int,
    end_time: int,
    duration: int
) -> Dict[str, Any]:
    """
    Phase 3: Post-processing (saving, statistical analysis, reporting, visualization).
    
    This function performs all operations after analysis is complete:
    - Saves raw data (mismatch events, funding timelines, interval matrices)
    - Performs statistical analysis
    - Generates text and summary reports
    - Creates visualizations and plots
    - Saves metadata
    
    Args:
        all_results: List of analysis results from Phase 2
        all_mismatches: List of all mismatch events found
        interval_matrices: Dict of interval matrices per symbol
        stats_analyzer: StatisticsAnalyzer instance
        visualizer: Visualizer instance
        start_time: Analysis start time (ms)
        end_time: Analysis end time (ms)
        duration: Duration in days
    
    Returns:
        Dict with post-processing summary including report and metadata
    """
    logger.info("="*70)
    logger.info("Phase 3: Post-processing (Save, Analyze, Report, Visualize)")
    logger.info("="*70)
    
    # Phase 3A: Save raw data
    logger.info("[Post-process] Saving raw data...")
    
    # Save mismatch events
    files_saved = 0
    if all_mismatches:
        mismatches_df = pd.DataFrame(all_mismatches)
        mismatches_df.to_csv(DATA_DIR / 'mismatch_events.csv', index=False)
        logger.info(f"[Post-process] Saved {len(all_mismatches)} mismatch events to CSV")
        files_saved += 1
    
    # Save complete funding rate timelines for each symbol
    logger.info("[Post-process] Saving complete funding rate timelines for each symbol...")
    for result in all_results:
        symbol = result['symbol']
        funding_timeline = result['funding_rate_timeline']
        if not funding_timeline.empty:
            output_file = DATA_DIR / f'funding_rate_timeline_{symbol}.csv'
            funding_timeline.to_csv(output_file, index=False)
    
    logger.info(f"[Post-process] Saved {len(all_results)} complete funding rate timelines")
    files_saved += len(all_results)
    
    # Phase 3B: Perform statistical analysis
    logger.info("[Post-process] Performing statistical analysis...")
    stats = stats_analyzer.analyze_mismatch_events(all_mismatches)
    
    # Add tradable opportunities statistics
    logger.info("[Post-process] Calculating tradable opportunities statistics...")
    tradable_opportunities_by_symbol = {}
    total_tradable_opportunities = 0
    
    for result in all_results:
        symbol = result['symbol']
        funding_timeline = result['funding_rate_timeline']
        if not funding_timeline.empty:
            # Count tradable = True records
            tradable_count = (funding_timeline['tradable'] == True).sum()
            if tradable_count > 0:
                tradable_opportunities_by_symbol[symbol] = tradable_count
                total_tradable_opportunities += tradable_count
    
    # Sort by tradable count
    top_symbols_by_tradable = dict(sorted(
        tradable_opportunities_by_symbol.items(),
        key=lambda x: x[1],
        reverse=True
    ))
    
    stats['total_tradable_opportunities'] = total_tradable_opportunities
    stats['top_symbols_by_tradable'] = top_symbols_by_tradable
    stats['symbols_with_tradable'] = len(tradable_opportunities_by_symbol)
    
    logger.info(f"[Post-process] Found {total_tradable_opportunities} tradable opportunities across {len(tradable_opportunities_by_symbol)} symbols")
    
    # Generate text report
    report = stats_analyzer.generate_text_report(stats)
    
    # Generate tradable opportunities report
    tradable_report = stats_analyzer.generate_tradable_opportunities_report(stats)
    
    # Combine both reports
    combined_report = report + "\n\n" + tradable_report
    
    # Save combined report
    with open(OUTPUT_DIR / 'analysis_report.txt', 'w') as f:
        f.write(combined_report)
    
    logger.info("[Post-process] Saved analysis report")
    files_saved += 1
    
    # Phase 3C: Create summary tables
    logger.info("[Post-process] Creating summary tables...")
    
    tables_created = 0
    if stats['total_events'] > 0:
        # Summary table
        summary_table = stats_analyzer.create_summary_table(stats)
        summary_table.to_csv(OUTPUT_DIR / 'summary_statistics.csv', index=False)
        tables_created += 1
        
        # Symbol ranking
        symbol_ranking = stats_analyzer.create_symbol_ranking_table(stats, top_n=20)
        symbol_ranking.to_csv(OUTPUT_DIR / 'symbol_ranking.csv')
        tables_created += 1
        
        # Monthly summary
        monthly_summary = stats_analyzer.create_monthly_summary_table(stats)
        monthly_summary.to_csv(OUTPUT_DIR / 'monthly_summary.csv')
        tables_created += 1
        
        # Symbol funding rates
        df = stats['dataframe']
        symbol_funding = df.groupby('symbol').agg({
            'avg_binance_rate': 'mean',
            'avg_bybit_rate': 'mean',
            'duration_hours': ['count', 'sum', 'mean']
        }).round(6)
        
        symbol_funding.columns = [
            'Avg Binance Rate',
            'Avg Bybit Rate',
            'Event Count',
            'Total Duration (h)',
            'Avg Duration (h)'
        ]
        
        # Add bps columns
        symbol_funding['Avg Binance (bps)'] = symbol_funding['Avg Binance Rate'] * 10000
        symbol_funding['Avg Bybit (bps)'] = symbol_funding['Avg Bybit Rate'] * 10000
        symbol_funding['Net Funding (bps)'] = (symbol_funding['Avg Binance Rate'] - symbol_funding['Avg Bybit Rate']) * 10000
        
        # Reorder columns
        symbol_funding = symbol_funding[[
            'Event Count',
            'Total Duration (h)',
            'Avg Duration (h)',
            'Avg Binance (bps)',
            'Avg Bybit (bps)',
            'Net Funding (bps)',
            'Avg Binance Rate',
            'Avg Bybit Rate'
        ]]
        
        # Sort by net funding (absolute value)
        symbol_funding = symbol_funding.sort_values('Net Funding (bps)', key=abs, ascending=False)
        symbol_funding.to_csv(OUTPUT_DIR / 'symbol_funding_rates.csv')
        tables_created += 1
        
        logger.info(f"[Post-process] Created {tables_created} summary tables")
        files_saved += tables_created
    
    # Phase 3D: Generate visualizations
    logger.info("[Post-process] Generating visualizations...")
    
    plots_created = 0
    if stats['total_events'] > 0:
        # Heatmap
        visualizer.plot_heatmap(interval_matrices, top_n=20)
        plots_created += 1
        
        # Duration histogram
        visualizer.plot_duration_histogram(stats)
        plots_created += 1
        
        # Symbol ranking
        visualizer.plot_symbol_ranking(stats, top_n=20)
        plots_created += 1
        
        # Mismatch type distribution
        visualizer.plot_mismatch_type_distribution(stats)
        plots_created += 1
        
        # Tradable opportunities by hour
        visualizer.plot_tradable_opportunities(all_results)
        plots_created += 1
        
        # Tradable average funding rate by symbol
        visualizer.plot_tradable_avg_funding_by_symbol(all_results)
        plots_created += 1
        
        # Timeline for all symbols with mismatch events
        # 🚀 OPTIMIZED: Use multiprocessing for parallel timeline generation
        symbols_with_mismatches = list(stats['all_symbols_with_mismatches'].keys())
        logger.info(f"[Post-process] Generating timeline plots for {len(symbols_with_mismatches)} symbols with mismatches...")
        
        if len(symbols_with_mismatches) > 0:
            # Prepare data for multiprocessing
            plot_tasks = []
            for symbol in symbols_with_mismatches:
                result = next((r for r in all_results if r['symbol'] == symbol), None)
                if result and not result['funding_rate_timeline'].empty:
                    plot_tasks.append((symbol, result['funding_rate_timeline']))
            
            if plot_tasks:
                # Use multiprocessing for parallel timeline generation
                import os
                from multiprocessing import Pool
                
                num_workers = min(64, len(plot_tasks), os.cpu_count() or 64)
                logger.info(f"[Post-process] Generating {len(plot_tasks)} timelines using {num_workers} workers...")
                
                try:
                    with Pool(processes=num_workers) as pool:
                        results = pool.map(_generate_timeline_plot_worker, plot_tasks)
                    
                    # Log results
                    success_count = sum(1 for _, success, _ in results if success)
                    for symbol, success, message in results:
                        if not success:
                            logger.warning(f"[Post-process] {message}")
                        elif (symbols_with_mismatches.index(symbol) + 1) % 10 == 0 or symbols_with_mismatches.index(symbol) == len(symbols_with_mismatches) - 1:
                            logger.info(f"[Post-process]   {message}")
                    
                    logger.info(f"[Post-process] Successfully generated {success_count}/{len(plot_tasks)} timeline plots")
                
                except Exception as e:
                    logger.error(f"[Post-process] Multiprocessing failed, falling back to sequential: {e}")
                    # Fallback to sequential execution
                    for idx, (symbol, timeline_df) in enumerate(plot_tasks, 1):
                        try:
                            visualizer.plot_timeline_from_df(timeline_df, symbol)
                            if idx % 10 == 0:
                                logger.info(f"[Post-process]   Generated timeline plots for {idx}/{len(plot_tasks)} symbols")
                        except Exception as e:
                            logger.warning(f"[Post-process] Failed to generate timeline for {symbol}: {e}")
        
        plots_created += len(symbols_with_mismatches)
        logger.info(f"[Post-process] Created {plots_created} plots/timelines in total")
    
    # Phase 3E: Save metadata
    logger.info("[Post-process] Saving metadata...")
    
    metadata = {
        'analysis_date': datetime.now().isoformat(),
        'analysis_days': duration,
        'start_time': datetime.fromtimestamp(start_time/1000).isoformat(),
        'end_time': datetime.fromtimestamp(end_time/1000).isoformat(),
        'total_symbols_analyzed': len(all_results),
        'total_mismatch_events': len(all_mismatches),
        'files_saved': files_saved,
        'plots_created': plots_created,
        'output_directory': str(OUTPUT_DIR)
    }
    
    with open(OUTPUT_DIR / 'metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info("[Post-process] Saved metadata")
    
    # Phase 3 Summary
    logger.info("="*70)
    logger.info("Phase 3 Summary:")
    logger.info(f"  Files saved: {files_saved}")
    logger.info(f"  Plots created: {plots_created}")
    logger.info(f"  Mismatch events analyzed: {len(all_mismatches)}")
    logger.info("="*70)
    
    return {
        'report': report,
        'stats': stats,
        'metadata': metadata,
        'files_saved': files_saved,
        'plots_created': plots_created
    }


def analyze_from_cache(
    symbol: str,
    preloaded: Dict[str, Any],
    analyzer: IntervalAnalyzer,
    start_time: int,
    end_time: int
) -> Optional[Dict[str, Any]]:
    """
    Phase 2A: Analyze a single symbol using preloaded data (no API calls).
    
    This function performs all analysis operations using data that was
    already loaded in Phase 1. No API calls are made here.
    
    ⚠️ IMPORTANT: This is a PURE FUNCTION suitable for multiprocessing.
    Do NOT use async/await. All operations are CPU-bound (pandas, logic).
    
    Args:
        symbol: Trading symbol to analyze
        preloaded: Dict containing preloaded data for this symbol:
                   {
                       'bn_data': [...],
                       'by_data': [...],
                       'bn_timeline': pd.DataFrame(),
                       'by_timeline': pd.DataFrame(),
                       'funding_timeline': pd.DataFrame()
                   }
        analyzer: IntervalAnalyzer instance
        start_time: Analysis start time (ms)
        end_time: Analysis end time (ms)
    
    Returns:
        Dictionary with analysis results or None if analysis fails
    """
    try:
        # Extract preloaded data
        bn_data = preloaded.get('bn_data', [])
        by_data = preloaded.get('by_data', [])
        bn_timeline = preloaded.get('bn_timeline')
        by_timeline = preloaded.get('by_timeline')
        funding_timeline = preloaded.get('funding_timeline')
        
        # Validate data quality (pass lists, not DataFrames)
        bn_quality = analyzer.validate_data_quality(
            bn_data,
            expected_interval=28800,  # Default 8h
            start_time=start_time,
            end_time=end_time
        )
        
        by_quality = analyzer.validate_data_quality(
            by_data,
            expected_interval=28800,
            start_time=start_time,
            end_time=end_time
        )
        
        if not bn_quality['is_valid'] or not by_quality['is_valid']:
            logger.warning(
                f"[Analysis] {symbol}: data quality issues "
                f"(BN={bn_quality['completeness']:.2%}, BY={by_quality['completeness']:.2%})"
            )
            # Continue anyway, log the issues but don't fail
        
        # Detect mismatches
        mismatches = analyzer.detect_mismatches(
            bn_timeline,
            by_timeline,
            start_time,
            end_time,
            symbol
        )
        
        # Create interval matrix for heatmap
        interval_matrix = analyzer.create_interval_matrix(
            bn_timeline,
            by_timeline,
            start_time,
            end_time
        )
        
        return {
            'symbol': symbol,
            'binance_data': bn_data,
            'bybit_data': by_data,
            'mismatches': mismatches,
            'interval_matrix': interval_matrix,
            'funding_rate_timeline': funding_timeline,
            'data_quality': {
                'binance': bn_quality,
                'bybit': by_quality
            }
        }
    
    except Exception as e:
        logger.error(f"[Analysis] {symbol}: ✗ Error - {str(e)}", exc_info=False)
        return None


def phase2_analyze_batch(
    symbols_batch: List[str],
    preloaded_data: Dict[str, Dict[str, Any]],
    analyzer: IntervalAnalyzer,
    start_time: int,
    end_time: int,
    num_workers: int = 64
) -> tuple[List[Optional[Dict[str, Any]]], int]:
    """
    Phase 2B: Parallel analysis of a batch of symbols using preloaded data.
    
    This function takes a batch of symbols and analyzes them in parallel
    using multiprocessing.Pool (NOT async). No API calls are made - all 
    data was preloaded in Phase 1.
    
    🚀 OPTIMIZATION: Uses multiprocessing instead of async because all
    analysis operations are CPU-bound (pandas, logic, no I/O).
    
    Implements FAST-SKIP logic: symbols not in preloaded_data are quickly
    skipped without attempting analysis. This saves time and prevents
    unnecessary error handling.
    
    Args:
        symbols_batch: List of symbols to analyze in this batch
        preloaded_data: Dict mapping symbols to their preloaded data
        analyzer: IntervalAnalyzer instance
        start_time: Analysis start time (ms)
        end_time: Analysis end time (ms)
        num_workers: Number of CPU workers for multiprocessing (default: 64)
    
    Returns:
        Tuple of (analysis_results, skipped_count)
    """
    from multiprocessing import Pool
    from functools import partial
    import os
    
    # ========================================================================
    # FAST-SKIP Logic: Ticket #7
    # ========================================================================
    
    valid_symbols = [s for s in symbols_batch if s in preloaded_data]
    skipped_symbols = [s for s in symbols_batch if s not in preloaded_data]
    
    # Log skipped symbols with reasons
    if skipped_symbols:
        logger.info(f"[Phase 2] FAST-SKIP: {len(skipped_symbols)} symbols not preloaded")
        for symbol in skipped_symbols[:5]:  # Log first 5 for visibility
            logger.debug(f"  - {symbol}: skipped (Phase 1 failure)")
        if len(skipped_symbols) > 5:
            logger.debug(f"  ... and {len(skipped_symbols) - 5} more")
    
    # Early return if no valid symbols
    if not valid_symbols:
        logger.warning("[Phase 2] No valid symbols in this batch (all skipped)")
        return [], len(skipped_symbols)
    
    logger.info(f"[Phase 2] Analyzing {len(valid_symbols)}/{len(symbols_batch)} symbols "
                f"({len(skipped_symbols)} skipped) with {num_workers} CPU workers")
    
    # Determine optimal number of workers
    actual_workers = min(num_workers, len(valid_symbols), os.cpu_count() or 64)
    
    # Create worker function with partial application
    worker_fn = partial(
        analyze_from_cache,
        analyzer=analyzer,
        start_time=start_time,
        end_time=end_time
    )
    
    # Execute analysis in parallel using multiprocessing
    try:
        with Pool(processes=actual_workers) as pool:
            results = []
            for symbol in valid_symbols:
                # Pass symbol and its preloaded data
                result = pool.apply_async(
                    analyze_from_cache,
                    (symbol, preloaded_data[symbol], analyzer, start_time, end_time)
                )
                results.append(result)
            
            # Collect results in order
            analysis_results = [r.get() for r in results]
    except Exception as e:
        logger.error(f"[Phase 2] Multiprocessing error: {e}", exc_info=True)
        # Fallback to sequential execution
        logger.warning("[Phase 2] Falling back to sequential analysis")
        analysis_results = [
            analyze_from_cache(symbol, preloaded_data[symbol], analyzer, start_time, end_time)
            for symbol in valid_symbols
        ]
    
    return analysis_results, len(skipped_symbols)


async def analyze_symbol(
    symbol: str,
    symbol_mapping: dict,
    start_time: int,
    end_time: int,
    analyzer: IntervalAnalyzer,
    regen_data: bool = False
):
    """
    Analyze a single symbol for interval mismatches.
    
    Args:
        symbol: Trading symbol key
        symbol_mapping: Symbol mapping dict
        start_time: Analysis start time (ms)
        end_time: Analysis end time (ms)
        analyzer: IntervalAnalyzer instance
        regen_data: If True, ignore cache and fetch from API
    
    Returns:
        Dictionary with analysis results
    """
    logger.info(f"Analyzing {symbol}...")
    
    try:
        # Load data from cache or fetch from API
        bn_data, by_data = await load_or_fetch_funding_data(
            symbol, symbol_mapping, start_time, end_time, analyzer, regen_data
        )
        
        if not bn_data or not by_data:
            logger.warning(f"Insufficient data for {symbol}")
            return None
        
        # Validate data quality (pass lists, not DataFrames)
        bn_quality = analyzer.validate_data_quality(
            bn_data,
            expected_interval=28800,  # Default 8h
            start_time=start_time,
            end_time=end_time
        )
        
        by_quality = analyzer.validate_data_quality(
            by_data,
            expected_interval=28800,
            start_time=start_time,
            end_time=end_time
        )
        
        if not bn_quality['is_valid'] or not by_quality['is_valid']:
            logger.warning(
                f"Data quality issues for {symbol}: "
                f"BN={bn_quality['completeness']:.2%}, "
                f"BY={by_quality['completeness']:.2%}"
            )
            # Continue anyway, but log the issues
        
        # Create timelines
        bn_timeline = analyzer.create_interval_timeline(bn_data)
        by_timeline = analyzer.create_interval_timeline(by_data)
        
        # Detect mismatches
        mismatches = analyzer.detect_mismatches(
            bn_timeline,
            by_timeline,
            start_time,
            end_time,
            symbol
        )
        
        # Create interval matrix for heatmap
        interval_matrix = analyzer.create_interval_matrix(
            bn_timeline,
            by_timeline,
            start_time,
            end_time
        )
        
        # Note: funding_rate_timeline already loaded from load_or_fetch_funding_data
        # No need to recreate it here
        
        return {
            'symbol': symbol,
            'binance_data': bn_data,
            'bybit_data': by_data,
            'mismatches': mismatches,
            'interval_matrix': interval_matrix,
            'funding_rate_timeline': analyzer.create_funding_rate_timeline(bn_timeline, by_timeline, start_time, end_time),
            'data_quality': {
                'binance': bn_quality,
                'bybit': by_quality
            }
        }
    
    except Exception as e:
        logger.error(f"Error analyzing {symbol}: {e}", exc_info=True)
        return None


async def main():
    """Main analysis workflow with performance monitoring (Ticket #8)."""
    # Initialize performance monitor (Ticket #8)
    perf_monitor = PerformanceMonitor()
    
    logger.info("="*70)
    logger.info("Starting Funding Interval Mismatch Existence Analysis")
    logger.info("="*70)
    
    # Parse command line arguments for time range
    start_time, end_time, duration, regen_data = parse_time_arguments()
    logger.info(f"Analysis period: {duration} days")
    logger.info(f"Start: {datetime.fromtimestamp(start_time/1000)}")
    logger.info(f"End: {datetime.fromtimestamp(end_time/1000)}")
    
    # Get symbol mapping dynamically from exchanges
    logger.info("Fetching all available symbols from exchanges...")
    symbol_mapping = await get_all_symbols_from_exchanges()
    
    if not symbol_mapping:
        logger.error("Failed to fetch symbols from exchanges, using fallback list")
        symbol_mapping = create_symbol_mapping()
    
    symbols = list(symbol_mapping.keys())
    logger.info(f"Found {len(symbols)} common symbols between Binance and Bybit")
    logger.info(f"Analyzing all {len(symbols)} symbols")
    
    # Initialize analyzers
    interval_analyzer = IntervalAnalyzer()
    stats_analyzer = StatisticsAnalyzer()
    visualizer = Visualizer()
    
    # ========================================================================
    # THREE-PHASE ARCHITECTURE WITH PERFORMANCE MONITORING (Ticket #8)
    # ========================================================================
    
    # Phase 1: Sequential data preloading (all API calls happen here)
    perf_monitor.start_phase1()
    preloaded_data = await phase1_preload_data(
        symbols,
        symbol_mapping,
        start_time,
        end_time,
        interval_analyzer,
        regen_data
    )
    
    logger.info(f"Phase 1 complete: {len(preloaded_data)} symbols preloaded")
    
    # Phase 1 performance tracking (Ticket #8)
    # Note: detailed phase1a/1b/1c timings are logged within phase1_preload_data()
    # For simplicity, we estimate from overall phase 1 time
    perf_monitor.end_phase1(10, 10, 80)  # Placeholder; actual values logged separately
    
    # Phase 2: Parallel analysis of preloaded data (no API calls)
    logger.info("="*70)
    logger.info("Phase 2: Analyzing data (Parallel processing)")
    logger.info("="*70)
    
    all_results = []
    all_mismatches = []
    interval_matrices = {}
    total_skipped = 0  # Track fast-skipped symbols (Ticket #7)
    
    perf_monitor.start_phase2()
    
    # Process symbols in batches for parallel analysis
    # Batch size can be larger now since no API calls are made
    batch_size = 20 if len(symbols) > 50 else 10
    num_batches = (len(symbols) - 1) // batch_size + 1
    
    for batch_idx in range(0, len(symbols), batch_size):
        batch = symbols[batch_idx:batch_idx+batch_size]
        current_batch_num = batch_idx // batch_size + 1
        
        logger.info(f"[Phase 2] Batch {current_batch_num}/{num_batches}: Processing {len(batch)} symbols in parallel...")
        
        # Analyze batch in parallel (returns tuple: (results, skipped_count))
        # 🚀 Using multiprocessing now (NOT async) for CPU-bound analysis
        batch_results, batch_skipped = phase2_analyze_batch(
            batch,
            preloaded_data,
            interval_analyzer,
            start_time,
            end_time
        )
        
        total_skipped += batch_skipped
        
        # Aggregate results
        for result in batch_results:
            if result:
                all_results.append(result)
                all_mismatches.extend(result['mismatches'])
                if not result['interval_matrix'].empty:
                    interval_matrices[result['symbol']] = result['interval_matrix']
        
        # Small delay between batches to avoid memory bloat (now using time.sleep instead of await)
        time.sleep(0.5)  # Reduced from 1s since multiprocessing is faster
    
    logger.info("="*70)
    logger.info(f"Phase 2 Summary:")
    logger.info(f"  Total results: {len(all_results)}")
    logger.info(f"  Total mismatch events: {len(all_mismatches)}")
    logger.info(f"  Fast-skipped symbols (Ticket #7): {total_skipped}")
    logger.info("="*70)
    
    # Phase 2 performance tracking (Ticket #8)
    perf_monitor.end_phase2(
        skipped_count=total_skipped,
        analyzed_count=len(all_results),
        failed_count=len(symbols) - len(all_results) - total_skipped
    )
    
    # Phase 3: Post-processing (save, analyze, report, visualize)
    perf_monitor.start_phase3()
    phase3_result = await phase3_postprocess(
        all_results,
        all_mismatches,
        interval_matrices,
        stats_analyzer,
        visualizer,
        start_time,
        end_time,
        duration
    )
    
    # Extract results from Phase 3
    report = phase3_result['report']
    stats = phase3_result['stats']
    
    # Phase 3 performance tracking (Ticket #8)
    perf_monitor.end_phase3(
        files_saved=phase3_result.get('files_saved', 0),
        plots_created=phase3_result.get('plots_created', 0)
    )
    
    # Print performance summary (Ticket #8)
    perf_monitor.print_summary()
    
    # Print report to console
    print(report)
    
    logger.info("="*70)
    logger.info("Analysis Complete!")
    logger.info(f"Results saved to: {OUTPUT_DIR}")
    logger.info("="*70)


if __name__ == "__main__":
    asyncio.run(main())

