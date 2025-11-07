"""Interval mismatch detection and analysis."""
import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
import sys

# Add funding_interval_arb directory to path
sys.path.insert(0, '/home/james/research/funding_interval_arb')

from opportunity_analysis.config import MISMATCH_THRESHOLD, VALID_INTERVALS
from data_collector.utils import timestamp_to_datetime, interval_to_hours, standardize_interval

logger = logging.getLogger(__name__)


class IntervalAnalyzer:
    """Analyze funding interval mismatches between exchanges."""
    
    def __init__(self):
        self.mismatch_threshold = MISMATCH_THRESHOLD
        self.valid_intervals = VALID_INTERVALS
    
    def create_interval_timeline(
        self,
        funding_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Create a timeline of interval periods from funding data.
        Each period represents the interval that was active between two funding events.
        
        Args:
            funding_data: Processed funding data with intervals
        
        Returns:
            List of timeline periods with start, end, interval, and rate
        """
        if not funding_data or len(funding_data) < 2:
            return []
        
        timeline = []
        for i in range(len(funding_data) - 1):
            current = funding_data[i]
            next_record = funding_data[i + 1]
            
            if current['interval'] is not None:
                timeline.append({
                    'start': current['fundingTime'],
                    'end': next_record['fundingTime'],
                    'interval': current['interval'],
                    'interval_hours': current['interval_hours'],
                    'rate': current['fundingRate']
                })
        
        return timeline
    
    def get_interval_at_time(
        self,
        timeline: List[Dict[str, Any]],
        query_time: int
    ) -> Tuple[int, float]:
        """
        Get the active interval at a specific time.
        
        Args:
            timeline: Interval timeline
            query_time: Query timestamp in milliseconds
        
        Returns:
            Tuple of (interval_seconds, funding_rate) or (None, None)
        """
        for period in timeline:
            if period['start'] <= query_time < period['end']:
                return period['interval'], period['rate']
        return None, None
    
    def detect_mismatches(
        self,
        binance_timeline: List[Dict[str, Any]],
        bybit_timeline: List[Dict[str, Any]],
        start_time: int,
        end_time: int,
        symbol: str
    ) -> List[Dict[str, Any]]:
        """
        Detect interval mismatches between Binance and Bybit.
        
        Args:
            binance_timeline: Binance interval timeline
            bybit_timeline: Bybit interval timeline
            start_time: Analysis start time (ms)
            end_time: Analysis end time (ms)
            symbol: Trading symbol
        
        Returns:
            List of mismatch events
        """
        if not binance_timeline or not bybit_timeline:
            logger.warning(f"Empty timeline for {symbol}")
            return []
        
        # Create hourly time grid
        time_grid = range(start_time, end_time, 3600000)  # 1 hour in milliseconds
        
        mismatch_events = []
        current_mismatch = None
        
        for t in time_grid:
            bn_interval, bn_rate = self.get_interval_at_time(binance_timeline, t)
            by_interval, by_rate = self.get_interval_at_time(bybit_timeline, t)
            
            if bn_interval is None or by_interval is None:
                continue
            
            # Check if there's a mismatch
            interval_diff = abs(bn_interval - by_interval)
            is_mismatch = interval_diff >= self.mismatch_threshold
            
            if is_mismatch:
                if current_mismatch is None:
                    # Start new mismatch event
                    current_mismatch = {
                        'symbol': symbol,
                        'start_time': t,
                        'binance_interval': bn_interval,
                        'bybit_interval': by_interval,
                        'binance_rates': [bn_rate],
                        'bybit_rates': [by_rate],
                        'interval_diff': interval_diff
                    }
                else:
                    # Continue existing mismatch
                    current_mismatch['binance_rates'].append(bn_rate)
                    current_mismatch['bybit_rates'].append(by_rate)
            else:
                if current_mismatch is not None:
                    # End current mismatch event
                    current_mismatch['end_time'] = t
                    current_mismatch['duration_hours'] = (t - current_mismatch['start_time']) / 3600000
                    current_mismatch['avg_binance_rate'] = np.mean(current_mismatch['binance_rates'])
                    current_mismatch['avg_bybit_rate'] = np.mean(current_mismatch['bybit_rates'])
                    bn_hours = round(current_mismatch['binance_interval'] / 3600)
                    by_hours = round(current_mismatch['bybit_interval'] / 3600)
                    current_mismatch['mismatch_type'] = f"{bn_hours}h_vs_{by_hours}h"
                    
                    mismatch_events.append(current_mismatch)
                    current_mismatch = None
        
        # Handle ongoing mismatch at end of period
        if current_mismatch is not None:
            current_mismatch['end_time'] = end_time
            current_mismatch['duration_hours'] = (end_time - current_mismatch['start_time']) / 3600000
            current_mismatch['avg_binance_rate'] = np.mean(current_mismatch['binance_rates'])
            current_mismatch['avg_bybit_rate'] = np.mean(current_mismatch['bybit_rates'])
            bn_hours = round(current_mismatch['binance_interval'] / 3600)
            by_hours = round(current_mismatch['bybit_interval'] / 3600)
            current_mismatch['mismatch_type'] = f"{bn_hours}h_vs_{by_hours}h"
            mismatch_events.append(current_mismatch)
        
        logger.info(f"Found {len(mismatch_events)} mismatch events for {symbol}")
        return mismatch_events
    
    def create_funding_rate_timeline(
        self,
        binance_timeline: List[Dict[str, Any]],
        bybit_timeline: List[Dict[str, Any]],
        start_time: int,
        end_time: int
    ) -> pd.DataFrame:
        """
        Create a complete timeline of funding rates for the entire analysis period.
        This captures ALL rates (not just during mismatches) for complete time-series analysis.
        
        Returns:
            DataFrame with datetime, binance_interval, bybit_interval, binance_rate, bybit_rate, and mismatch flag
        """
        # Create hourly time grid
        time_grid = pd.date_range(
            start=timestamp_to_datetime(start_time),
            end=timestamp_to_datetime(end_time),
            freq='h'
        )
        
        data = []
        for dt in time_grid:
            t = int(dt.timestamp() * 1000)
            bn_interval, bn_rate = self.get_interval_at_time(binance_timeline, t)
            by_interval, by_rate = self.get_interval_at_time(bybit_timeline, t)
            
            if bn_interval is not None and by_interval is not None:
                # Check if there's a mismatch
                interval_diff = abs(bn_interval - by_interval)
                is_mismatch = interval_diff >= self.mismatch_threshold
                
                # Convert intervals to hours
                bn_hours = round(bn_interval / 3600)
                by_hours = round(by_interval / 3600)
                
                # Calculate settlement times based on actual intervals
                # Binance: 0, 8, 16 UTC (8h interval)
                # Bybit: 0, 4, 8, 12, 16, 20 UTC (4h interval)
                # etc.
                hour_utc = dt.hour
                binance_pay = (hour_utc % bn_hours == 0)  # Settlement every bn_hours
                bybit_pay = (hour_utc % by_hours == 0)    # Settlement every by_hours
                
                # Calculate tradable opportunity
                # True if only one exchange is paying AND that exchange's rate > 16bp (0.0016)
                only_binance_paying = binance_pay and not bybit_pay
                only_bybit_paying = bybit_pay and not binance_pay
                
                binance_rate_bp = abs(bn_rate) * 10000 if bn_rate else 0  # Convert to basis points
                bybit_rate_bp = abs(by_rate) * 10000 if by_rate else 0
                
                tradable = False
                if only_binance_paying and binance_rate_bp > 16:
                    tradable = True
                elif only_bybit_paying and bybit_rate_bp > 16:
                    tradable = True
                
                data.append({
                    'datetime': dt,
                    'binance_interval': bn_hours,
                    'bybit_interval': by_hours,
                    'interval_diff': abs(bn_hours - by_hours),
                    'binance_rate': bn_rate,
                    'bybit_rate': by_rate,
                    'rate_diff': bn_rate - by_rate,
                    'is_mismatch': is_mismatch,
                    'mismatch_type': f"{bn_hours}h_vs_{by_hours}h" if is_mismatch else 'match',
                    'binance_pay': binance_pay,
                    'bybit_pay': bybit_pay,
                    'tradable': tradable
                })
        
        return pd.DataFrame(data)
    
    def create_interval_matrix(
        self,
        binance_timeline: List[Dict[str, Any]],
        bybit_timeline: List[Dict[str, Any]],
        start_time: int,
        end_time: int
    ) -> pd.DataFrame:
        """
        Create a time-series matrix of intervals for heatmap visualization.
        
        Returns:
            DataFrame with timestamps as index and interval differences
        """
        # Create hourly time grid
        time_grid = pd.date_range(
            start=timestamp_to_datetime(start_time),
            end=timestamp_to_datetime(end_time),
            freq='h'
        )
        
        data = []
        for dt in time_grid:
            t = int(dt.timestamp() * 1000)
            bn_interval, _ = self.get_interval_at_time(binance_timeline, t)
            by_interval, _ = self.get_interval_at_time(bybit_timeline, t)
            
            if bn_interval is not None and by_interval is not None:
                # Convert intervals to hours (intervals are in seconds)
                bn_hours = round(bn_interval / 3600)
                by_hours = round(by_interval / 3600)
                diff_hours = abs(bn_hours - by_hours)
                data.append({
                    'datetime': dt,
                    'binance_interval': bn_hours,
                    'bybit_interval': by_hours,
                    'interval_diff': diff_hours
                })
        
        return pd.DataFrame(data)
    
    def validate_data_quality(
        self,
        funding_data: List[Dict[str, Any]],
        expected_interval: int,
        start_time: int,
        end_time: int
    ) -> Dict[str, Any]:
        """
        Validate data quality and completeness.
        
        Args:
            funding_data: Funding rate records
            expected_interval: Expected funding interval in seconds
            start_time: Analysis start time
            end_time: Analysis end time
        
        Returns:
            Dictionary with quality metrics
        """
        if not funding_data:
            return {
                'is_valid': False,
                'completeness': 0.0,
                'issues': ['No data']
            }
        
        issues = []
        
        # Calculate expected number of records
        time_range = (end_time - start_time) / 1000  # in seconds
        actual_records = len(funding_data)
        
        # For very small time ranges (< 1 interval), calculate completeness differently
        # to avoid unrealistic percentages
        if time_range < expected_interval:
            # Small time range - just check if we have at least 1 record
            completeness = 1.0 if actual_records > 0 else 0.0
            expected_records = 1
        else:
            expected_records = int(time_range / expected_interval)
            completeness = actual_records / expected_records if expected_records > 0 else 0
        
        # Cap completeness at 100% for practical purposes (avoid showing 799%)
        completeness = min(completeness, 1.0)
        
        # Check for large time gaps
        for i in range(1, len(funding_data)):
            gap = (funding_data[i]['fundingTime'] - funding_data[i-1]['fundingTime']) / 1000
            if gap > 86400:  # 24 hours
                issues.append(f"Large time gap: {gap/3600:.1f}h at {funding_data[i]['datetime']}")
        
        # Check funding rate validity
        for record in funding_data:
            if abs(record['fundingRate']) > 0.005:  # ±0.5%
                issues.append(f"Unusual funding rate: {record['fundingRate']:.4f} at {record['datetime']}")
        
        return {
            'is_valid': completeness >= 0.8 and len(issues) < 5,
            'completeness': completeness,
            'expected_records': expected_records,
            'actual_records': actual_records,
            'issues': issues[:10]  # Limit to first 10 issues
        }

