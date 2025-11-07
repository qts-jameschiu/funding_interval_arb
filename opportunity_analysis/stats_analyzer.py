"""Statistical analysis of mismatch events and tradable opportunities."""
import logging
from typing import List, Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime
import sys

# Add funding_interval_arb directory to path
sys.path.insert(0, '/home/james/research/funding_interval_arb')

from data_collector.utils import timestamp_to_datetime, format_duration

logger = logging.getLogger(__name__)


class StatisticsAnalyzer:
    """Analyze statistics of interval mismatch events."""
    
    def __init__(self):
        pass
    
    def analyze_mismatch_events(
        self,
        mismatch_events: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Perform comprehensive statistical analysis on mismatch events.
        
        Args:
            mismatch_events: List of detected mismatch events
        
        Returns:
            Dictionary containing various statistics
        """
        if not mismatch_events:
            logger.warning("No mismatch events to analyze")
            return {
                'total_events': 0,
                'total_symbols': 0,
                'summary': 'No mismatch events found'
            }
        
        df = pd.DataFrame(mismatch_events)
        
        # Convert timestamps to datetime
        df['start_datetime'] = pd.to_datetime(df['start_time'], unit='ms')
        df['end_datetime'] = pd.to_datetime(df['end_time'], unit='ms')
        
        # Basic statistics
        total_events = len(df)
        total_symbols = df['symbol'].nunique()
        
        # Duration statistics
        duration_stats = {
            'mean': df['duration_hours'].mean(),
            'median': df['duration_hours'].median(),
            'std': df['duration_hours'].std(),
            'min': df['duration_hours'].min(),
            'max': df['duration_hours'].max(),
            'p25': df['duration_hours'].quantile(0.25),
            'p75': df['duration_hours'].quantile(0.75)
        }
        
        # Events per symbol
        events_per_symbol = df.groupby('symbol').size().sort_values(ascending=False)
        top_symbols = events_per_symbol.head(20).to_dict()
        all_symbols_with_mismatches = events_per_symbol.to_dict()  # 添加：所有有 mismatch 的 symbols
        
        # Mismatch type distribution
        mismatch_type_dist = df['mismatch_type'].value_counts().to_dict()
        
        # Monthly statistics
        df['month'] = df['start_datetime'].dt.to_period('M')
        monthly_stats = df.groupby('month').agg({
            'symbol': 'count',
            'duration_hours': ['mean', 'sum']
        }).to_dict()
        
        # Funding rate statistics during mismatch
        funding_rate_stats = {
            'binance': {
                'mean': df['avg_binance_rate'].mean(),
                'median': df['avg_binance_rate'].median(),
                'std': df['avg_binance_rate'].std(),
                'min': df['avg_binance_rate'].min(),
                'max': df['avg_binance_rate'].max()
            },
            'bybit': {
                'mean': df['avg_bybit_rate'].mean(),
                'median': df['avg_bybit_rate'].median(),
                'std': df['avg_bybit_rate'].std(),
                'min': df['avg_bybit_rate'].min(),
                'max': df['avg_bybit_rate'].max()
            }
        }
        
        # Duration distribution buckets
        duration_buckets = {
            '<1h': len(df[df['duration_hours'] < 1]),
            '1-4h': len(df[(df['duration_hours'] >= 1) & (df['duration_hours'] < 4)]),
            '4-12h': len(df[(df['duration_hours'] >= 4) & (df['duration_hours'] < 12)]),
            '12-24h': len(df[(df['duration_hours'] >= 12) & (df['duration_hours'] < 24)]),
            '>24h': len(df[df['duration_hours'] >= 24])
        }
        
        return {
            'total_events': total_events,
            'total_symbols': total_symbols,
            'duration_stats': duration_stats,
            'top_symbols': top_symbols,
            'all_symbols_with_mismatches': all_symbols_with_mismatches,  # 新增：所有有 mismatch 的 symbols
            'mismatch_type_distribution': mismatch_type_dist,
            'monthly_stats': monthly_stats,
            'funding_rate_stats': funding_rate_stats,
            'duration_buckets': duration_buckets,
            'dataframe': df
        }
    
    def create_summary_table(
        self,
        stats: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Create a summary table of key statistics.
        
        Args:
            stats: Statistics dictionary from analyze_mismatch_events
        
        Returns:
            DataFrame with summary statistics
        """
        if stats['total_events'] == 0:
            return pd.DataFrame()
        
        duration = stats['duration_stats']
        
        summary_data = [
            ['Total Mismatch Events', stats['total_events'], ''],
            ['Unique Symbols', stats['total_symbols'], ''],
            ['', '', ''],
            ['Duration Statistics', '', ''],
            ['  Mean Duration', f"{duration['mean']:.2f}", 'hours'],
            ['  Median Duration', f"{duration['median']:.2f}", 'hours'],
            ['  Min Duration', f"{duration['min']:.2f}", 'hours'],
            ['  Max Duration', f"{duration['max']:.2f}", 'hours'],
            ['  P25 Duration', f"{duration['p25']:.2f}", 'hours'],
            ['  P75 Duration', f"{duration['p75']:.2f}", 'hours'],
            ['', '', ''],
            ['Funding Rate (Binance)', '', ''],
            ['  Mean', f"{stats['funding_rate_stats']['binance']['mean']*10000:.2f}", 'bps'],
            ['  Median', f"{stats['funding_rate_stats']['binance']['median']*10000:.2f}", 'bps'],
            ['', '', ''],
            ['Funding Rate (Bybit)', '', ''],
            ['  Mean', f"{stats['funding_rate_stats']['bybit']['mean']*10000:.2f}", 'bps'],
            ['  Median', f"{stats['funding_rate_stats']['bybit']['median']*10000:.2f}", 'bps'],
        ]
        
        df = pd.DataFrame(summary_data, columns=['Metric', 'Value', 'Unit'])
        return df
    
    def create_symbol_ranking_table(
        self,
        stats: Dict[str, Any],
        top_n: int = 20
    ) -> pd.DataFrame:
        """
        Create a table ranking symbols by mismatch frequency.
        
        Args:
            stats: Statistics dictionary
            top_n: Number of top symbols to include
        
        Returns:
            DataFrame with symbol rankings
        """
        if stats['total_events'] == 0:
            return pd.DataFrame()
        
        df = stats['dataframe']
        
        # Group by symbol and calculate statistics
        symbol_stats = df.groupby('symbol').agg({
            'duration_hours': ['count', 'sum', 'mean', 'median'],
            'avg_binance_rate': 'mean',
            'avg_bybit_rate': 'mean'
        }).round(4)
        
        symbol_stats.columns = [
            'Event Count',
            'Total Duration (h)',
            'Avg Duration (h)',
            'Median Duration (h)',
            'Avg BN Rate',
            'Avg BY Rate'
        ]
        
        # Sort by event count
        symbol_stats = symbol_stats.sort_values('Event Count', ascending=False)
        
        return symbol_stats.head(top_n)
    
    def create_monthly_summary_table(
        self,
        stats: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        Create monthly summary statistics table.
        
        Args:
            stats: Statistics dictionary
        
        Returns:
            DataFrame with monthly statistics
        """
        if stats['total_events'] == 0:
            return pd.DataFrame()
        
        df = stats['dataframe']
        
        monthly = df.groupby('month').agg({
            'symbol': ['count', 'nunique'],
            'duration_hours': ['sum', 'mean']
        }).round(2)
        
        monthly.columns = [
            'Total Events',
            'Unique Symbols',
            'Total Duration (h)',
            'Avg Duration (h)'
        ]
        
        return monthly
    
    def generate_text_report(
        self,
        stats: Dict[str, Any]
    ) -> str:
        """
        Generate a text report summarizing the analysis.
        
        Args:
            stats: Statistics dictionary
        
        Returns:
            Formatted text report
        """
        if stats['total_events'] == 0:
            return "No interval mismatch events detected in the analysis period."
        
        duration = stats['duration_stats']
        
        report = f"""
╔══════════════════════════════════════════════════════════════════════╗
║           Funding Interval Mismatch - Existence Analysis            ║
╚══════════════════════════════════════════════════════════════════════╝

📊 SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total Mismatch Events:  {stats['total_events']}
  Unique Symbols:         {stats['total_symbols']}

⏱️  DURATION STATISTICS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Mean Duration:          {duration['mean']:.2f} hours
  Median Duration:        {duration['median']:.2f} hours
  Min Duration:           {duration['min']:.2f} hours
  Max Duration:           {duration['max']:.2f} hours
  P25 - P75:              {duration['p25']:.2f}h - {duration['p75']:.2f}h

📈 DURATION DISTRIBUTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  < 1 hour:               {stats['duration_buckets']['<1h']} events
  1 - 4 hours:            {stats['duration_buckets']['1-4h']} events
  4 - 12 hours:           {stats['duration_buckets']['4-12h']} events
  12 - 24 hours:          {stats['duration_buckets']['12-24h']} events
  > 24 hours:             {stats['duration_buckets']['>24h']} events

🔄 MISMATCH TYPE DISTRIBUTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        for mtype, count in stats['mismatch_type_distribution'].items():
            report += f"  {mtype:20s} {count:4d} events\n"
        
        # Calculate per-symbol funding rates
        df = stats['dataframe']
        symbol_funding = df.groupby('symbol').agg({
            'avg_binance_rate': 'mean',
            'avg_bybit_rate': 'mean'
        }).sort_values('avg_binance_rate', ascending=False)
        
        # Calculate net funding (difference between exchanges)
        symbol_funding['net_funding_bps'] = (symbol_funding['avg_binance_rate'] - symbol_funding['avg_bybit_rate']) * 10000
        symbol_funding['avg_binance_bps'] = symbol_funding['avg_binance_rate'] * 10000
        symbol_funding['avg_bybit_bps'] = symbol_funding['avg_bybit_rate'] * 10000
        
        report += f"""
💰 FUNDING RATE DURING MISMATCH (BY SYMBOL)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Top 20 Symbols by Average Funding Rate:
  
"""
        for i, (symbol, row) in enumerate(list(symbol_funding.head(20).iterrows()), 1):
            report += f"  {i:2d}. {symbol:15s} BN: {row['avg_binance_bps']:7.2f} bps  |  BY: {row['avg_bybit_bps']:7.2f} bps  |  Net: {row['net_funding_bps']:7.2f} bps\n"
        
        report += f"""
📊 OVERALL EXCHANGE AVERAGES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Binance (mean):         {stats['funding_rate_stats']['binance']['mean']*10000:.2f} bps
  Bybit (mean):           {stats['funding_rate_stats']['bybit']['mean']*10000:.2f} bps

🏆 TOP 10 SYMBOLS BY FREQUENCY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        for i, (symbol, count) in enumerate(list(stats['top_symbols'].items())[:10], 1):
            report += f"  {i:2d}. {symbol:15s} {count:4d} events\n"
        
        report += "\n" + "="*70 + "\n"
        
        return report
    
    def generate_tradable_opportunities_report(
        self,
        stats: Dict[str, Any]
    ) -> str:
        """
        Generate a text report for tradable opportunities statistics.
        
        Args:
            stats: Statistics dictionary containing top_symbols_by_tradable
        
        Returns:
            Formatted text report
        """
        if 'top_symbols_by_tradable' not in stats or not stats['top_symbols_by_tradable']:
            return "No tradable opportunities found in the analysis period."
        
        report = f"""
╔══════════════════════════════════════════════════════════════════════╗
║              Tradable Opportunities - Statistics Report              ║
╚══════════════════════════════════════════════════════════════════════╝

📊 SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total Tradable Opportunities:  {stats.get('total_tradable_opportunities', 0)}
  Symbols with Tradable Opps:    {stats.get('symbols_with_tradable', 0)}

🏆 TOP 20 SYMBOLS BY TRADABLE OPPORTUNITY COUNT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        top_symbols = stats.get('top_symbols_by_tradable', {})
        for i, (symbol, count) in enumerate(list(top_symbols.items())[:20], 1):
            report += f"  {i:2d}. {symbol:15s} {count:4d} opportunities\n"
        
        report += "\n" + "="*70 + "\n"
        
        return report

