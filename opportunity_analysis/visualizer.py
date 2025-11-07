"""Visualization tools for interval mismatch analysis."""
import logging
from typing import List, Dict, Any
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
import sys

# Add funding_interval_arb directory to path
sys.path.insert(0, '/home/james/research/funding_interval_arb')

from opportunity_analysis.config import PLOTS_DIR
from data_collector.utils import timestamp_to_datetime

logger = logging.getLogger(__name__)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10


class Visualizer:
    """Create visualizations for interval mismatch analysis."""
    
    def __init__(self, output_dir: Path = PLOTS_DIR):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def plot_heatmap(
        self,
        interval_matrices: Dict[str, pd.DataFrame],
        top_n: int = 20
    ) -> str:
        """
        Create heatmap showing interval differences over time for multiple symbols.
        
        Args:
            interval_matrices: Dict mapping symbol to interval difference DataFrame
            top_n: Number of symbols to display
        
        Returns:
            Path to saved plot
        """
        if not interval_matrices:
            logger.warning("No data for heatmap")
            return ""
        
        # Select top N symbols by number of mismatch hours
        symbol_mismatch_hours = {}
        for symbol, df in interval_matrices.items():
            if not df.empty:
                mismatch_hours = (df['interval_diff'] > 0).sum()
                symbol_mismatch_hours[symbol] = mismatch_hours
        
        top_symbols = sorted(
            symbol_mismatch_hours.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]
        
        if not top_symbols:
            logger.warning("No symbols with mismatches for heatmap")
            return ""
        
        # Prepare data for heatmap
        heatmap_data = []
        for symbol, _ in top_symbols:
            df = interval_matrices[symbol]
            if not df.empty:
                df_indexed = df.set_index('datetime')
                heatmap_data.append(df_indexed['interval_diff'])
        
        if not heatmap_data:
            return ""
        
        # Create heatmap matrix
        heatmap_df = pd.concat(heatmap_data, axis=1)
        heatmap_df.columns = [s[0] for s in top_symbols]
        
        # Resample to daily for better visualization
        heatmap_df_daily = heatmap_df.resample('D').mean()
        
        # Plot
        fig, ax = plt.subplots(figsize=(16, 10))
        
        # Create custom colormap: green (0) -> yellow (1-3) -> red (>3)
        colors = ['#2ecc71', '#f1c40f', '#e74c3c']
        n_bins = 100
        cmap = sns.blend_palette(colors, n_colors=n_bins, as_cmap=True)
        
        sns.heatmap(
            heatmap_df_daily.T,
            cmap=cmap,
            cbar_kws={'label': 'Interval Difference (hours)'},
            linewidths=0.5,
            linecolor='white',
            ax=ax,
            vmin=0,
            vmax=8
        )
        
        ax.set_title(
            'Funding Interval Mismatch Heatmap\n'
            'Green = Synchronized, Yellow = Small Difference, Red = Large Difference',
            fontsize=14,
            fontweight='bold',
            pad=20
        )
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Symbol', fontsize=12)
        
        # Rotate x-axis labels
        plt.xticks(rotation=45, ha='right')
        plt.yticks(rotation=0)
        
        plt.tight_layout()
        
        output_path = self.output_dir / 'interval_mismatch_heatmap.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved heatmap to {output_path}")
        return str(output_path)
    
    def plot_duration_histogram(
        self,
        stats: Dict[str, Any]
    ) -> str:
        """
        Create histogram of mismatch event durations.
        
        Args:
            stats: Statistics dictionary
        
        Returns:
            Path to saved plot
        """
        if stats['total_events'] == 0:
            logger.warning("No events for duration histogram")
            return ""
        
        df = stats['dataframe']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Histogram with custom bins
        bins = [0, 1, 4, 12, 24, 48, 100]
        ax1.hist(df['duration_hours'], bins=bins, edgecolor='black', alpha=0.7, color='#3498db')
        ax1.set_xlabel('Duration (hours)', fontsize=12)
        ax1.set_ylabel('Number of Events', fontsize=12)
        ax1.set_title('Mismatch Event Duration Distribution', fontsize=14, fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # Add statistics text
        mean_dur = stats['duration_stats']['mean']
        median_dur = stats['duration_stats']['median']
        ax1.axvline(mean_dur, color='red', linestyle='--', linewidth=2, label=f'Mean: {mean_dur:.1f}h')
        ax1.axvline(median_dur, color='green', linestyle='--', linewidth=2, label=f'Median: {median_dur:.1f}h')
        ax1.legend()
        
        # Bar chart of duration buckets
        buckets = stats['duration_buckets']
        bucket_names = list(buckets.keys())
        bucket_values = list(buckets.values())
        
        bars = ax2.bar(bucket_names, bucket_values, edgecolor='black', alpha=0.7, color='#e74c3c')
        ax2.set_xlabel('Duration Range', fontsize=12)
        ax2.set_ylabel('Number of Events', fontsize=12)
        ax2.set_title('Events by Duration Range', fontsize=14, fontweight='bold')
        ax2.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax2.text(
                bar.get_x() + bar.get_width()/2.,
                height,
                f'{int(height)}',
                ha='center',
                va='bottom',
                fontsize=10,
                fontweight='bold'
            )
        
        plt.tight_layout()
        
        output_path = self.output_dir / 'duration_histogram.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved duration histogram to {output_path}")
        return str(output_path)
    
    def plot_symbol_ranking(
        self,
        stats: Dict[str, Any],
        top_n: int = 20,
        metric: str = 'tradable'
    ) -> str:
        """
        Create bar chart of top symbols by mismatch frequency or tradable opportunities.
        
        Args:
            stats: Statistics dictionary
            top_n: Number of top symbols to display
            metric: 'tradable' or 'mismatch' - which metric to use for ranking
        
        Returns:
            Path to saved plot
        """
        # Use tradable opportunities by default
        if metric == 'tradable' and 'top_symbols_by_tradable' in stats:
            if stats.get('total_tradable_opportunities', 0) == 0:
                logger.warning("No tradable opportunities for symbol ranking")
                return ""
            
            top_symbols = dict(list(stats['top_symbols_by_tradable'].items())[:top_n])
            xlabel_text = 'Number of Tradable Opportunities'
            title_text = f'Top {top_n} Symbols by Tradable Opportunities'
        else:
            # Fallback to mismatch events
            if stats.get('total_events', 0) == 0:
                logger.warning("No events for symbol ranking")
                return ""
            
            top_symbols = dict(list(stats['top_symbols'].items())[:top_n])
            xlabel_text = 'Number of Mismatch Events'
            title_text = f'Top {top_n} Symbols by Mismatch Frequency'
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        symbols = list(top_symbols.keys())
        counts = list(top_symbols.values())
        
        bars = ax.barh(symbols, counts, edgecolor='black', alpha=0.7, color='#9b59b6')
        
        ax.set_xlabel(xlabel_text, fontsize=12)
        ax.set_ylabel('Symbol', fontsize=12)
        ax.set_title(
            title_text,
            fontsize=14,
            fontweight='bold'
        )
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add value labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax.text(
                width,
                bar.get_y() + bar.get_height()/2.,
                f' {int(width)}',
                ha='left',
                va='center',
                fontsize=9,
                fontweight='bold'
            )
        
        # Invert y-axis to show highest at top
        ax.invert_yaxis()
        
        plt.tight_layout()
        
        output_path = self.output_dir / 'symbol_ranking.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved symbol ranking to {output_path}")
        return str(output_path)
    
    def plot_timeline(
        self,
        binance_data: List[Dict[str, Any]],
        bybit_data: List[Dict[str, Any]],
        symbol: str
    ) -> str:
        """
        Create timeline chart showing interval changes for a specific symbol.
        
        Args:
            binance_data: Binance funding data
            bybit_data: Bybit funding data
            symbol: Trading symbol
        
        Returns:
            Path to saved plot
        """
        if not binance_data or not bybit_data:
            logger.warning(f"No data for timeline of {symbol}")
            return ""
        
        # Convert to DataFrames
        bn_df = pd.DataFrame(binance_data)
        by_df = pd.DataFrame(bybit_data)
        
        # Log raw data info
        logger.info(f"Timeline for {symbol}:")
        logger.info(f"  Binance: {len(bn_df)} records")
        logger.info(f"  Bybit: {len(by_df)} records")
        
        # Convert timestamps to datetime
        bn_df['datetime'] = pd.to_datetime(bn_df['fundingTime'], unit='ms')
        by_df['datetime'] = pd.to_datetime(by_df['fundingTime'], unit='ms')
        
        # Sort by datetime to ensure correct line drawing
        bn_df = bn_df.sort_values('datetime').reset_index(drop=True)
        by_df = by_df.sort_values('datetime').reset_index(drop=True)
        
        logger.info(f"  Binance time range: {bn_df['datetime'].min()} to {bn_df['datetime'].max()}")
        logger.info(f"  Bybit time range: {by_df['datetime'].min()} to {by_df['datetime'].max()}")
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True)
        
        # ===== 第一個子圖：Interval =====
        # Plot Binance intervals
        ax1.plot(
            bn_df['datetime'],
            bn_df['interval_hours'],
            marker='o',
            linestyle='-',
            linewidth=2,
            markersize=4,
            label=f'Binance ({len(bn_df)} records)',
            color='#f39c12',
            alpha=0.8
        )
        
        # Plot Bybit intervals
        ax1.plot(
            by_df['datetime'],
            by_df['interval_hours'],
            marker='s',
            linestyle='-',
            linewidth=2,
            markersize=4,
            label=f'Bybit ({len(by_df)} records)',
            color='#3498db',
            alpha=0.8
        )
        
        ax1.set_ylabel('Funding Interval (hours)', fontsize=12)
        ax1.set_title(
            f'Funding Interval Timeline - {symbol}',
            fontsize=14,
            fontweight='bold'
        )
        ax1.legend(loc='upper right', fontsize=11)
        ax1.grid(True, alpha=0.3)
        ax1.set_yticks([1, 2, 4, 8])
        
        # ===== 第二個子圖：Funding Rate =====
        # Plot Binance funding rates
        ax2.plot(
            bn_df['datetime'],
            bn_df['fundingRate'] * 10000,
            marker='o',
            linestyle='-',
            linewidth=1.5,
            markersize=3,
            label=f'Binance',
            color='#f39c12',
            alpha=0.7
        )
        
        # Plot Bybit funding rates
        ax2.plot(
            by_df['datetime'],
            by_df['fundingRate'] * 10000,
            marker='s',
            linestyle='-',
            linewidth=1.5,
            markersize=3,
            label=f'Bybit',
            color='#3498db',
            alpha=0.7
        )
        
        ax2.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
        ax2.set_xlabel('Date', fontsize=12)
        ax2.set_ylabel('Funding Rate (bps)', fontsize=12)
        ax2.set_title('Funding Rate Timeline', fontsize=14, fontweight='bold')
        ax2.legend(loc='upper right', fontsize=11)
        ax2.grid(True, alpha=0.3)
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        output_path = self.output_dir / f'timeline_{symbol}.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved timeline to {output_path}")
        return str(output_path)
    
    def plot_timeline_from_df(
        self,
        timeline_df: pd.DataFrame,
        symbol: str
    ) -> str:
        """
        Create timeline chart from complete funding rate timeline DataFrame.
        
        Args:
            timeline_df: Complete 90-day funding rate timeline DataFrame
            symbol: Trading symbol
        
        Returns:
            Path to saved plot
        """
        if timeline_df.empty:
            logger.warning(f"No data for timeline of {symbol}")
            return ""
        
        # Ensure datetime is in datetime format
        if not pd.api.types.is_datetime64_any_dtype(timeline_df['datetime']):
            timeline_df = timeline_df.copy()
            timeline_df['datetime'] = pd.to_datetime(timeline_df['datetime'])
        
        logger.info(f"Timeline from DataFrame for {symbol}:")
        logger.info(f"  Total records: {len(timeline_df)}")
        logger.info(f"  Time range: {timeline_df['datetime'].min()} to {timeline_df['datetime'].max()}")
        
        # Create subdirectory for mismatch symbols if it doesn't exist
        mismatch_dir = self.output_dir / 'mismatch_symbol'
        mismatch_dir.mkdir(parents=True, exist_ok=True)
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True)
        
        # ===== 第一個子圖：Interval =====
        # Plot Binance intervals
        ax1.plot(
            timeline_df['datetime'],
            timeline_df['binance_interval'],
            marker='o',
            linestyle='-',
            linewidth=2,
            markersize=3,
            label=f'Binance',
            color='#f39c12',
            alpha=0.8
        )
        
        # Plot Bybit intervals
        ax1.plot(
            timeline_df['datetime'],
            timeline_df['bybit_interval'],
            marker='s',
            linestyle='-',
            linewidth=2,
            markersize=3,
            label=f'Bybit',
            color='#3498db',
            alpha=0.8
        )
        
        # ❌ 移除: Highlight mismatch periods with X markers
        # mismatches = timeline_df[timeline_df['is_mismatch'] == True]
        # if not mismatches.empty:
        #     ax1.scatter(...)
        
        ax1.set_ylabel('Funding Interval (hours)', fontsize=12)
        ax1.set_title(
            f'Funding Interval Timeline - {symbol} (90 days)',
            fontsize=14,
            fontweight='bold'
        )
        ax1.legend(loc='upper right', fontsize=11)
        ax1.grid(True, alpha=0.3)
        ax1.set_yticks([1, 2, 4, 8])
        
        # ===== 第二個子圖：Funding Rate =====
        # Plot Binance funding rates
        ax2.plot(
            timeline_df['datetime'],
            timeline_df['binance_rate'] * 10000,
            marker='o',
            linestyle='-',
            linewidth=1.5,
            markersize=2,
            label=f'Binance',
            color='#f39c12',
            alpha=0.7
        )
        
        # Plot Bybit funding rates
        ax2.plot(
            timeline_df['datetime'],
            timeline_df['bybit_rate'] * 10000,
            marker='s',
            linestyle='-',
            linewidth=1.5,
            markersize=2,
            label=f'Bybit',
            color='#3498db',
            alpha=0.7
        )
        
        # Highlight mismatch periods in funding rate chart (with red background)
        # Only highlight when there's actually a mismatch (is_mismatch == True)
        # This creates individual red spans for each continuous mismatch period
        mismatches_mask = timeline_df['is_mismatch'] == True
        
        if mismatches_mask.any():
            # Group continuous mismatch periods
            mismatch_groups = (mismatches_mask != mismatches_mask.shift()).cumsum()
            
            for group_id in mismatch_groups[mismatches_mask].unique():
                group_data = timeline_df[mismatch_groups == group_id]
                group_start = group_data['datetime'].min()
                group_end = group_data['datetime'].max()
                
                ax2.axvspan(
                    group_start,
                    group_end,
                    alpha=0.1,
                    color='red',
                    label='Mismatch Period' if group_id == mismatch_groups[mismatches_mask].unique()[0] else ""
                )
        
        ax2.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
        ax2.set_xlabel('Date', fontsize=12)
        ax2.set_ylabel('Funding Rate (bps)', fontsize=12)
        ax2.set_title('Funding Rate Timeline', fontsize=14, fontweight='bold')
        ax2.legend(loc='upper right', fontsize=11)
        ax2.grid(True, alpha=0.3)
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Save to mismatch_symbol subdirectory
        output_path = mismatch_dir / f'timeline_{symbol}.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved timeline from DataFrame to {output_path}")
        return str(output_path)
    
    def plot_mismatch_type_distribution(
        self,
        stats: Dict[str, Any]
    ) -> str:
        """
        Create pie chart of mismatch type distribution.
        
        Args:
            stats: Statistics dictionary
        
        Returns:
            Path to saved plot
        """
        if stats['total_events'] == 0:
            logger.warning("No events for mismatch type distribution")
            return ""
        
        mismatch_types = stats['mismatch_type_distribution']
        
        fig, ax = plt.subplots(figsize=(10, 8))
        
        colors = sns.color_palette('Set2', len(mismatch_types))
        
        wedges, texts, autotexts = ax.pie(
            mismatch_types.values(),
            labels=mismatch_types.keys(),
            autopct='%1.1f%%',
            startangle=90,
            colors=colors,
            textprops={'fontsize': 11}
        )
        
        # Make percentage text bold
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(12)
        
        ax.set_title(
            'Mismatch Type Distribution',
            fontsize=14,
            fontweight='bold',
            pad=20
        )
        
        plt.tight_layout()
        
        output_path = self.output_dir / 'mismatch_type_distribution.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved mismatch type distribution to {output_path}")
        return str(output_path)
    
    def plot_tradable_opportunities(
        self,
        all_results: List[Dict[str, Any]]
    ) -> str:
        """
        Create time-series plot showing tradable opportunities from start_date to end_date.
        
        For each hourly datetime point:
        - Count how many symbols have tradable=true at that specific time
        - Calculate the average absolute funding rate of those tradable records
        - Fill missing time periods with 0
        - Generate both a CSV table and visualization
        
        Args:
            all_results: List of analysis results for all symbols
        
        Returns:
            Path to saved plot
        """
        logger.info("Generating tradable opportunities time-series plot...")
        
        # Collect all tradable data points across all symbols
        tradable_data = []
        
        for result in all_results:
            if result is None or result['funding_rate_timeline'].empty:
                continue
            
            df = result['funding_rate_timeline']
            
            # Check if 'tradable' column exists
            if 'tradable' not in df.columns:
                logger.warning(f"No 'tradable' column for {result['symbol']}, skipping")
                continue
            
            # Filter tradable rows only
            tradable_df = df[df['tradable'] == True].copy()
            
            if tradable_df.empty:
                continue
            
            # Convert datetime to proper format
            tradable_df['datetime'] = pd.to_datetime(tradable_df['datetime'])
            
            # Calculate funding rate in basis points using absolute values
            # avg = (abs(binance_rate) + abs(bybit_rate)) / tradable_count
            tradable_df['funding_rate_bp'] = tradable_df.apply(
                lambda row: (abs(row['binance_rate']) * 10000 + abs(row['bybit_rate']) * 10000) / 2,
                axis=1
            )
            
            # Store each tradable record
            for _, row in tradable_df.iterrows():
                tradable_data.append({
                    'datetime': row['datetime'],
                    'rate_bp': row['funding_rate_bp'],
                    'symbol': result['symbol']
                })
        
        if not tradable_data:
            logger.warning("No tradable data found")
            return ""
        
        # Convert to DataFrame for easier aggregation
        tradable_df_all = pd.DataFrame(tradable_data)
        
        # Group by datetime and calculate count and average rate
        timeseries_data = tradable_df_all.groupby('datetime').agg({
            'rate_bp': ['count', 'mean']
        }).reset_index()
        
        # Flatten column names
        timeseries_data.columns = ['datetime', 'opportunity', 'average_funding_bp']
        timeseries_data = timeseries_data.sort_values('datetime').reset_index(drop=True)
        
        # Create continuous time series with hourly frequency
        min_datetime = timeseries_data['datetime'].min()
        max_datetime = timeseries_data['datetime'].max()
        
        # Generate all hourly timestamps between min and max
        all_datetimes = pd.date_range(start=min_datetime, end=max_datetime, freq='1H')
        continuous_timeseries = pd.DataFrame({'datetime': all_datetimes})
        
        # Merge with existing data, filling missing values with 0
        timeseries_data = continuous_timeseries.merge(
            timeseries_data, 
            on='datetime', 
            how='left'
        )
        timeseries_data['opportunity'] = timeseries_data['opportunity'].fillna(0).astype(int)
        timeseries_data['average_funding_bp'] = timeseries_data['average_funding_bp'].fillna(0).round(2)
        
        # Save to CSV
        csv_output_path = self.output_dir / 'tradable_opportunities_timeseries.csv'
        timeseries_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved tradable opportunities timeseries CSV to {csv_output_path}")
        logger.info(f"\nTimeseries Summary:")
        logger.info(f"\n{timeseries_data.to_string()}\n")
        
        # Create figure with two y-axes
        fig, ax1 = plt.subplots(figsize=(24, 8))
        
        # Plot 1: Average Funding Rate on left y-axis (bar chart)
        color1 = '#e74c3c'
        ax1.set_xlabel('DateTime (UTC)', fontsize=13, fontweight='bold')
        ax1.set_ylabel('Average Funding Rate (bp)', color=color1, fontsize=13, fontweight='bold')
        bars = ax1.bar(range(len(timeseries_data)), timeseries_data['average_funding_bp'], 
                       color=color1, alpha=0.6, edgecolor='darkred', linewidth=0.5, label='Average Funding Rate (bp)')
        ax1.tick_params(axis='y', labelcolor=color1, labelsize=11)
        ax1.grid(True, alpha=0.3, axis='y')
        # Set y-axis to start from 0
        ax1.set_ylim(bottom=0)
        
        # Plot 2: Opportunity Count on right y-axis (line chart)
        ax2 = ax1.twinx()
        color2 = '#3498db'
        ax2.set_ylabel('Number of Tradable Opportunities', color=color2, fontsize=13, fontweight='bold')
        line = ax2.plot(range(len(timeseries_data)), timeseries_data['opportunity'],
                        color=color2, linewidth=2.5, marker='o', markersize=4,
                        label='Opportunity Count', alpha=0.9)
        ax2.tick_params(axis='y', labelcolor=color2, labelsize=11)
        # Align zero on both axes
        ax2.set_ylim(bottom=0)
        
        # Set x-axis with datetime labels
        ax1.set_xticks(range(0, len(timeseries_data), max(1, len(timeseries_data) // 20)))
        x_labels = [timeseries_data.loc[i, 'datetime'].strftime('%Y-%m-%d %H:%M') 
                   for i in range(0, len(timeseries_data), max(1, len(timeseries_data) // 20))]
        ax1.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=10)
        
        # Title and legend
        plt.title(
            'Tradable Opportunities Time Series Analysis',
            fontsize=15, fontweight='bold', pad=20
        )
        
        # Combine legends from both axes
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=12, framealpha=0.95)
        
        plt.tight_layout()
        
        output_path = self.output_dir / 'tradable_opportunities_by_hour.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved tradable opportunities time-series plot to {output_path}")
        logger.info(f"  Total time periods: {len(timeseries_data)}")
        logger.info(f"  Periods with opportunities: {len(timeseries_data[timeseries_data['opportunity'] > 0])}")
        peak_idx = timeseries_data['opportunity'].idxmax()
        logger.info(f"  Peak: {timeseries_data.loc[peak_idx, 'datetime']} with {int(timeseries_data.loc[peak_idx, 'opportunity'])} opportunities")
        logger.info(f"  Average funding rate: {timeseries_data['average_funding_bp'].mean():.2f} bp")
        
        return str(output_path)
    
    def plot_tradable_avg_funding_by_symbol(
        self,
        all_results: List[Dict[str, Any]]
    ) -> str:
        """
        Create bar chart showing average abs(funding rate) for tradable opportunities by symbol.
        
        This chart shows which symbols have the highest average funding rates during tradable windows.
        
        Args:
            all_results: List of analysis results from Phase 2
        
        Returns:
            Path to saved plot
        """
        # Collect tradable funding rates by symbol
        tradable_funding_by_symbol = {}
        
        for result in all_results:
            symbol = result['symbol']
            funding_timeline = result['funding_rate_timeline']
            
            if not funding_timeline.empty:
                # Filter for tradable opportunities only
                tradable_rows = funding_timeline[funding_timeline['tradable'] == True]
                
                if not tradable_rows.empty:
                    # Calculate average cost of funding (the funding we need to pay)
                    # If binance_pay=False: we RECEIVE from Binance (use bybit's cost)
                    # If binance_pay=True: we PAY to Binance (use binance's cost)
                    funding_costs = []
                    for _, row in tradable_rows.iterrows():
                        bn_rate_bp = abs(row['binance_rate']) * 10000 if row['binance_rate'] else 0
                        by_rate_bp = abs(row['bybit_rate']) * 10000 if row['bybit_rate'] else 0
                        
                        # Determine which funding rate represents our cost
                        # If binance_pay is False, we're receiving from Binance, so cost is Bybit's
                        # If bybit_pay is True, we're paying to Bybit, so cost is Bybit's
                        if not row.get('binance_pay', False) or row.get('bybit_pay', False):
                            # We pay to Bybit (or receive from Binance)
                            funding_costs.append(by_rate_bp)
                        else:
                            # We pay to Binance
                            funding_costs.append(bn_rate_bp)
                    
                    avg_funding = sum(funding_costs) / len(funding_costs) if funding_costs else 0
                    tradable_funding_by_symbol[symbol] = avg_funding
        
        if not tradable_funding_by_symbol:
            logger.warning("No tradable opportunities found for funding analysis")
            return ""
        
        # Sort by average funding rate (descending)
        sorted_symbols = dict(sorted(
            tradable_funding_by_symbol.items(),
            key=lambda x: x[1],
            reverse=True
        ))
        
        # Take top 20
        top_20 = dict(list(sorted_symbols.items())[:20])
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        symbols = list(top_20.keys())
        avg_fundings = list(top_20.values())
        
        # Color based on funding rate intensity
        colors = plt.cm.RdYlGn_r([(x - min(avg_fundings)) / (max(avg_fundings) - min(avg_fundings)) 
                                   for x in avg_fundings])
        
        bars = ax.barh(symbols, avg_fundings, edgecolor='black', alpha=0.8, color=colors)
        
        ax.set_xlabel('Average Funding Rate (bp)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Symbol', fontsize=12)
        ax.set_title(
            'Top Symbols by Average Funding Rate During Tradable Windows',
            fontsize=14,
            fontweight='bold'
        )
        ax.grid(True, alpha=0.3, axis='x')
        
        # Add value labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax.text(
                width,
                bar.get_y() + bar.get_height()/2.,
                f' {width:.2f} bp',
                ha='left',
                va='center',
                fontsize=9,
                fontweight='bold'
            )
        
        # Invert y-axis to show highest at top
        ax.invert_yaxis()
        
        plt.tight_layout()
        
        output_path = self.output_dir / 'tradable_avg_funding_by_symbol.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved tradable average funding by symbol plot to {output_path}")
        logger.info(f"  Symbols with tradable opportunities: {len(tradable_funding_by_symbol)}")
        logger.info(f"  Top symbol: {symbols[0]} with {avg_fundings[0]:.2f} bp average funding")
        logger.info(f"  Average across all: {sum(avg_fundings) / len(avg_fundings):.2f} bp")
        
        return str(output_path)

