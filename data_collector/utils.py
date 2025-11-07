"""Utility functions for data processing and analysis."""
import time
import logging
from typing import Optional, Dict, Any, Union
from datetime import datetime, timedelta
import asyncio
import aiohttp
import numpy as np
import pandas as pd

from .config import MAX_RETRIES, RETRY_DELAY, BACKOFF_FACTOR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = MAX_RETRIES
) -> Optional[Dict[str, Any]]:
    """Fetch data from API with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, timeout=30) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Rate limit
                    wait_time = RETRY_DELAY * (BACKOFF_FACTOR ** attempt)
                    logger.warning(f"Rate limit hit. Waiting {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"HTTP {response.status}: {await response.text()}")
                    return None
        except asyncio.TimeoutError:
            logger.warning(f"Timeout on attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                await asyncio.sleep(RETRY_DELAY * (BACKOFF_FACTOR ** attempt))
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(RETRY_DELAY * (BACKOFF_FACTOR ** attempt))
    
    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
    return None


def timestamp_to_datetime(ts: int) -> datetime:
    """Convert millisecond timestamp to datetime."""
    return datetime.fromtimestamp(ts / 1000)


def datetime_to_timestamp(dt: datetime) -> int:
    """Convert datetime to millisecond timestamp."""
    return int(dt.timestamp() * 1000)


def pandas_timestamp_to_ms(ts: Union[pd.Timestamp, np.int64]) -> int:
    """
    Convert Pandas Timestamp to millisecond Unix timestamp.
    
    Pandas stores timestamps as nanoseconds internally.
    This function converts nanoseconds to milliseconds.
    """
    if isinstance(ts, pd.Timestamp):
        # pandas.Timestamp.value is in nanoseconds, divide by 1,000,000 to get milliseconds
        return int(ts.value // 1_000_000)
    else:
        # Already an integer (likely nanoseconds from datetime64[ns])
        return int(ts // 1_000_000)


def pandas_dt64_to_ms(arr: Union[np.ndarray, pd.Series]) -> np.ndarray:
    """
    Convert numpy datetime64[ns] array or pandas Series to millisecond timestamps.
    
    Parameters:
    -----------
    arr : np.ndarray or pd.Series
        Array or Series with dtype datetime64[ns]
    
    Returns:
    --------
    np.ndarray
        Array of millisecond timestamps (int64)
    """
    if isinstance(arr, pd.Series):
        arr = arr.values
    
    # Convert datetime64[ns] to int64 (nanoseconds), then to milliseconds
    return arr.astype('int64') // 1_000_000


def standardize_timestamp_column(
    df: pd.DataFrame,
    col: str = 'timestamp',
    inplace: bool = False
) -> pd.DataFrame:
    """
    Standardize a timestamp column to millisecond integers.
    
    Handles multiple formats:
    - pandas.Timestamp objects
    - numpy datetime64[ns]
    - integer nanoseconds (from Parquet)
    - integer milliseconds (already correct)
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with timestamp column
    col : str
        Name of timestamp column (default: 'timestamp')
    inplace : bool
        If True, modifies DataFrame in place
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with standardized timestamp column (int64 milliseconds)
    """
    if not inplace:
        df = df.copy()
    
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in DataFrame")
    
    ts_col = df[col]
    
    if len(ts_col) == 0:
        return df
    
    first_val = ts_col.iloc[0]
    
    # Case 1: Pandas Timestamp (has .value attribute with nanoseconds)
    if hasattr(first_val, 'value'):
        df[col] = ts_col.values.astype('int64') // 1_000_000
    # Case 2: numpy datetime64[ns]
    elif str(ts_col.dtype) == 'datetime64[ns]':
        df[col] = ts_col.values.astype('int64') // 1_000_000
    # Case 3: Already integer (either ms or ns)
    else:
        val_int = int(first_val)
        # Heuristic: if value > 1e13, it's likely nanoseconds; < 1e13 is milliseconds
        # Valid millisecond range: [1483228800000, 1767225599999] (2017-2025)
        if val_int > 1e13:  # Nanoseconds
            df[col] = ts_col.astype('int64') // 1_000_000
        else:  # Milliseconds
            df[col] = ts_col.astype('int64')
    
    return df


def validate_timestamp_range(
    timestamps: Union[np.ndarray, pd.Series, list],
    min_ts: int = 1483228800000,  # 2017-01-01
    max_ts: int = 1767225599999   # 2025-12-31
) -> bool:
    """
    Validate that timestamps are in reasonable range (milliseconds).
    
    Parameters:
    -----------
    timestamps : np.ndarray, pd.Series, or list
        Timestamps to validate
    min_ts : int
        Minimum valid timestamp (default: 2017-01-01)
    max_ts : int
        Maximum valid timestamp (default: 2025-12-31)
    
    Returns:
    --------
    bool
        True if all timestamps are in valid range
    """
    if isinstance(timestamps, pd.Series):
        timestamps = timestamps.values
    
    if isinstance(timestamps, np.ndarray):
        return bool(np.all(timestamps >= min_ts) and np.all(timestamps <= max_ts))
    
    # For lists or single values
    return all(min_ts <= int(ts) <= max_ts for ts in timestamps)


def get_time_range(days: int) -> tuple[int, int]:
    """Get start and end timestamps for analysis period.
    
    Returns timestamps aligned to the start of an hour (00:00:00).
    """
    end_time = datetime.now()
    # Align end_time to the start of the next hour
    end_time = end_time.replace(minute=0, second=0, microsecond=0)
    # Move to next hour to ensure we cover the current period
    end_time = end_time + timedelta(hours=1)
    
    start_time = end_time - timedelta(days=days)
    
    return datetime_to_timestamp(start_time), datetime_to_timestamp(end_time)


def interval_to_hours(interval_seconds: int) -> float:
    """Convert interval in seconds to hours."""
    return interval_seconds / 3600


def standardize_interval(interval_seconds: int) -> int:
    """Standardize interval to nearest valid interval (1h, 4h, 8h)."""
    valid_intervals = [3600, 14400, 28800]
    return min(valid_intervals, key=lambda x: abs(x - interval_seconds))


async def get_all_symbols_from_exchanges():
    """
    Dynamically fetch all available symbols from both exchanges.
    Returns a mapping of common symbols.
    """
    import asyncio
    from .binance_client import BinanceClient
    from .bybit_client import BybitClient
    
    async with BinanceClient() as bn_client, BybitClient() as by_client:
        # Get symbols from both exchanges
        bn_info = await bn_client.get_exchange_info()
        by_info = await by_client.get_instruments_info()
        
        if not bn_info or not by_info:
            return {}
        
        bn_symbols = set(bn_info.get('symbols', []))
        by_symbols = set([s['symbol'] for s in by_info.get('symbols', [])])
        
        # Special naming mappings
        special_mappings = {
            # Binance uses 1000X prefix for some low-price tokens
            '1000PEPEUSDT': 'PEPEUSDT',
            '1000SHIBUSDT': 'SHIBUSDT',
            '1000FLOKIUSDT': 'FLOKIUSDT',
            '1000BONKUSDT': 'BONKUSDT',
            '1000RATSUSDT': 'RATSUSDT',
            '1000SATSUSDT': 'SATSUSDT',
            '1000LUNCUSDT': 'LUNCUSDT',
            '1000XECUSDT': 'XECUSDT',
        }
        
        mapping = {}
        
        # Add direct matches
        common_symbols = bn_symbols & by_symbols
        for symbol in common_symbols:
            mapping[symbol] = {
                'binance': symbol,
                'bybit': symbol
            }
        
        # Add special mappings
        for bn_symbol, by_symbol in special_mappings.items():
            if bn_symbol in bn_symbols and by_symbol in by_symbols:
                # Use the Bybit symbol as the key (without 1000 prefix)
                mapping[by_symbol] = {
                    'binance': bn_symbol,
                    'bybit': by_symbol
                }
        
        return mapping


def create_symbol_mapping() -> Dict[str, Dict[str, str]]:
    """
    Create symbol mapping between Binance and Bybit.
    This is a synchronous wrapper that returns a default set.
    For dynamic fetching, use get_all_symbols_from_exchanges() directly.
    """
    # Fallback static mapping for backward compatibility
    common_symbols = [
        'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT',
        'ADAUSDT', 'DOGEUSDT', 'MATICUSDT', 'DOTUSDT', 'LTCUSDT',
        'AVAXUSDT', 'LINKUSDT', 'ATOMUSDT', 'UNIUSDT', 'APTUSDT',
        'ARBUSDT', 'OPUSDT', 'SUIUSDT', 'INJUSDT', 'SEIUSDT'
    ]
    
    mapping = {}
    for symbol in common_symbols:
        mapping[symbol] = {
            'binance': symbol,
            'bybit': symbol
        }
    
    # Special cases with different naming
    special_cases = {
        'PEPEUSDT': {'binance': '1000PEPEUSDT', 'bybit': 'PEPEUSDT'},
        'SHIBUSDT': {'binance': '1000SHIBUSDT', 'bybit': 'SHIBUSDT'},
        'FLOKIUSDT': {'binance': '1000FLOKIUSDT', 'bybit': 'FLOKIUSDT'},
    }
    
    mapping.update(special_cases)
    
    return mapping


def calculate_data_completeness(
    actual_records: int,
    expected_records: int
) -> float:
    """Calculate data completeness ratio."""
    if expected_records == 0:
        return 0.0
    return actual_records / expected_records


def format_duration(seconds: int) -> str:
    """Format duration in seconds to human readable string."""
    hours = seconds / 3600
    if hours < 1:
        return f"{seconds / 60:.0f}m"
    elif hours < 24:
        return f"{hours:.1f}h"
    else:
        days = hours / 24
        return f"{days:.1f}d"

