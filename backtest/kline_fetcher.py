"""
K 線獲取器 - 非同步並行獲取和緩存 (支持智能增量更新)
使用 asyncio + aiohttp 實現 20x 性能加速
支持 Binance 和 Bybit 的分頁獲取
根據快取覆蓋和 symbol 上市時間智能補充缺失數據
"""

import asyncio
import aiohttp
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_collector.utils import standardize_timestamp_column, validate_timestamp_range

logger = logging.getLogger(__name__)


class KlineFetcher:
    """K 線非同步獲取器 (支持智能增量更新)"""
    
    def __init__(self, cache_dir: str = "/tmp/kline_cache"):
        """
        初始化獲取器
        
        Args:
            cache_dir: K 線快取目錄
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Semaphore 設定 (限制並發度)
        self.bn_semaphore = asyncio.Semaphore(5)   # Binance: 5 並發
        self.by_semaphore = asyncio.Semaphore(5)   # Bybit: 5 並發
        
        # API 配置
        self.BINANCE_LIMIT = 1000  # Binance 每次最多返回 1000 條
        self.BYBIT_LIMIT = 200     # Bybit 每次最多返回 200 條
        
        # Symbol 上市時間快取
        self.listing_times: Dict[str, Dict[str, Optional[int]]] = {}
    
    def _get_cache_path(self, symbol: str, exchange: str) -> Path:
        """生成快取路徑 (簡化格式)"""
        return self.cache_dir / f"{symbol}_{exchange}.parquet"
    
    def _check_cache_coverage(
        self,
        symbol: str,
        exchange: str,
        required_start_ms: int,
        required_end_ms: int
    ) -> Tuple[bool, Optional[pd.DataFrame]]:
        """
        檢查快取覆蓋情況
        
        Returns:
            (is_complete, dataframe)
            - is_complete: True 表示快取完全覆蓋所需時間範圍
            - dataframe: 快取數據 (如果存在)
        """
        cache_path = self._get_cache_path(symbol, exchange)
        
        if not cache_path.exists():
            return False, None
        
        try:
            df = pd.read_parquet(cache_path)
            
            if len(df) == 0:
                return False, None
            
            # 使用統一的時間戳標準化函數
            df = standardize_timestamp_column(df, col='timestamp')
            
            # 驗證時間戳範圍
            if not validate_timestamp_range(df['timestamp'].values):
                logger.warning(f"快取時間戳超出範圍 {symbol}({exchange})")
                return False, None
            
            # 檢查時間範圍
            cache_start = int(df['timestamp'].min())
            cache_end = int(df['timestamp'].max())
            
            # 檢查是否完全覆蓋
            is_complete = (cache_start <= required_start_ms and 
                          cache_end >= required_end_ms)
            
            if is_complete:
                logger.debug(f"快取完全覆蓋 {symbol}({exchange}): "
                           f"{cache_start} ~ {cache_end}")
            else:
                logger.debug(f"快取部分覆蓋 {symbol}({exchange}): "
                           f"快取 {cache_start} ~ {cache_end}, "
                           f"需要 {required_start_ms} ~ {required_end_ms}")
            
            return is_complete, df
        except Exception as e:
            logger.warning(f"快取讀取失敗 {symbol}({exchange}): {e}")
            return False, None
    
    def _normalize_listing_time(self, listing_time_ms: Optional[int]) -> Optional[int]:
        """
        規範化上市時間，確保單位正確
        上市時間應該在 2017-2025 年之間（合理範圍）
        
        Args:
            listing_time_ms: 可能來自 API 的上市時間
        
        Returns:
            規範化後的毫秒時間戳，或 None 如果無效
        """
        if listing_time_ms is None:
            return None
        
        # 合理的時間範圍：2017-01-01 ~ 2025-12-31 (毫秒)
        MIN_TIME_MS = 1483228800000  # 2017-01-01
        MAX_TIME_MS = 1767225599999  # 2025-12-31
        
        # 檢查是否在合理範圍
        if MIN_TIME_MS <= listing_time_ms <= MAX_TIME_MS:
            return listing_time_ms
        
        # 如果太大，可能是秒而不是毫秒
        if listing_time_ms > MAX_TIME_MS:
            listing_time_seconds = listing_time_ms // 1000
            if MIN_TIME_MS <= listing_time_seconds * 1000 <= MAX_TIME_MS:
                # 可能是以秒為單位，轉換為毫秒
                logger.debug(f"上市時間可能以秒為單位，轉換: {listing_time_ms} → {listing_time_seconds * 1000}")
                return listing_time_seconds * 1000
        
        # 無法規範化，返回 None
        logger.warning(f"上市時間超出合理範圍，忽略: {listing_time_ms}")
        return None
    
    def _calculate_missing_periods(
        self,
        symbol: str,
        exchange: str,
        required_start_ms: int,
        required_end_ms: int,
        cached_start_ms: Optional[int],
        cached_end_ms: Optional[int]
    ) -> List[Tuple[int, int, str]]:
        """
        計算缺失的時間段
        根據快取覆蓋和 symbol 上市時間智能決定需要補充的數據
        
        邏輯：
        1. 不抓取 symbol 上市前的數據
        2. 只補充快取沒有覆蓋的時間段
        3. 如果沒有快取，抓取完整時間範圍
        
        Args:
            symbol: 交易對
            exchange: 交易所
            required_start_ms: 所需開始時間
            required_end_ms: 所需結束時間
            cached_start_ms: 快取開始時間 (None 表示無快取)
            cached_end_ms: 快取結束時間
        
        Returns:
            [(start_ms, end_ms, period_type), ...]
            period_type: 'before', 'after', 或 'full'
        """
        fetch_periods = []
        
        # 獲取該 symbol 在此交易所的上市時間
        listing_time_ms = None
        if symbol in self.listing_times:
            listing_time_ms = self.listing_times[symbol].get(exchange)
        
        # 決定實際的開始時間 (不早於上市時間)
        effective_start_ms = required_start_ms
        if listing_time_ms is not None and required_start_ms < listing_time_ms:
            logger.debug(f"[Cache] {symbol}({exchange}): gap start before listing time, "
                        f"adjusted fetch start from {datetime.fromtimestamp(required_start_ms/1000).strftime('%Y-%m-%d')} "
                        f"to {datetime.fromtimestamp(listing_time_ms/1000).strftime('%Y-%m-%d')}")
            effective_start_ms = listing_time_ms
        elif listing_time_ms is None:
            # 沒有上市時間信息，使用原始時間
            logger.debug(f"[Cache] {symbol}({exchange}): no listing time available, using required start {datetime.fromtimestamp(required_start_ms/1000).strftime('%Y-%m-%d')}")
        else:
            # 上市時間在所需時間之後，正常使用所需時間
            logger.debug(f"[Cache] {symbol}({exchange}): listing time {datetime.fromtimestamp(listing_time_ms/1000).strftime('%Y-%m-%d')} is after required start {datetime.fromtimestamp(required_start_ms/1000).strftime('%Y-%m-%d')}")
        
        # 無快取情況
        if cached_start_ms is None:
            logger.info(f"[Cache] {symbol}({exchange}): no cache, fetching full period ({datetime.fromtimestamp(effective_start_ms/1000).strftime('%Y-%m-%d')} ~ {datetime.fromtimestamp(required_end_ms/1000).strftime('%Y-%m-%d')})")
            fetch_periods.append((effective_start_ms, required_end_ms, "full"))
            return fetch_periods
        
        # 前段缺失 (effective_start < cached_start)
        if effective_start_ms < cached_start_ms:
            gap_before_ms = cached_start_ms - effective_start_ms
            gap_before_days = gap_before_ms / (24 * 60 * 60 * 1000)
            
            # 檢查是否快取前的缺失結束於上市時間之前
            if listing_time_ms is not None and gap_before_ms > 0:
                if effective_start_ms <= listing_time_ms <= cached_start_ms:
                    logger.info(f"[Cache] {symbol}({exchange}): gap before cache ends before listing time "
                               f"({datetime.fromtimestamp(listing_time_ms/1000).strftime('%Y-%m-%d')}), SKIPPING fetch")
                    # 不補充上市時間之前的數據
                else:
                    logger.info(f"[Cache] {symbol}({exchange}): gap before cache ({gap_before_days:.1f} days), "
                               f"fetching from {datetime.fromtimestamp(effective_start_ms/1000).strftime('%Y-%m-%d')}...")
                    fetch_periods.append((effective_start_ms, cached_start_ms, "before"))
            else:
                logger.info(f"[Cache] {symbol}({exchange}): gap before cache ({gap_before_days:.1f} days), "
                           f"fetching from {datetime.fromtimestamp(effective_start_ms/1000).strftime('%Y-%m-%d')}...")
                fetch_periods.append((effective_start_ms, cached_start_ms, "before"))
        
        # 後段缺失 (required_end > cached_end)
        if required_end_ms > cached_end_ms:
            gap_after_ms = required_end_ms - cached_end_ms
            gap_after_days = gap_after_ms / (24 * 60 * 60 * 1000)
            
            # 如果缺失少於 2 分鐘（120,000 毫秒），認為是時間戳精度誤差，不補充
            MIN_GAP_MS = 120000  # 2 分鐘
            
            if gap_after_ms < MIN_GAP_MS:
                logger.debug(f"[Cache] {symbol}({exchange}): gap after cache 太小 ({gap_after_ms} ms), 跳過補充")
            else:
                logger.info(f"[Cache] {symbol}({exchange}): gap after cache ({gap_after_days:.1f} days), fetching...")
                fetch_periods.append((cached_end_ms, required_end_ms, "after"))
        
        return fetch_periods
    
    async def fetch_klines_async(
        self,
        symbol: str,
        exchange: str,
        start_ms: int,
        end_ms: int,
        session: Optional[aiohttp.ClientSession] = None,
        max_retries: int = 3
    ) -> Optional[pd.DataFrame]:
        """
        非同步獲取 K 線 (支持智能增量更新)
        根據快取覆蓋和 symbol 上市時間決定是否需要補充數據
        
        Args:
            symbol: 交易對
            exchange: 交易所 ('binance' or 'bybit')
            start_ms: 開始時間戳 (毫秒)
            end_ms: 結束時間戳 (毫秒)
            session: aiohttp 會話 (可選)
            max_retries: 最大重試次數
        
        Returns:
            K 線 DataFrame 或 None
        """
        # 1. 檢查快取覆蓋
        is_complete, cached_df = self._check_cache_coverage(
            symbol, exchange, start_ms, end_ms
        )
        
        if is_complete and cached_df is not None:
            # 快取完全覆蓋，直接返回所需範圍的數據
            logger.debug(f"[Cache] {symbol}({exchange}): 使用完整快取")
            # 確保 timestamp 是毫秒整數
            result_df = cached_df[
                (cached_df['timestamp'] >= start_ms) &
                (cached_df['timestamp'] <= end_ms)
            ].reset_index(drop=True)
            
            # 轉換 timestamp 為毫秒整數（如果還不是）
            if len(result_df) > 0 and result_df['timestamp'].dtype != 'int64':
                result_df['timestamp'] = result_df['timestamp'].astype('int64')
            
            # 確保 OHLCV 欄位都是 float64
            if len(result_df) > 0:
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in result_df.columns and result_df[col].dtype != 'float64':
                        result_df[col] = pd.to_numeric(result_df[col], errors='coerce').astype('float64')
            
            return result_df
        
        # 2. 計算缺失的時間段
        cached_start_ms = None
        cached_end_ms = None
        if cached_df is not None and len(cached_df) > 0:
            # 時間戳已經在 _check_cache_coverage 中標準化為毫秒整數
            ts_min = int(cached_df['timestamp'].min())
            ts_max = int(cached_df['timestamp'].max())
            
            cached_start_ms = ts_min
            cached_end_ms = ts_max
        
        fetch_periods = self._calculate_missing_periods(
            symbol, exchange, start_ms, end_ms, cached_start_ms, cached_end_ms
        )
        
        # 3. 抓取缺失的時間段
        all_new_data = []
        for period_start, period_end, period_type in fetch_periods:
            try:
                start_date = datetime.fromtimestamp(period_start/1000).strftime('%Y-%m-%d')
                end_date = datetime.fromtimestamp(period_end/1000).strftime('%Y-%m-%d')
                logger.info(f"[Fetch] {symbol}({exchange}): fetching {period_type} period ({start_date} ~ {end_date})")
            except (ValueError, OSError):
                logger.info(f"[Fetch] {symbol}({exchange}): fetching {period_type} period")
            
            for attempt in range(max_retries):
                try:
                    if exchange == 'binance':
                        df = await self._fetch_binance_klines_paginated(
                            symbol, period_start, period_end, session
                        )
                    elif exchange == 'bybit':
                        df = await self._fetch_bybit_klines_paginated(
                            symbol, period_start, period_end, session
                        )
                    else:
                        df = None
                    
                    if df is not None and len(df) > 0:
                        all_new_data.append(df)
                        logger.info(f"[Fetch] {symbol}({exchange}): fetched {len(df)} records "
                                  f"for {period_type} period")
                        break
                    else:
                        logger.warning(f"[Fetch] {symbol}({exchange}): no data for {period_type} period")
                        break
                        
                except Exception as e:
                    logger.warning(f"[Fetch] {symbol}({exchange}): failed (attempt {attempt+1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # 指數退避
        
        # 4. 合併快取和新數據
        if all_new_data:
            new_df = pd.concat(all_new_data, ignore_index=True)
        else:
            new_df = None
        
        if new_df is not None and cached_df is not None and len(cached_df) > 0:
            # 合併快取和新數據
            combined_df = pd.concat([cached_df, new_df], ignore_index=True)
            # 移除重複 (按 timestamp)
            combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='first')
            combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
        elif new_df is not None:
            combined_df = new_df.sort_values('timestamp').reset_index(drop=True)
        elif cached_df is not None:
            combined_df = cached_df
        else:
            combined_df = None
        
        # 5. 保存更新的快取
        if combined_df is not None and len(combined_df) > 0:
            # 確保時間戳已標準化為毫秒整數
            combined_df = standardize_timestamp_column(combined_df, col='timestamp')
            
            # 確保 OHLCV 欄位都是 float64
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in combined_df.columns:
                    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').astype('float64')
            
            try:
                cache_path = self._get_cache_path(symbol, exchange)
                combined_df.to_parquet(cache_path, index=False, compression='snappy')
                logger.debug(f"[Cache] {symbol}({exchange}): 保存快取 {cache_path.name} ({len(combined_df)} 行)")
            except Exception as e:
                logger.warning(f"[Cache] {symbol}({exchange}): 快取保存失敗: {e}")
            
            # 返回所需範圍的數據
            result_df = combined_df[
                (combined_df['timestamp'] >= start_ms) &
                (combined_df['timestamp'] <= end_ms)
            ].reset_index(drop=True)
            
            # 最終驗證時間戳範圍
            if len(result_df) > 0 and not validate_timestamp_range(result_df['timestamp'].values):
                logger.warning(f"返回的 K 線數據時間戳超出範圍 {symbol}({exchange})")
            
            # 確保 OHLCV 欄位都是 float64
            if len(result_df) > 0:
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in result_df.columns and result_df[col].dtype != 'float64':
                        result_df[col] = pd.to_numeric(result_df[col], errors='coerce').astype('float64')
            
            return result_df
        
        return None
    
    async def _fetch_binance_klines_paginated(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        session: Optional[aiohttp.ClientSession] = None
    ) -> Optional[pd.DataFrame]:
        """
        Binance K 線分頁獲取 (從舊到新)
        """
        url = "https://api.binance.com/api/v3/klines"
        all_data = []
        current_start = start_ms
        
        while current_start < end_ms:
            try:
                params = {
                    'symbol': symbol,
                    'interval': '1m',
                    'startTime': int(current_start),
                    'endTime': int(end_ms),
                    'limit': self.BINANCE_LIMIT
                }
                
                if session is None:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                            else:
                                # 處理不同的 HTTP 狀態碼
                                if resp.status == 400:
                                    # 400 Bad Request - 可能是符號無效或不存在
                                    error_msg = await resp.text()
                                    logger.warning(f"Binance API 400 - Symbol 可能無效或不存在: {symbol} ({error_msg[:100]})")
                                    break  # 直接跳過，不重試
                                elif resp.status in (418, 429):
                                    # 418 I'm a teapot (speed limit) 或 429 Too Many Requests
                                    logger.error(f"Binance rate limit (HTTP {resp.status})，暫停 60 秒...")
                                    await asyncio.sleep(60)
                                    break  # 暫停後放棄這個請求
                                else:
                                    logger.warning(f"Binance API 錯誤 HTTP {resp.status}")
                                    break
                else:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                        else:
                            # 處理不同的 HTTP 狀態碼
                            if resp.status == 400:
                                # 400 Bad Request - 可能是符號無效或不存在
                                error_msg = await resp.text()
                                logger.warning(f"Binance API 400 - Symbol 可能無效或不存在: {symbol} ({error_msg[:100]})")
                                break  # 直接跳過，不重試
                            elif resp.status in (418, 429):
                                # 418 I'm a teapot (speed limit) 或 429 Too Many Requests
                                logger.error(f"Binance rate limit (HTTP {resp.status})，暫停 60 秒...")
                                await asyncio.sleep(60)
                                break  # 暫停後放棄這個請求
                            else:
                                logger.warning(f"Binance API 錯誤 HTTP {resp.status}")
                                break
                
                if not data:
                    logger.debug(f"{symbol} 無更多數據")
                    break
                
                all_data.extend(data)
                
                # 更新下一批的開始時間
                last_timestamp = data[-1][0]
                current_start = last_timestamp + 1
                
                # 避免 rate limit
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.warning(f"Binance 單次請求失敗: {e}")
                break
        
        if not all_data:
            return None
        
        # 轉換為 DataFrame
        df = pd.DataFrame(all_data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        # 轉換數據類型
        df['timestamp'] = df['timestamp'].astype('int64')
        df['open'] = df['open'].astype('float64')
        df['high'] = df['high'].astype('float64')
        df['low'] = df['low'].astype('float64')
        df['close'] = df['close'].astype('float64')
        df['volume'] = df['volume'].astype('float64')
        
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        
        return df
    
    async def _fetch_bybit_klines_paginated(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        session: Optional[aiohttp.ClientSession] = None
    ) -> Optional[pd.DataFrame]:
        """
        Bybit K 線分頁獲取 (從新到舊，需要反轉)
        """
        url = "https://api.bybit.com/v5/market/kline"
        all_data = []
        current_end = end_ms  # 從最新時間開始往回
        
        while current_end > start_ms:
            try:
                params = {
                    'category': 'linear',
                    'symbol': symbol,
                    'interval': '1',
                    'end': str(int(current_end)),
                    'limit': str(self.BYBIT_LIMIT)
                }
                
                if session is None:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                            if resp.status == 200:
                                result = await resp.json()
                                data = result.get('result', {}).get('list', [])
                            else:
                                logger.warning(f"Bybit API 錯誤: {resp.status}")
                                if resp.status == 429:
                                    logger.error(f"已達 Bybit rate limit，暫停 30 秒...")
                                    await asyncio.sleep(30)
                                break
                else:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status == 200:
                            result = await resp.json()
                            data = result.get('result', {}).get('list', [])
                        else:
                            logger.warning(f"Bybit API 錯誤: {resp.status}")
                            if resp.status == 429:
                                logger.error(f"已達 Bybit rate limit，暫停 30 秒...")
                                await asyncio.sleep(30)
                            break
                
                if not data:
                    logger.debug(f"{symbol} 無更多數據")
                    break
                
                all_data.extend(data)
                
                # 如果已經回到 start_ms 就停止
                current_end = int(data[-1][0]) - 1
                if current_end <= start_ms:
                    break
                
                # 避免 rate limit
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.warning(f"Bybit 單次請求失敗: {e}")
                break
        
        if not all_data:
            return None
        
        # 轉換為 DataFrame (Bybit 返回 7 列)
        df = pd.DataFrame(all_data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'
        ])
        
        # 轉換數據類型
        df['timestamp'] = df['timestamp'].astype('int64')
        df['open'] = df['open'].astype('float64')
        df['high'] = df['high'].astype('float64')
        df['low'] = df['low'].astype('float64')
        df['close'] = df['close'].astype('float64')
        df['volume'] = df['volume'].astype('float64')
        
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        
        # ⚠️ Bybit 返回新→舊排序，需要反轉為舊→新
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        return df
    
    async def _fetch_with_semaphore(
        self,
        symbol: str,
        exchange: str,
        start_ms: int,
        end_ms: int,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore
    ) -> Tuple[str, str, Optional[pd.DataFrame]]:
        """帶 Semaphore 的獲取 (控制並發)"""
        async with semaphore:
            df = await self.fetch_klines_async(
                symbol, exchange, start_ms, end_ms, session
            )
            return symbol, exchange, df
    
    async def _load_listing_times(
        self,
        tradable_symbols: List[str]
    ) -> None:
        """
        異步加載所有 symbols 的上市時間
        應用與 opportunity_analysis 相同的邏輯 (使用 async with context manager)
        失敗不會中斷流程，只是無法利用上市時間優化
        """
        logger.info(f"📅 [Phase 0] 加載 {len(tradable_symbols)} 個 symbols 的上市時間...")
        
        # 初始化為空
        self.listing_times = {s: {'binance': None, 'bybit': None} for s in tradable_symbols}
        
        try:
            from data_collector.binance_client import BinanceClient
            from data_collector.bybit_client import BybitClient
            
            # 使用 context manager 確保 session 被正確初始化和關閉 (與 analysis 相同)
            async with BinanceClient() as bn_client, BybitClient() as by_client:
                
                # 嘗試加載 Binance 上市時間
                try:
                    bn_listing_times = await bn_client.get_all_symbols_listing_times()
                    if isinstance(bn_listing_times, dict):
                        for symbol, bn_time in bn_listing_times.items():
                            if symbol in self.listing_times:
                                self.listing_times[symbol]['binance'] = bn_time
                        logger.debug(f"✓ 加載 {len(bn_listing_times)} 個 Binance 上市時間")
                except Exception as e:
                    logger.warning(f"加載 Binance 上市時間失敗: {e}")
                
                # 嘗試加載 Bybit 上市時間 (逐個請求，但錯誤不中斷)
                try:
                    loaded_count = 0
                    failed_count = 0
                    no_data_count = 0
                    
                    # 用 gather with return_exceptions 避免單個失敗影響整體
                    by_tasks = [by_client.get_symbol_listing_time(symbol) for symbol in tradable_symbols]
                    by_listing_times = await asyncio.gather(*by_tasks, return_exceptions=True)
                    
                    for symbol, by_time in zip(tradable_symbols, by_listing_times):
                        if isinstance(by_time, Exception):
                            # 異常，跳過
                            failed_count += 1
                            logger.debug(f"Bybit {symbol}: 加載失敗 ({type(by_time).__name__}: {by_time})")
                        elif by_time is not None:
                            self.listing_times[symbol]['bybit'] = by_time
                            loaded_count += 1
                        else:
                            no_data_count += 1
                            logger.debug(f"Bybit {symbol}: 無上市時間數據 (不在 Bybit 上市)")
                    
                    logger.debug(f"✓ 加載 {loaded_count} 個 Bybit 上市時間 "
                               f"({failed_count} 個異常, {no_data_count} 個無數據)")
                    
                except Exception as e:
                    logger.warning(f"加載 Bybit 上市時間失敗: {e}")
            
            # 統計成功加載的數量
            bn_loaded = sum(1 for times in self.listing_times.values() if times.get('binance') is not None)
            by_loaded = sum(1 for times in self.listing_times.values() if times.get('bybit') is not None)
            both_loaded = sum(1 for times in self.listing_times.values() 
                            if times.get('binance') is not None and times.get('bybit') is not None)
            
            logger.info(f"✓ [Phase 0] 上市時間加載完成")
            logger.info(f"   Binance: {bn_loaded}/{len(tradable_symbols)} ✓")
            logger.info(f"   Bybit:   {by_loaded}/{len(tradable_symbols)} ✓")
            logger.info(f"   雙邊都有: {both_loaded}/{len(tradable_symbols)}")
            
            # 如果某些 symbols 只有單邊上市時間，日誌會顯示
            if bn_loaded > 0 or by_loaded > 0:
                logger.debug(f"   將使用上市時間進行智能補充")
            
            # 診斷：顯示缺失 Bybit 上市時間的 symbols
            if by_loaded < len(tradable_symbols):
                missing_by = [s for s, times in self.listing_times.items() 
                            if times.get('bybit') is None]
                if len(missing_by) <= 10:
                    logger.debug(f"   Bybit 缺失上市時間: {missing_by}")
                else:
                    logger.debug(f"   Bybit 缺失上市時間: {len(missing_by)} 個 symbols")
            
        except Exception as e:
            logger.warning(f"上市時間加載流程異常: {e}，將繼續不使用上市時間限制")
    
    async def fetch_all_klines(
        self,
        tradable_symbols: List[str],
        config,
        exchanges: List[str] = ['binance', 'bybit']
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        非同步並行獲取所有 K 線 (含進度條和智能增量更新)
        支持根據快取覆蓋和 symbol 上市時間自動補充缺失數據
        
        Args:
            tradable_symbols: 可交易的交易對列表
            config: 回測配置 (包含時間範圍)
            exchanges: 交易所列表
        
        Returns:
            {symbol: {exchange: DataFrame}}
        """
        start_ms, end_ms = config.get_time_range()
        
        print("\n" + "="*80)
        logger.info(f"🚀 開始並行獲取 {len(tradable_symbols)} 個 symbol 的 K 線 (智能增量模式)")
        logger.info(f"📅 時間範圍: {datetime.fromtimestamp(start_ms/1000)} 到 {datetime.fromtimestamp(end_ms/1000)}")
        logger.info(f"💾 快取目錄: {self.cache_dir}")
        print("="*80 + "\n")
        
        # Phase 0: 加載上市時間
        await self._load_listing_times(tradable_symbols)
        
        tasks = []
        task_info = []
        
        # 建立任務列表
        async with aiohttp.ClientSession() as session:
            for symbol in tradable_symbols:
                for exchange in exchanges:
                    # 選擇正確的 Semaphore
                    semaphore = self.bn_semaphore if exchange == 'binance' else self.by_semaphore
                    
                    task = self._fetch_with_semaphore(
                        symbol, exchange, start_ms, end_ms, session, semaphore
                    )
                    tasks.append(task)
                    task_info.append((symbol, exchange))
            
            # 並行執行 (帶進度條)
            total_tasks = len(tasks)
            print(f"📊 並行執行 {total_tasks} 個任務 ({len(tradable_symbols)} symbols × {len(exchanges)} exchanges)...")
            print("⏳ 進度條:\n")
            
            start_time = time.time()
            
            # 使用 asyncio.gather 執行，同時使用 tqdm 顯示進度
            # 創建進度條
            pbar = tqdm(total=total_tasks, desc="K線下載進度", unit="task")
            
            # 創建包裝後的任務，每個任務完成後更新進度條
            async def task_with_progress(task):
                try:
                    result = await task
                    pbar.update(1)
                    return result
                except Exception as e:
                    pbar.update(1)
                    logger.error(f"任務異常: {e}")
                    return None, None, None
            
            wrapped_tasks = [task_with_progress(task) for task in tasks]
            
            # 執行所有任務 (改為 return_exceptions=False 讓異常被 task_with_progress 的 try-except 捕捉)
            results = await asyncio.gather(*wrapped_tasks, return_exceptions=False)
            pbar.close()
            
            # 整理結果
            klines_dict = {}
            success_count = 0
            none_count = 0
            error_count = 0
            
            for i, result in enumerate(results):
                if result is None:
                    none_count += 1
                    continue
                
                # 檢查是否是異常對象（雖然不應該有，但以防萬一）
                if isinstance(result, Exception):
                    logger.error(f"結果 {i}: 異常對象 - {result}")
                    error_count += 1
                    continue
                
                if not isinstance(result, tuple) or len(result) != 3:
                    logger.warning(f"結果 {i}: 格式不對，type={type(result)}, len={len(result) if isinstance(result, (list, tuple)) else 'N/A'}")
                    error_count += 1
                    continue
                
                symbol, exchange, df = result
                
                # 無論 df 是否為 None，都要保持 {symbol: {exchange: df}} 的結構
                if symbol not in klines_dict:
                    klines_dict[symbol] = {}
                
                klines_dict[symbol][exchange] = df
                
                if df is None:
                    none_count += 1
                else:
                    success_count += 1
            
            elapsed = time.time() - start_time
            print(f"\n✅ 完成")
            print(f"   成功: {success_count}/{total_tasks}")
            print(f"   無數據 (None): {none_count}")
            print(f"   錯誤: {error_count}")
            print(f"   耗時: {elapsed:.1f} 秒 ({total_tasks/elapsed:.1f} tasks/sec)")
            print("="*80 + "\n")
            
            if not klines_dict:
                logger.warning("⚠️ 警告: klines_dict 為空，沒有 K-line 數據被加載")
            else:
                logger.info(f"✓ 加載完成: {len(klines_dict)} 個 symbols，{success_count} 個數據集")
            
            return klines_dict
