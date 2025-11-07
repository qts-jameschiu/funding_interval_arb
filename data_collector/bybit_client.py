"""Bybit API client for funding rate data collection."""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import aiohttp

from .config import BYBIT_BASE_URL, BYBIT_RATE_LIMIT
from .utils import fetch_with_retry, timestamp_to_datetime

logger = logging.getLogger(__name__)


class BybitClient:
    """Client for Bybit V5 API."""
    
    def __init__(self):
        self.base_url = BYBIT_BASE_URL
        self.rate_limit = BYBIT_RATE_LIMIT
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_instruments_info(self) -> Optional[Dict[str, Any]]:
        """Get all available linear perpetual symbols and their funding intervals."""
        url = f"{self.base_url}/v5/market/instruments-info"
        params = {
            'category': 'linear'
        }
        
        logger.info("Fetching Bybit instruments info...")
        
        all_symbols = []
        cursor = None
        
        while True:
            if cursor:
                params['cursor'] = cursor
            
            data = await fetch_with_retry(self.session, url, params)
            
            if not data or data.get('retCode') != 0:
                logger.error(f"Failed to fetch Bybit instruments: {data}")
                break
            
            result = data.get('result', {})
            symbols_data = result.get('list', [])
            
            # Filter USDT perpetuals
            for s in symbols_data:
                if (s.get('quoteCoin') == 'USDT' 
                    and s.get('contractType') == 'LinearPerpetual'
                    and s.get('status') == 'Trading'):
                    all_symbols.append({
                        'symbol': s['symbol'],
                        'fundingInterval': int(s.get('fundingInterval', 480))  # in minutes
                    })
            
            # Check if there's more data
            cursor = result.get('nextPageCursor')
            if not cursor:
                break
            
            await asyncio.sleep(0.1)
        
        logger.info(f"Found {len(all_symbols)} Bybit USDT perpetual symbols")
        return {'symbols': all_symbols}
    
    async def get_symbol_listing_time(self, symbol: str) -> Optional[int]:
        """
        Get listing time for a symbol from Bybit's instrument info.
        
        Uses the /v5/market/instruments-info endpoint to get the official launchTime.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
        
        Returns:
            Listing time in milliseconds, or None if not found
        """
        url = f"{self.base_url}/v5/market/instruments-info"
        
        try:
            params = {
                'category': 'linear',
                'symbol': symbol
            }
            
            logger.debug(f"Bybit {symbol}: querying instrument info for launch time...")
            data = await fetch_with_retry(self.session, url, params)
            
            if data and data.get('retCode') == 0:
                result = data.get('result', {})
                instruments = result.get('list', [])
                
                if instruments:
                    instrument = instruments[0]
                    launch_time_str = instrument.get('launchTime')
                    
                    if launch_time_str:
                        # Bybit returns launchTime as a string representing milliseconds timestamp
                        try:
                            listing_time = int(launch_time_str)
                            logger.debug(f"Bybit {symbol}: launch time = {listing_time}")
                            return listing_time
                        except (ValueError, TypeError):
                            logger.warning(f"Bybit {symbol}: could not parse launchTime as integer ({launch_time_str})")
            else:
                logger.warning(f"Bybit {symbol}: failed to query instrument info ({data})")
        except Exception as e:
            logger.warning(f"Bybit {symbol}: error fetching listing time ({e})")
        
        return None
    
    async def get_funding_rate_history(
        self,
        symbol: str,
        start_time: int,
        end_time: int,
        limit: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Get historical funding rate for a symbol.
        
        Bybit API limitation: max 200 records per request, no pagination cursor.
        Strategy: Fetch in batches by narrowing the time window from newest to oldest.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            limit: Max records per request (default 200, max 200)
        
        Returns:
            List of funding rate records sorted by fundingRateTimestamp (oldest first)
        """
        url = f"{self.base_url}/v5/market/funding/history"
        all_data = []
        
        logger.info(f"Fetching Bybit funding history for {symbol}...")
        
        # Bybit returns data in descending order (newest first)
        # Strategy: Start from end_time and work backwards to start_time
        current_end = end_time
        max_iterations = 100  # Increased safety limit
        iteration = 0
        
        while current_end > start_time and iteration < max_iterations:
            params = {
                'category': 'linear',
                'symbol': symbol,
                'startTime': start_time,
                'endTime': current_end,
                'limit': limit
            }
            
            data = await fetch_with_retry(self.session, url, params)
            
            if not data or data.get('retCode') != 0:
                logger.warning(f"Failed to fetch funding history for {symbol}: {data}")
                break
            
            result = data.get('result', {})
            funding_list = result.get('list', [])
            
            if not funding_list:
                # No more data - reached the start time
                logger.info(f"Reached start of available data for {symbol}")
                break
            
            all_data.extend(funding_list)
            
            # Get the oldest timestamp in this batch
            # Bybit uses 'fundingRateTimestamp' field
            oldest_time = min(int(item['fundingRateTimestamp']) for item in funding_list)
            
            # If we got less than limit records, we've reached the start
            if len(funding_list) < limit:
                logger.info(f"Got {len(funding_list)} records (< {limit}), reached start of data")
                break
            
            # Check if we've reached the start time
            if oldest_time <= start_time:
                logger.info(f"Oldest record ({oldest_time}) <= start_time ({start_time}), all data fetched")
                break
            
            # Move the end time to just before the oldest record we got
            # This ensures we fetch non-overlapping data in the next iteration
            current_end = oldest_time - 1
            iteration += 1
            
            logger.info(f"Iteration {iteration}: fetched {len(funding_list)} records, oldest: {oldest_time}, continuing...")
            
            # Small delay to respect rate limits
            await asyncio.sleep(0.1)
        
        logger.info(f"Completed {iteration + 1} API calls for {symbol}")
        
        # Remove duplicates and filter to time range
        seen = set()
        unique_data = []
        for item in all_data:
            funding_time = int(item['fundingRateTimestamp'])
            # Ensure the record is within the requested time range
            if funding_time not in seen and start_time <= funding_time <= end_time:
                seen.add(funding_time)
                unique_data.append(item)
        
        # Sort by fundingRateTimestamp (oldest first)
        unique_data.sort(key=lambda x: int(x['fundingRateTimestamp']))
        
        # Log summary
        if unique_data:
            first_time = int(unique_data[0]['fundingRateTimestamp'])
            last_time = int(unique_data[-1]['fundingRateTimestamp'])
            from datetime import datetime
            first_dt = datetime.fromtimestamp(first_time / 1000)
            last_dt = datetime.fromtimestamp(last_time / 1000)
            time_span_days = (last_time - first_time) / 1000 / 86400
            logger.info(f"Fetched {len(unique_data)} unique records for {symbol}")
            logger.info(f"  Time range: {first_dt} to {last_dt} ({time_span_days:.1f} days)")
            logger.info(f"  Expected days: {(end_time - start_time) / 1000 / 86400:.1f}")
            logger.info(f"  Coverage: {(time_span_days / ((end_time - start_time) / 1000 / 86400)) * 100:.1f}%")
        else:
            logger.warning(f"No data found for {symbol} in time range")
        
        return unique_data
    
    async def get_current_tickers(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get current ticker information including funding rate.
        """
        url = f"{self.base_url}/v5/market/tickers"
        params = {
            'category': 'linear',
            'symbol': symbol
        }
        
        data = await fetch_with_retry(self.session, url, params)
        
        if data and data.get('retCode') == 0:
            result = data.get('result', {})
            ticker_list = result.get('list', [])
            if ticker_list:
                ticker = ticker_list[0]
                return {
                    'symbol': ticker.get('symbol'),
                    'lastPrice': float(ticker.get('lastPrice', 0)),
                    'indexPrice': float(ticker.get('indexPrice', 0)),
                    'fundingRate': float(ticker.get('fundingRate', 0)),
                    'nextFundingTime': ticker.get('nextFundingTime'),
                }
        
        return None
    
    def process_funding_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process raw funding data into standardized format.
        Calculate intervals between consecutive funding events.
        """
        if not raw_data:
            return []
        
        # Sort by funding time (Bybit returns newest first, so reverse)
        sorted_data = sorted(raw_data, key=lambda x: int(x['fundingRateTimestamp']))
        
        processed = []
        for i, record in enumerate(sorted_data):
            funding_time = int(record['fundingRateTimestamp'])
            
            processed_record = {
                'symbol': record['symbol'],
                'fundingTime': funding_time,
                'fundingRate': float(record['fundingRate']),
                'datetime': timestamp_to_datetime(funding_time).isoformat()
            }
            
            # Calculate interval from previous funding
            if i > 0:
                prev_time = int(sorted_data[i-1]['fundingRateTimestamp'])
                interval_seconds = (funding_time - prev_time) / 1000
                processed_record['interval'] = int(interval_seconds)
                # Round to nearest integer hour to avoid floating point precision issues
                processed_record['interval_hours'] = round(interval_seconds / 3600)
            else:
                processed_record['interval'] = None
                processed_record['interval_hours'] = None
            
            processed.append(processed_record)
        
        return processed

