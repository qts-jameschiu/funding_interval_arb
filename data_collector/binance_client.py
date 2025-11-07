"""Binance API client for funding rate data collection."""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import aiohttp

from .config import BINANCE_BASE_URL, BINANCE_RATE_LIMIT
from .utils import fetch_with_retry, timestamp_to_datetime

logger = logging.getLogger(__name__)


class BinanceClient:
    """Client for Binance Futures API."""
    
    def __init__(self):
        self.base_url = BINANCE_BASE_URL
        self.rate_limit = BINANCE_RATE_LIMIT
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_exchange_info(self) -> Optional[Dict[str, Any]]:
        """Get all available linear perpetual symbols."""
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        
        logger.info("Fetching Binance exchange info...")
        data = await fetch_with_retry(self.session, url)
        
        if data:
            # Filter only PERPETUAL contracts with USDT
            symbols = [
                s['symbol'] for s in data.get('symbols', [])
                if s.get('contractType') == 'PERPETUAL' 
                and s.get('quoteAsset') == 'USDT'
                and s.get('status') == 'TRADING'
            ]
            logger.info(f"Found {len(symbols)} Binance USDT perpetual symbols")
            return {'symbols': symbols}
        
        return None
    
    async def get_all_symbols_listing_times(self) -> Dict[str, Optional[int]]:
        """
        Get listing times (onboardDate) for all USDT-M perpetual symbols in one API call.
        
        This is more efficient than calling get_symbol_listing_time for each symbol.
        
        Returns:
            Dictionary mapping symbol name to listing time in milliseconds
        """
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        
        try:
            data = await fetch_with_retry(self.session, url)
            
            if data:
                listing_times = {}
                all_symbols = data.get('symbols', [])
                
                # Filter for USDT-M perpetual contracts and trading status
                for symbol_info in all_symbols:
                    # Only include PERPETUAL contracts with USDT quote asset and TRADING status
                    if (symbol_info.get('contractType') == 'PERPETUAL' 
                        and symbol_info.get('quoteAsset') == 'USDT'
                        and symbol_info.get('status') == 'TRADING'):
                        
                        symbol = symbol_info.get('symbol')
                        if symbol:
                            # Try onboardDate first (for futures)
                            listing_time = symbol_info.get('onboardDate')
                            if listing_time:
                                listing_times[symbol] = int(listing_time)
                            else:
                                # Fallback to listDate if available
                                list_date = symbol_info.get('listDate')
                                if list_date:
                                    listing_times[symbol] = int(list_date)
                                else:
                                    listing_times[symbol] = None
                
                logger.debug(f"Binance: fetched listing times for {len(listing_times)} USDT-M perpetual symbols")
                return listing_times
        except Exception as e:
            logger.warning(f"Binance: error fetching all listing times ({e})")
        
        return {}
    
    async def get_symbol_listing_time(self, symbol: str) -> Optional[int]:
        """
        Get the listing time (onboardDate) for a single symbol in milliseconds.
        
        Note: For efficiency, consider using get_all_symbols_listing_times() instead
        if you need listing times for multiple symbols.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
        
        Returns:
            Listing time in milliseconds, or None if not found
        """
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        params = {'symbol': symbol}
        
        try:
            data = await fetch_with_retry(self.session, url, params)
            
            if data:
                symbols = data.get('symbols', [])
                if symbols:
                    # Try onboardDate first (for futures)
                    listing_time = symbols[0].get('onboardDate')
                    if listing_time:
                        logger.debug(f"Binance {symbol}: listing time from onboardDate = {listing_time}")
                        return int(listing_time)
                    
                    # Fallback to other fields if available
                    list_date = symbols[0].get('listDate')
                    if list_date:
                        logger.debug(f"Binance {symbol}: listing time from listDate = {list_date}")
                        return int(list_date)
        except Exception as e:
            logger.warning(f"Binance {symbol}: error fetching listing time ({e})")
        
        return None
    
    async def get_funding_rate_history(
        self,
        symbol: str,
        start_time: int,
        end_time: int,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get historical funding rate for a symbol.
        
        Binance supports up to 1000 records per request.
        Strategy: Fetch in batches from start_time to end_time.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            limit: Max records per request (default 1000, max 1000)
        
        Returns:
            List of funding rate records
        """
        url = f"{self.base_url}/fapi/v1/fundingRate"
        all_data = []
        current_start = start_time
        iteration = 0
        
        logger.info(f"Fetching Binance funding history for {symbol}...")
        logger.info(f"  Time range: {start_time} to {end_time}")
        
        max_iterations = 100  # Safety limit
        
        while current_start < end_time and iteration < max_iterations:
            params = {
                'symbol': symbol,
                'startTime': current_start,
                'endTime': end_time,
                'limit': limit
            }
            
            data = await fetch_with_retry(self.session, url, params)
            
            if not data:
                logger.warning(f"No data returned for {symbol}")
                break
            
            if len(data) == 0:
                logger.info(f"Reached end of data for {symbol}")
                break
            
            all_data.extend(data)
            
            # Update start time for next batch
            last_funding_time = data[-1]['fundingTime']
            if last_funding_time >= current_start:
                current_start = last_funding_time + 1
            else:
                logger.warning(f"Unexpected data order for {symbol}")
                break
            
            iteration += 1
            logger.info(f"Iteration {iteration}: fetched {len(data)} records, latest: {last_funding_time}")
            
            # Rate limiting: small delay between requests
            await asyncio.sleep(0.2)
            
            # If we got less than limit, we've reached the end
            if len(data) < limit:
                logger.info(f"Got {len(data)} records (< {limit}), reached end of data")
                break
        
        # Log summary
        logger.info(f"Completed {iteration} API calls for {symbol}")
        
        if all_data:
            first_time = all_data[0]['fundingTime']
            last_time = all_data[-1]['fundingTime']
            from datetime import datetime
            first_dt = datetime.fromtimestamp(first_time / 1000)
            last_dt = datetime.fromtimestamp(last_time / 1000)
            time_span_days = (last_time - first_time) / 1000 / 86400
            expected_days = (end_time - start_time) / 1000 / 86400
            coverage = (time_span_days / expected_days * 100) if expected_days > 0 else 0
            
            logger.info(f"Fetched {len(all_data)} total records for {symbol}")
            logger.info(f"  Time range: {first_dt} to {last_dt} ({time_span_days:.1f} days)")
            logger.info(f"  Expected days: {expected_days:.1f}")
            logger.info(f"  Coverage: {coverage:.1f}%")
            
            if coverage < 90:
                logger.warning(f"⚠️  Data coverage is only {coverage:.1f}% for {symbol}")
        else:
            logger.warning(f"No data found for {symbol}")
        
        return all_data
    
    async def get_current_funding_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get current funding rate and next funding time.
        This endpoint provides the current interval information.
        """
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        params = {'symbol': symbol}
        
        data = await fetch_with_retry(self.session, url, params)
        
        if data:
            return {
                'symbol': data.get('symbol'),
                'markPrice': float(data.get('markPrice', 0)),
                'indexPrice': float(data.get('indexPrice', 0)),
                'lastFundingRate': float(data.get('lastFundingRate', 0)),
                'nextFundingTime': data.get('nextFundingTime'),
                'time': data.get('time')
            }
        
        return None
    
    def process_funding_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process raw funding data into standardized format.
        Calculate intervals between consecutive funding events.
        """
        if not raw_data:
            return []
        
        # Sort by funding time
        sorted_data = sorted(raw_data, key=lambda x: x['fundingTime'])
        
        processed = []
        for i, record in enumerate(sorted_data):
            processed_record = {
                'symbol': record['symbol'],
                'fundingTime': record['fundingTime'],
                'fundingRate': float(record['fundingRate']),
                'datetime': timestamp_to_datetime(record['fundingTime']).isoformat()
            }
            
            # Calculate interval from previous funding
            if i > 0:
                interval_seconds = (record['fundingTime'] - sorted_data[i-1]['fundingTime']) / 1000
                processed_record['interval'] = int(interval_seconds)
                # Round to nearest integer hour to avoid floating point precision issues
                processed_record['interval_hours'] = round(interval_seconds / 3600)
            else:
                processed_record['interval'] = None
                processed_record['interval_hours'] = None
            
            processed.append(processed_record)
        
        return processed

