"""Configuration for data collector module."""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Base URLs
BINANCE_BASE_URL = "https://fapi.binance.com"
BYBIT_BASE_URL = "https://api.bybit.com"

# Rate Limits (requests per minute)
BINANCE_RATE_LIMIT = 1200
BYBIT_RATE_LIMIT = 600

# API Keys (from environment variables, optional)
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', '')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET', '')
BYBIT_API_KEY = os.getenv('BYBIT_API_KEY', '')
BYBIT_API_SECRET = os.getenv('BYBIT_API_SECRET', '')

# Retry Configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
BACKOFF_FACTOR = 2

# API Request Configuration
REQUEST_TIMEOUT = 30  # seconds
