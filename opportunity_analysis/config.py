"""Configuration for funding interval arbitrage research."""
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# API Endpoints
BINANCE_BASE_URL = "https://fapi.binance.com"
BYBIT_BASE_URL = "https://api.bybit.com"

# API Rate Limits (requests per time window)
BINANCE_RATE_LIMIT = 500  # per 5 minutes
BYBIT_RATE_LIMIT = 600  # per 5 seconds

# Analysis Parameters
ANALYSIS_DAYS = int(os.getenv("ANALYSIS_DAYS", "90"))
LOOKBACK_DAYS = ANALYSIS_DAYS

# Valid funding intervals in seconds
VALID_INTERVALS = [3600, 14400, 28800]  # 1h, 4h, 8h

# Mismatch threshold (seconds)
MISMATCH_THRESHOLD = 3600  # 1 hour difference

# Generate timestamp for output directory
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

# Output Directory with timestamp
BASE_OUTPUT_DIR = Path(os.getenv("BASE_OUTPUT_DIR", "/home/james/research_output/funding_interval_arb/existence_analysis"))
OUTPUT_DIR = BASE_OUTPUT_DIR / TIMESTAMP
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Data Directory (funding timeline cache)
DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/funding_cache"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Plots Directory
PLOTS_DIR = OUTPUT_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# API Keys (optional for public endpoints)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")

# Retry Configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
BACKOFF_FACTOR = 2  # exponential backoff

# Data Quality Thresholds
MIN_DATA_COMPLETENESS = 0.8  # 80% of expected records
MAX_FUNDING_RATE = 0.005  # ±0.5%
MAX_TIME_GAP = 86400  # 24 hours in seconds

