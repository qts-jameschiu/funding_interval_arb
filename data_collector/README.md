# Data Collector for Funding Interval Arbitrage

## 🎯 概述

這個模塊提供共用的數據收集功能，用於從 Binance 和 Bybit 獲取 funding rate 歷史數據。

可被多個研究項目（如 opportunity_analysis, mismatch_pattern_analysis 等）使用。

## 📦 模塊組成

- **binance_client.py**: Binance Futures API 客戶端
  - 獲取交易所信息（symbols、intervals）
  - 批量獲取 funding rate 歷史
  - 數據處理和標準化

- **bybit_client.py**: Bybit V5 API 客戶端
  - 獲取交易所信息（instruments、intervals）
  - 分頁獲取 funding rate 歷史（支持突破 200 筆記錄限制）
  - 數據處理和標準化

- **utils.py**: 工具函數
  - 時間戳轉換（`timestamp_to_datetime`, `datetime_to_timestamp`）
  - 時間範圍計算（`get_time_range`）
  - Interval 轉換（`interval_to_hours`, `standardize_interval`）
  - Symbol 映射（`get_all_symbols_from_exchanges`, `create_symbol_mapping`）
  - 數據完整性驗證

## 🚀 使用方式

### 基本導入

```python
from data_collector.binance_client import BinanceClient
from data_collector.bybit_client import BybitClient
from data_collector.utils import get_time_range, get_all_symbols_from_exchanges
```

### 數據收集示例

```python
import asyncio
from data_collector.binance_client import BinanceClient
from data_collector.bybit_client import BybitClient

async def collect_data(symbol, start_time, end_time):
    async with BinanceClient() as bn_client, BybitClient() as by_client:
        # 獲取 Binance 數據
        bn_data = await bn_client.get_funding_rate_history(symbol, start_time, end_time)
        
        # 獲取 Bybit 數據
        by_data = await by_client.get_funding_rate_history(symbol, start_time, end_time)
        
        return bn_data, by_data

# 運行
asyncio.run(collect_data('BTCUSDT', 1700000000000, 1700100000000))
```

## 📋 主要功能

### 動態 Symbol 發現
自動從交易所獲取所有 USDT perpetual 交易對，支持 symbol 名稱映射（如 1000PEPEUSDT ↔ PEPEUSDT）。

```python
symbol_mapping = await get_all_symbols_from_exchanges()
# 返回: {'BTCUSDT': {'binance': 'BTCUSDT', 'bybit': 'BTCUSDT'}, ...}
```

### 時間對齐
將時間戳對齐到整點小時邊界，用於時間序列分析。

```python
from data_collector.utils import get_time_range
start_time, end_time = get_time_range(90)  # 獲取過去 90 天的時間範圍
```

### Bybit API 分頁
自動處理 Bybit 200 筆記錄限制，通過分批獲取實現完整數據收集。

```python
# 自動分頁，無需手動處理
by_data = await by_client.get_funding_rate_history(symbol, start_time, end_time)
```

### 重試機制
集成指數退避重試機制，自動處理 API 限制和臨時錯誤。

```python
# 自動重試，最多 3 次，使用指數退避
```

## ⚙️ 配置

通過環境變數配置 API 認證（可選）：

```bash
export BINANCE_API_KEY=your_key
export BINANCE_API_SECRET=your_secret
export BYBIT_API_KEY=your_key
export BYBIT_API_SECRET=your_secret
```

## 📊 數據格式

### Funding Rate 數據
```python
{
    'symbol': 'BTCUSDT',
    'fundingTime': 1700000000000,        # 毫秒時間戳
    'fundingRate': 0.0001,               # Funding rate
    'datetime': '2024-11-15 08:00:00',   # ISO 格式時間
    'interval': 28800,                   # 秒（時間差）
    'interval_hours': 8                  # 小時（四捨五入）
}
```

## 📈 性能特性

- ✅ **異步並行**: 使用 asyncio 並行獲取多個 symbol 的數據
- ✅ **批量處理**: 動態調整批次大小，優化 API 調用
- ✅ **智能分頁**: Bybit 自動分頁，突破 200 筆記錄限制
- ✅ **容錯機制**: 指數退避重試，自動處理 API 限制
- ✅ **日誌記錄**: 詳細的日誌，便於調試和監控

## 🔌 API 端點

### Binance
- 交易所信息: `/fapi/v1/exchangeInfo`
- Funding Rate: `/fapi/v1/fundingRate`
- Premium Index: `/fapi/v1/premiumIndex`

### Bybit
- 交易所信息: `/v5/market/instruments`
- Funding Rate: `/v5/market/funding/history`

## 📝 注意事項

- 時間戳以毫秒為單位
- Funding intervals 標準化為整數小時（1, 2, 4, 8 等）
- 自動處理時區問題，使用 UTC 時間
- Symbol 映射處理了交易所命名差異（如 1000PEPEUSDT）

## 🛠️ 開發

### 添加新的交易所
1. 創建新的 `<exchange>_client.py` 文件
2. 實現 `BaseClient` 接口
3. 添加到 `__init__.py` 中
4. 更新 `utils.py` 中的 symbol 映射函數

### 依賴
- aiohttp - 異步 HTTP 客戶端
- requests - 同步 HTTP（用於初始化）
- python-dotenv - 環境變數支持

## 📄 許可證

MIT

---

**最後更新**: 2025-11-03  
**狀態**: ✅ 生產就緒
