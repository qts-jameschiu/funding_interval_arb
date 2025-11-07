# Funding Interval Arbitrage - Backtest Architecture

## 📋 目錄

1. [概述](#概述)
2. [套利策略](#套利策略)
3. [系統架構](#系統架構)
4. [設定系統](#設定系統)
5. [執行流程](#執行流程)
6. [數據流](#數據流)
7. [核心模塊](#核心模塊)
8. [結果輸出](#結果輸出)
9. [性能考量](#性能考量)

---

## 🎯 概述

### 目標
基於已識別的 **funding interval mismatch** 機會，進行實證回測，評估套利策略的實際收益。

### 核心邏輯
```
1. 運行存在性分析（如需要）
   ↓
2. 篩選 tradable=True 的時間點和 symbol
   ↓
3. 獲取該時間點前後 t 分鐘的 1 分鐘 K 線
   ↓
4. 計算 Entry VWAP 和 Exit VWAP
   ↓
5. 根據 bybit_pay 和 binance_pay 判斷交易方向
   ↓
6. 模擬交易，計算 P&L
   ↓
7. 生成收益報告和績效指標
```

---

## 💹 套利策略

### 交易觸發條件

```
進場條件：tradable == True
  └─ 只有一個交易所支付 funding，另一個接收

交易方向判斷邏輯（根據 funding rate 正負號）：

  Case 1: bybit_pay=True && binance_pay=False
    
    根據 bybit_funding_rate 的符號確定 Bybit 方向：
      if bybit_funding_rate > 0:
        ├─ Bybit 多頭支付 funding
        ├─ 策略：Short Bybit（避免支付）+ Long Binance（接收 funding）
        └─ Direction: "SHORT_BYBIT_LONG_BINANCE"
      
      elif bybit_funding_rate < 0:
        ├─ Bybit 多頭接收 funding
        ├─ 策略：Long Bybit（接收 funding）+ Short Binance（避免支付）
        └─ Direction: "LONG_BYBIT_SHORT_BINANCE"
    
    收益來自：
      - 接收方交易所的 funding fee 收入
      - 價格套利（如果有）
      - 避免支付方交易所的 funding fee 支出

  Case 2: binance_pay=True && bybit_pay=False
    
    根據 binance_funding_rate 的符號確定 Binance 方向：
      if binance_funding_rate > 0:
        ├─ Binance 多頭支付 funding
        ├─ 策略：Short Binance（避免支付）+ Long Bybit（接收 funding）
        └─ Direction: "SHORT_BINANCE_LONG_BYBIT"
      
      elif binance_funding_rate < 0:
        ├─ Binance 多頭接收 funding
        ├─ 策略：Long Binance（接收 funding）+ Short Bybit（避免支付）
        └─ Direction: "LONG_BINANCE_SHORT_BYBIT"
    
    收益來自：
      - 接收方交易所的 funding fee 收入
      - 價格套利（如果有）
      - 避免支付方交易所的 funding fee 支出
```

### 時間序列和 VWAP 計算

```
例子：
  timestamp = 2025-10-31 11:00:00 (tradable=True)
  設定：vwap_window_minutes = t (如 5 分鐘)

時間軸：
  10:55    10:56   ...  10:59   11:00   11:01   ...  11:04   11:05
  ◄───────── Entry VWAP 窗口 ────────┬──────────── Exit VWAP 窗口 ──────────►
             (前 t 分鐘)          timestamp        (後 t 分鐘)

計算邏輯：
  Entry VWAP = [timestamp - t min, timestamp] 的加權平均價格
  Exit VWAP = [timestamp, timestamp + t min] 的加權平均價格
  
  VWAP = Σ(典型價格 × 交易量) / Σ(交易量)
       = Σ((high + low + close)/3 × volume) / Σ(volume)
```

### 資本分配

```
動態資本分配邏輯：

給定時間點 T，統計 tradable=True 的 symbol 數量 n：

  if n == 1:
    K_symbol = total_capital (全部資金給這個 symbol)
  
  else if n > 1:
    K_symbol = total_capital / n (平均分配)

單筆交易倉位：
  position_size = K_symbol / 2
  
    多頭倉位 = position_size  (在接收方交易所)
    空頭倉位 = position_size  (在支付方交易所)
    ──────────────────────
    總占用資金 = K_symbol
```

### P&L 計算公式

#### Case 1: Long Bybit + Short Binance (bybit_pay=True && bybit_rate < 0)

```
交易結構：
  多頭倉：Buy Bybit @ Entry VWAP → Sell Bybit @ Exit VWAP
  空頭倉：Sell Binance @ Entry VWAP → Buy Binance @ Exit VWAP

條件：bybit_rate < 0 表示 Bybit 多頭接收 funding

P&L 組成部分：

1. Bybit 多頭利潤 (Long Profit):
   price_P&L_bybit = (bybit_exit_vwap - bybit_entry_vwap) / bybit_entry_vwap

2. Binance 空頭利潤 (Short Profit):
   price_P&L_binance = -(binance_exit_vwap - binance_entry_vwap) / binance_entry_vwap

3. 資金費收入 (Funding Income):
   funding_P&L = abs(bybit_funding_rate)  ← Bybit 多頭接收資金費

總 P&L（稅前）：
  gross_P&L = K/2 × [
    (bybit_exit_vwap - bybit_entry_vwap) / bybit_entry_vwap
    - (binance_exit_vwap - binance_entry_vwap) / binance_entry_vwap
    + abs(bybit_funding_rate)
  ]

P&L（稅後）：
  net_P&L = gross_P&L - trading_fees
```

#### Case 2: Short Bybit + Long Binance (bybit_pay=True && bybit_rate > 0)

```
交易結構：
  空頭倉：Sell Bybit @ Entry VWAP → Buy Bybit @ Exit VWAP
  多頭倉：Buy Binance @ Entry VWAP → Sell Binance @ Exit VWAP

條件：bybit_rate > 0 表示 Bybit 多頭支付 funding，我們做空避免支付

P&L 組成部分：

1. Bybit 空頭利潤 (Short Profit):
   price_P&L_bybit = -(bybit_exit_vwap - bybit_entry_vwap) / bybit_entry_vwap

2. Binance 多頭利潤 (Long Profit):
   price_P&L_binance = (binance_exit_vwap - binance_entry_vwap) / binance_entry_vwap

3. 資金費收入 (Funding Income):
   funding_P&L = abs(bybit_funding_rate)  ← Bybit 空頭接收資金費

總 P&L（稅前）：
  gross_P&L = K/2 × [
    - (bybit_exit_vwap - bybit_entry_vwap) / bybit_entry_vwap
    + (binance_exit_vwap - binance_entry_vwap) / binance_entry_vwap
    + abs(bybit_funding_rate)
  ]

P&L（稅後）：
  net_P&L = gross_P&L - trading_fees
```

#### Case 3: Long Binance + Short Bybit (binance_pay=True && binance_rate < 0)

```
交易結構：
  多頭倉：Buy Binance @ Entry VWAP → Sell Binance @ Exit VWAP
  空頭倉：Sell Bybit @ Entry VWAP → Buy Bybit @ Exit VWAP

條件：binance_rate < 0 表示 Binance 多頭接收 funding

P&L 組成部分：

1. Binance 多頭利潤 (Long Profit):
   price_P&L_binance = (binance_exit_vwap - binance_entry_vwap) / binance_entry_vwap

2. Bybit 空頭利潤 (Short Profit):
   price_P&L_bybit = -(bybit_exit_vwap - bybit_entry_vwap) / bybit_entry_vwap

3. 資金費收入 (Funding Income):
   funding_P&L = abs(binance_funding_rate)  ← Binance 多頭接收資金費

總 P&L（稅前）：
  gross_P&L = K/2 × [
    (binance_exit_vwap - binance_entry_vwap) / binance_entry_vwap
    - (bybit_exit_vwap - bybit_entry_vwap) / bybit_entry_vwap
    + abs(binance_funding_rate)
  ]

P&L（稅後）：
  net_P&L = gross_P&L - trading_fees
```

#### Case 4: Short Binance + Long Bybit (binance_pay=True && binance_rate > 0)

```
交易結構：
  空頭倉：Sell Binance @ Entry VWAP → Buy Binance @ Exit VWAP
  多頭倉：Buy Bybit @ Entry VWAP → Sell Bybit @ Exit VWAP

條件：binance_rate > 0 表示 Binance 多頭支付 funding，我們做空避免支付

P&L 組成部分：

1. Binance 空頭利潤 (Short Profit):
   price_P&L_binance = -(binance_exit_vwap - binance_entry_vwap) / binance_entry_vwap

2. Bybit 多頭利潤 (Long Profit):
   price_P&L_bybit = (bybit_exit_vwap - bybit_entry_vwap) / bybit_entry_vwap

3. 資金費收入 (Funding Income):
   funding_P&L = abs(binance_funding_rate)  ← Binance 空頭接收資金費

總 P&L（稅前）：
  gross_P&L = K/2 × [
    - (binance_exit_vwap - binance_entry_vwap) / binance_entry_vwap
    + (bybit_exit_vwap - bybit_entry_vwap) / bybit_entry_vwap
    + abs(binance_funding_rate)
  ]

P&L（稅後）：
  net_P&L = gross_P&L - trading_fees
```

### 實際例子

```
基礎 CSV 行：
  2025-10-31 11:00:00, 4, 1, 3, -0.00373496, -0.0032151, -0.00051986,
  True, 4h_vs_1h, False, True, True

解析（固定部分）：
  timestamp = 2025-10-31 11:00:00
  binance_interval = 4h, bybit_interval = 1h
  mismatch_type = "4h_vs_1h"
  binance_pay = False, bybit_pay = True
  tradable = True ✓

根據 funding rate 正負號，有 2 種交易方向：

═══════════════════════════════════════════════════════════════

情景 A：bybit_rate < 0（Bybit 多頭接收 funding）
  數據：binance_rate = -0.00373496, bybit_rate = -0.0032151
  
  交易方向：Long Bybit + Short Binance (Case 1)
  
  設定假設：
    vwap_window_minutes = 5
    total_capital = 100,000 USDT
    該時間點只有 1 個 symbol tradable → K = 100,000
    position_size = 50,000 USDT (K/2)
  
  K 線數據（示意）：
    Entry VWAP: Binance = 50,000, Bybit = 50,050
    Exit VWAP:  Binance = 50,100, Bybit = 50,150
  
  計算 P&L (Case 1 公式)：
    
    Bybit 多頭利潤 = (50150 - 50050) / 50050 = 0.001988
    Binance 空頭利潤 = -(50100 - 50000) / 50000 = -0.002
    資金費收入 = abs(-0.0032151) = 0.0032151
    
    gross_P&L = 50,000 × (0.001988 - 0.002 + 0.0032151)
              = 50,000 × 0.003203
              = 160.15 USDT
    
    fees = 60 USDT
    net_P&L = 160.15 - 60 = 100.15 USDT ✓

═══════════════════════════════════════════════════════════════

情景 B：bybit_rate > 0（Bybit 多頭支付 funding）
  假設：bybit_rate = +0.0032151（反向情況）
  
  交易方向：Short Bybit + Long Binance (Case 2)
  
  設定同上，position_size = 50,000
  K 線數據同上
  
  計算 P&L (Case 2 公式)：
    
    Bybit 空頭利潤 = -(50150 - 50050) / 50050 = -0.001988
    Binance 多頭利潤 = (50100 - 50000) / 50000 = 0.002
    資金費收入 = abs(0.0032151) = 0.0032151（做空接收）
    
    gross_P&L = 50,000 × (-0.001988 + 0.002 + 0.0032151)
              = 50,000 × 0.003227
              = 161.35 USDT
    
    fees = 60 USDT
    net_P&L = 161.35 - 60 = 101.35 USDT ✓

═══════════════════════════════════════════════════════════════

情景 C：binance_pay=True, bybit_pay=False, binance_rate < 0
  數據變化：binance_pay = True, bybit_pay = False
           binance_rate = -0.00373496
  
  交易方向：Long Binance + Short Bybit (Case 3)
  
  計算 P&L (Case 3 公式)：
    
    Binance 多頭利潤 = (50100 - 50000) / 50000 = 0.002
    Bybit 空頭利潤 = -(50150 - 50050) / 50050 = -0.001988
    資金費收入 = abs(-0.00373496) = 0.00373496
    
    gross_P&L = 50,000 × (0.002 - 0.001988 + 0.00373496)
              = 50,000 × 0.003747
              = 187.35 USDT
    
    fees = 60 USDT
    net_P&L = 187.35 - 60 = 127.35 USDT ✓

═══════════════════════════════════════════════════════════════

情景 D：binance_pay=True, bybit_pay=False, binance_rate > 0
  數據變化：binance_pay = True, bybit_pay = False
           binance_rate = +0.00373496（反向）
  
  交易方向：Short Binance + Long Bybit (Case 4)
  
  計算 P&L (Case 4 公式)：
    
    Binance 空頭利潤 = -(50100 - 50000) / 50000 = -0.002
    Bybit 多頭利潤 = (50150 - 50050) / 50050 = 0.001988
    資金費收入 = abs(0.00373496) = 0.00373496（做空接收）
    
    gross_P&L = 50,000 × (-0.002 + 0.001988 + 0.00373496)
              = 50,000 × 0.003715
              = 185.75 USDT
    
    fees = 60 USDT
    net_P&L = 185.75 - 60 = 125.75 USDT ✓
```

---

## 🏗️ 系統架構

```
funding_interval_arb/
├── backtest/                           # 回測模塊（新增）
│   ├── __init__.py
│   ├── backtest_main.py               # 回測主程序入口
│   ├── backtest_config.py             # 回測設定定義
│   ├── opportunity_loader.py          # 加載 tradable 機會
│   ├── kline_fetcher.py               # 1M K 線和交易量獲取
│   ├── vwap_calculator.py             # VWAP 計算引擎
│   ├── backtest_engine.py             # 回測執行引擎
│   ├── backtest_analyzer.py           # 結果分析和績效指標
│   ├── config/                        # 設定檔案目錄
│   │   └── default_backtest_config.json
│   └── README.md
│
├── opportunity_analysis/               # 既有分析模塊
│   └── main.py                         # 存在性分析
│
└── data_collector/                    # 既有數據收集模塊
```

---

## ⚙️ 設定系統

### 設定檔案位置
```
/home/james/research/funding_interval_arb/backtest/config/backtest_config.json
```

### 設定架構 (JSON Schema)

```json
{
  "analysis": {
    "run_analysis_first": false,          // 若已有分析數據，設為 false
    "start_date": "2025-08-07",           // 回測開始日期
    "end_date": "2025-11-05",             // 回測結束日期
    "duration_days": 90                   // 分析天數
  },
  
  "trading": {
    "initial_capital": 100000,            // 初始資金 (USDT)
    "vwap_window_minutes": 5,             // VWAP 計算窗口 (分鐘)
                                          // Entry: [timestamp-5min, timestamp]
                                          // Exit: [timestamp, timestamp+5min]
                                          // 總持倉時間 = vwap_window_minutes × 2
    "entry_buffer_pct": 0.0005,           // 入場滑點 (0.05%)
    "exit_buffer_pct": 0.0005             // 出場滑點 (0.05%)
  },

  "fees": {
    "maker_fee": 0.0002,                  // Maker 手續費 (0.02%)
    "taker_fee": 0.0004                   // Taker 手續費 (0.04%)
  },

  "symbols": {
    "include_all": true,                  // 是否包含所有 tradable symbols
    "symbol_whitelist": [],               // 若 include_all=false，指定 symbol 列表
    "exclude_symbols": []                 // 排除特定 symbols
  },

  "output": {
    "output_dir": "/home/james/research_output/funding_interval_arb/backtest_results",
    "save_detailed_trades": true,
    "save_equity_curve": true,
    "generate_plots": true
  }
}
```

### 設定參數說明

| 參數 | 類型 | 默認值 | 說明 |
|------|------|--------|------|
| `run_analysis_first` | bool | false | 若已有分析結果且時間範圍足夠，設為 false；否則自動改為 true |
| `start_date` | string | 2025-08-07 | 回測開始日期 (YYYY-MM-DD) |
| `end_date` | string | 2025-11-05 | 回測結束日期 (YYYY-MM-DD) |
| `initial_capital` | float | 100000 | 初始資金 (USDT)，在多個 symbol 同時 tradable 時按比例分配 |
| `vwap_window_minutes` | int | 5 | VWAP 計算窗口 (分鐘)。總持倉時間 = vwap_window_minutes × 2 |
| `entry_buffer_pct` | float | 0.0005 | 入場滑點 (0.05%)，用於模擬市場沖擊 |
| `exit_buffer_pct` | float | 0.0005 | 出場滑點 (0.05%)，用於模擬市場沖擊 |
| `maker_fee` | float | 0.0002 | Maker 手續費 (0.02%)，出場時使用 |
| `taker_fee` | float | 0.0004 | Taker 手續費 (0.04%)，入場時使用 |
| `include_all` | bool | true | 是否包含所有 tradable symbols；false 時使用 whitelist |

---

## 🔄 執行流程

### 總體流程圖

```
START (backtest_main.py)
  │
  ├─→ [步驟 1] 加載設定
  │       └─ 讀取 backtest_config.json，驗證有效性
  │
  ├─→ [步驟 2] 檢查時間覆蓋
  │       ├─ 檢查設定時間是否被已有分析覆蓋
  │       └─ 若不符合 → 強制 run_analysis_first=true
  │
  ├─→ [步驟 3] 運行存在性分析（可選）
  │       ├─ 若需要，執行 opportunity_analysis/main.py
  │       └─ 生成 funding_rate_timeline_*.csv
  │
  ├─→ [步驟 4] 加載 Tradable 機會
  │       ├─ 讀取所有 funding_rate_timeline_*.csv
  │       ├─ 篩選 tradable=True 的行
  │       ├─ 應用過濾條件（min_duration, min_funding_diff）
  │       └─ 按時間點分組，統計每個時間點的 tradable symbol 個數
  │
  ├─→ [步驟 5] 並行獲取完整 1M K 線到緩存
  │       ├─ 時間範圍：使用 config.start_date 和 config.end_date
  │       ├─ 對每個有 tradable opportunity 的 symbol
  │       ├─ 對每個 exchange (Binance, Bybit)
  │       │   ├─ 檢查本地緩存：kline_cache_{symbol}_{exchange}_{start}_{end}.pkl
  │       │   ├─ 若緩存存在 → 驗證完整性（檢查時間覆蓋、無缺口）
  │       │   └─ 若缺失/不完整 → 從 API 獲取
  │       ├─ 使用 asyncio + semaphore 並行獲取（Binance 32, Bybit 15）
  │       └─ 保存到 pkl 緩存
  │
  ├─→ [步驟 6] 從緩存讀取並計算 VWAP
  │       ├─ 對每個 tradable 機會
  │       ├─ 從緩存 kline_cache_{symbol}_{exchange}_{start}_{end}.pkl 讀取
  │       ├─ 定位 timestamp 的前後 vwap_window 分鐘
  │       ├─ 計算 Entry VWAP: [timestamp - t, timestamp]
  │       ├─ 計算 Exit VWAP: [timestamp, timestamp + t]
  │       └─ 驗證 VWAP 有效性（非 NaN、volume 充足）
  │
  ├─→ [步驟 7] 執行回測
  │       ├─ 對每個 tradable 機會
  │       ├─ 根據 bybit_pay/binance_pay 判斷交易方向
  │       ├─ 計算 P&L (價差 + 資金費 - 手續費)
  │       ├─ 動態分配資本 (total_capital / n_tradable_at_time)
  │       └─ 累計權益曲線
  │
  ├─→ [步驟 8] 分析績效
  │       ├─ 計算 Sharpe、Sortino、最大回撤等
  │       ├─ 按 symbol 分組統計
  │       └─ 生成績效報告
  │
  └─→ [步驟 9] 輸出結果
         ├─ 保存詳細交易 CSV
         ├─ 保存權益曲線
         ├─ 生成績效圖表
         └─ 打印總結報告
```

---

## 📊 數據流

### 完整數據轉換過程

```
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: Tradable 機會加載                                       │
├─────────────────────────────────────────────────────────────────┤
│ Input:  funding_rate_timeline_*.csv (所有 symbol)
│
│ 篩選條件：
│   ✓ tradable == True
│   ✓ duration_hours >= min_mismatch_duration_hours
│   ✓ abs(rate_diff) >= min_funding_rate_diff_bps
│
│ 分組邏輯：
│   按 timestamp 分組
│   → 統計每個 timestamp 有多少個 symbol tradable=True
│   → 計算該時間點的資本分配：K = capital / n_symbols
│
│ Output: TradeableOpportunity[]
│ [
│   {
│     timestamp: 2025-10-31 11:00:00,
│     symbol: "BTCUSDT",
│     K: 50000,                        // 分配到該 symbol 的資金
│     bybit_pay: true,
│     binance_pay: false,
│     binance_rate: -0.00373496,
│     bybit_rate: -0.0032151,
│     direction: "long_bybit_short_binance",
│     n_tradable_at_time: 2            // 該時間點 2 個 symbol tradable
│   },
│   ...
│ ]
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Step 2: 完整 K 線獲取和緩存                                      │
├─────────────────────────────────────────────────────────────────┤
│ 時間範圍：使用 config.start_date 和 config.end_date
│
│ 對每個有 tradable opportunity 的 symbol，對每個 exchange：
│
│ 1. 檢查本地緩存
│    cache_key = kline_cache_{symbol}_{exchange}_{start_ms}_{end_ms}.pkl
│    if cache_exists:
│      cached_klines = load_cache(cache_key)
│      → 驗證完整性
│    else:
│      → 從 API 獲取
│
│ 2. 完整性驗證（參考 analysis 中的做法）
│    預期記錄數 = (end_time - start_time) / 60秒 = 期間內的分鐘數
│    
│    驗證項目：
│      ✓ 記錄總數 >= 預期 × 0.95 (95% 覆蓋率)
│      ✓ 時間連續（最大時間缺口 <= 5 分鐘）
│      ✓ 無異常值（volume > 0, price > 0）
│      ✓ 首尾時間邊界正確
│    
│    若驗證失敗：
│      → 記錄警告
│      → 嘗試重新從 API 獲取
│      → 若仍失敗，標記 symbol 為不可用
│
│ 3. 並行獲取
│    使用 asyncio + semaphore
│    - Binance: semaphore=32
│    - Bybit: semaphore=15
│
│ 4. 保存緩存
│    kline_cache_{symbol}_{exchange}_{start_ms}_{end_ms}.pkl
│    ├─ DataFrame (timestamp, open, high, low, close, volume)
│    └─ 元數據 (fetch_time, coverage_pct, validation_result)
│
│ Output: 所有需要的 K 線都已在本地緩存
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Step 3: 從緩存讀取 K 線並計算 VWAP                              │
├─────────────────────────────────────────────────────────────────┤
│ 對每個 tradable 機會 (symbol, timestamp)：
│
│ 1. 從緩存讀取 K 線
│    for exchange in [Binance, Bybit]:
│      start_time = min(all_timestamps[symbol])
│      end_time = max(all_timestamps[symbol])
│      cache_key = kline_cache_{symbol}_{exchange}_{start_ms}_{end_ms}.pkl
│      klines_df = load_cache(cache_key)
│
│ 2. 定位 VWAP 計算窗口
│    Entry 窗口：[timestamp - vwap_window, timestamp]
│    Exit 窗口：[timestamp, timestamp + vwap_window]
│
│ 3. 計算 VWAP
│    公式：VWAP = Σ(typical_price × volume) / Σ(volume)
│           典型價格 = (high + low + close) / 3
│    
│    entry_vwap = calculate_vwap(klines_df, entry_window_start, timestamp)
│    exit_vwap = calculate_vwap(klines_df, timestamp, exit_window_end)
│
│ 4. 驗證 VWAP 有效性
│    ✓ 非 NaN
│    ✓ 在 [low, high] 範圍內
│    ✓ 窗口內 volume 充足
│    ✓ 窗口有足夠的 K 線記錄 (>= 80% 期望)
│    
│    若驗證失敗 → 標記 vwap_valid=False，跳過該交易
│
│ Output: 更新 opportunity
│ {
│   timestamp,
│   symbol,
│   vwap_entry_binance: 50000,
│   vwap_entry_bybit: 50050,
│   vwap_exit_binance: 50100,
│   vwap_exit_bybit: 50150,
│   vwap_valid: true,
│   entry_volume_bn: 1000000,
│   exit_volume_bn: 1000000,
│   entry_volume_by: 950000,
│   exit_volume_by: 950000
│ }
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Step 4: 交易執行和 P&L 計算                                     │
├─────────────────────────────────────────────────────────────────┤
│ 根據 direction，計算 P&L：
│
│ For Case 1 (Long Bybit + Short Binance):
│   price_P&L = (K/2) × [
│     -(binance_exit - binance_entry) / binance_entry
│     + (bybit_exit - bybit_entry) / bybit_entry
│   ]
│   funding_P&L = (K/2) × abs(binance_funding_rate)
│   total_fees = (K/2) × (entry_taker + exit_maker) × 2
│   net_P&L = price_P&L + funding_P&L - total_fees
│
│ Output: Trade Record
│ {
│   timestamp,
│   symbol,
│   K,
│   position_size: K/2,
│   direction,
│   vwap_entry_bn, vwap_entry_by,
│   vwap_exit_bn, vwap_exit_by,
│   price_P&L,
│   funding_P&L,
│   total_fees,
│   net_P&L,
│   net_P&L_pct
│ }
│
│ Equity Curve:
│   cumulative_P&L += net_P&L
│   equity = initial_capital + cumulative_P&L
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Step 5: 績效分析                                                │
├─────────────────────────────────────────────────────────────────┤
│ 計算指標：
│   - Total Return %
│   - Sharpe Ratio
│   - Sortino Ratio
│   - Max Drawdown %
│   - Win Rate %
│   - 按 symbol 分組統計
│   - 按月度統計
│
│ Output: PerformanceReport
│ {
│   summary: {...},
│   risk_metrics: {...},
│   by_symbol: {...},
│   by_month: {...}
│ }
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔧 核心模塊

### 1. backtest_config.py
**職責**: 設定管理

```
Class BacktestConfig:
  Methods:
    - load_from_json(filepath)
    - validate()
    - get_time_range()
    - __str__() → 設定摘要
```

### 2. opportunity_loader.py
**職責**: 加載 tradable 機會並分組

```
Class OpportunityLoader:
  Methods:
    - load_tradable_opportunities(config)
    - group_by_timestamp() → Dict[timestamp, List[Opportunity]]
    - calculate_capital_per_symbol() → Dict[symbol, K]
```

### 3. kline_fetcher.py
**職責**: 並行獲取完整 1M K 線到緩存，驗證完整性

```
Class KlineFetcher:
  Methods:
    - validate_kline_completeness(klines_df, start_time, end_time):
        """
        驗證 K 線完整性（參考 analysis 中的做法）
        
        檢查項：
          ✓ 覆蓋率：記錄數 >= (預期 × 0.95)
          ✓ 時間連續性：max(缺口) <= 5 分鐘
          ✓ 無異常值：volume > 0, price > 0
          ✓ 邊界檢查：首尾時間正確
        
        返回：(is_valid, coverage_pct, gaps, anomalies)
        """
    
    - fetch_klines_parallel(symbol, exchange, start_time, end_time, config):
        """
        並行獲取單個 (symbol, exchange) 的完整 K 線
        
        邏輯：
          1. 檢查本地緩存
          2. 若存在 → 驗證完整性
          3. 若缺失/不完整 → 從 API 獲取
          4. 驗證新數據
          5. 保存到 pkl 緩存
        
        時間範圍來自 config.start_date / config.end_date
        """
    
    - fetch_all_klines(tradable_symbols, config):
        """
        並行獲取所有 tradable symbol 的 K 線（asyncio gather）
        
        輸入：tradable_symbols (有 opportunity 的 symbol 列表)
        時間範圍：config.start_date ~ config.end_date
        使用 semaphore：Binance 32, Bybit 15
        """
    
    - load_cached_klines(symbol, exchange, start_time, end_time):
        """
        從本地 pkl 緩存讀取 K 線 DataFrame
        """
```

### 4. vwap_calculator.py
**職責**: 從緩存計算 VWAP

```
Class VWAPCalculator:
  Methods:
    - calculate_vwap(klines_df, start_time, end_time):
        """
        計算指定時間窗口的 VWAP
        
        公式：VWAP = Σ(典型價格 × volume) / Σ(volume)
              典型價格 = (high + low + close) / 3
        
        返回：float or NaN
        """
    
    - calculate_entry_exit_vwap(opportunity, klines_dict, config):
        """
        計算入場和出場 VWAP
        
        輸入：
          - opportunity: {timestamp, symbol, ...}
          - klines_dict: {exchange: klines_df, ...}
          - config: {vwap_window_minutes, ...}
        
        過程：
          1. 定位 Entry 窗口：[timestamp - window, timestamp]
          2. 定位 Exit 窗口：[timestamp, timestamp + window]
          3. 各自計算 VWAP
          4. 驗證 volume 充足、記錄數足夠
        
        返回：(vwap_entry_bn, vwap_entry_by, vwap_exit_bn, vwap_exit_by, is_valid)
        """
```

### 5. backtest_engine.py
**職責**: 執行回測邏輯

```
Class BacktestEngine:
  Methods:
    - initialize(capital, config)
    
    - determine_trade_direction(opportunity):
        """
        根據 pay flags 和 funding rate 符號判斷交易方向
        
        返回：(direction, receiving_exchange, paying_exchange)
        
        Logic:
          if bybit_pay and not binance_pay:
            if bybit_rate < 0:
              return ("LONG_BYBIT_SHORT_BINANCE", "bybit", "binance")
            else:  // bybit_rate > 0
              return ("SHORT_BYBIT_LONG_BINANCE", "binance", "bybit")
          
          elif binance_pay and not bybit_pay:
            if binance_rate < 0:
              return ("LONG_BINANCE_SHORT_BYBIT", "binance", "bybit")
            else:  // binance_rate > 0
              return ("SHORT_BINANCE_LONG_BYBIT", "bybit", "binance")
        """
    
    - execute_trade(opportunity, klines, config):
        """
        執行單筆交易
        
        Steps:
          1. 判斷交易方向
          2. 提取 Entry/Exit VWAP
          3. 計算價差利潤
          4. 計算資金費收入
          5. 計算手續費
          6. 返回交易記錄
        """
    
    - calculate_P&L(opportunity, vwap_entry_bn, vwap_exit_bn, 
                    vwap_entry_by, vwap_exit_by, direction):
        """
        根據方向計算 P&L
        
        對應 4 種 Case 的公式
        """
    
    - run_backtest(opportunities, config)
    - get_equity_curve()
```

### 6. backtest_analyzer.py
**職責**: 分析績效

```
Class BacktestAnalyzer:
  Methods:
    - calculate_metrics(trades, equity_curve)
    - generate_report(metrics)
    - plot_results(metrics)
```

### 7. backtest_main.py
**職責**: 主程序協調

```
Function main():
  1. load_config()
  2. check_time_coverage()
  3. run_analysis_if_needed()
  4. load_opportunities()
  5. fetch_klines()
  6. calculate_vwaps()
  7. run_backtest()
  8. analyze_performance()
  9. save_results()
```

---

## 📁 結果輸出

```
/home/james/research_output/funding_interval_arb/backtest_results/
├── BACKTEST_YYYYMMDD_timestamp/
│   ├── backtest_config.json          # 設定副本
│   ├── trades.csv                    # 詳細交易
│   ├── equity_curve.csv              # 權益曲線
│   ├── performance_report.txt        # 績效報告
│   ├── symbol_stats.csv              # symbol 統計
│   ├── daily_P&L.csv                 # 日度 P&L
│   ├── plots/                        # 圖表
│   │   ├── equity_curve.png
│   │   ├── drawdown_curve.png
│   │   ├── monthly_P&L.png
│   │   ├── symbol_heatmap.png
│   │   └── P&L_distribution.png
│   └── execution_log.txt
```

---

## ⚡ 性能考量

### 並行獲取 K 線
- **方案**: asyncio + semaphore（Binance 32, Bybit 15）
- **預期時間**: 500 symbols × 90 天 ≈ 10-30 分鐘

### VWAP 計算
- **複雜度**: O(n × m)，n=symbols, m=mismatch 時長（分鐘）
- **預期時間**: ≈ 1-5 分鐘

### 回測執行
- **複雜度**: O(k)，k=tradable opportunities 總數
- **預期時間**: < 1 分鐘

---

**文檔版本**: v2.0 (Corrected)  
**最後更新**: 2025-11-06  
**狀態**: ✅ 架構設計完成

