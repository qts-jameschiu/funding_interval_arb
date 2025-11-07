# Funding Interval Mismatch Arbitrage Analysis

## 🎯 概述

這個項目分析 Binance 和 Bybit 之間的 Funding Interval Mismatch 現象，以評估套利機會的存在性。

通過分析 90 天的歷史數據，識別兩個交易所 funding 結算時間的不同步現象，計算 mismatch 事件的頻率、持續時間和 funding rate 差異。

## 📋 前置要求

- Python 3.11+
- Anaconda 或 Miniconda
- 以下 Python 套件（已在 conda 環境中安裝）：
  - aiohttp, pandas, numpy, matplotlib, seaborn, requests, python-dotenv

## 🚀 快速開始

### 1. 環境設置（一次性）

```bash
# 創建或激活 quantrend 環境
conda activate quantrend

# 如果環境不存在，創建它
conda create -n quantrend python=3.11

# 安裝依賴套件
pip install aiohttp pandas numpy matplotlib seaborn requests python-dotenv
```

### 2. 運行分析

**自動運行（推薦）**
```bash
cd /home/james/research/funding_interval_arb
./run_analysis.sh
```

**手動運行**
```bash
conda activate quantrend
python main.py
```

## 📊 輸出結果

分析完成後，結果將保存在 `/home/james/research_output/funding_interval_arb/existence_analysis/`：

```
├── data/
│   ├── mismatch_events.csv              # 所有 mismatch 事件
│   ├── symbol_funding_rates.csv         # 每個幣種的 funding rate
│   └── interval_matrix_*.csv            # 各幣種時間序列矩陣
├── plots/
│   ├── interval_mismatch_heatmap.png   # Mismatch 熱圖
│   ├── duration_histogram.png          # 持續時間分佈
│   ├── symbol_ranking.png              # 幣種頻率排名
│   ├── mismatch_type_distribution.png  # 類型分佈
│   └── timeline_*.png                  # 各幣種時間線圖
└── analysis_report.txt                  # 完整分析報告
```

## ⚙️ 配置

編輯 `config.py` 來自定義分析參數：

```python
ANALYSIS_DAYS = 90              # 分析天數
BATCH_SIZE = 5                  # API 請求批次大小
MAX_RETRIES = 3                 # API 重試次數
MISMATCH_THRESHOLD = 3600       # Mismatch 閾值（秒）
VALID_INTERVALS = [1, 2, 4, 8]  # 有效的 interval 值（小時）
```

## 📁 項目結構

```
funding_interval_arb/
├── main.py                      # 主程序入口
├── config.py                    # 配置文件
├── binance_client.py            # Binance API 客戶端
├── bybit_client.py              # Bybit API 客戶端
├── interval_analyzer.py         # Interval 分析核心邏輯
├── stats_analyzer.py            # 統計分析
├── visualizer.py                # 數據可視化
├── utils.py                     # 工具函數
├── requirements.txt             # Python 依賴
├── run_analysis.sh              # 執行腳本
└── README.md                    # 本文件
```

## 📖 使用說明

### 配置 API 認證

創建 `.env` 文件（可選，如需 API 認證）：
```
BINANCE_API_KEY=your_key
BINANCE_API_SECRET=your_secret
BYBIT_API_KEY=your_key
BYBIT_API_SECRET=your_secret
```

### 運行分析

```bash
# 自動執行完整分析流程
./run_analysis.sh

# 或手動執行
python main.py
```

### 查看結果

分析完成後，查看：
- **圖表**: `plots/` 目錄下的 PNG 圖表
- **報告**: `analysis_report.txt` 完整的文字報告
- **數據**: `data/` 目錄下的 CSV 檔案

## 🔍 分析過程

1. **數據收集**：從 Binance 和 Bybit 獲取所有 USDT perpetual 交易對的 funding rate 歷史
2. **數據對齐**：通過時間窗口對齐不同 funding interval 的數據
3. **Mismatch 檢測**：識別 interval 不同步的時間段
4. **統計分析**：計算 mismatch 事件的統計特性
5. **可視化**：生成多種圖表展示結果

## ✨ 主要特性

- ✅ 動態獲取所有交易對（不需硬編碼）
- ✅ 完整的時間對齊演算法（支持不同 interval）
- ✅ Bybit API 分頁支持（突破 200 筆記錄限制）
- ✅ 詳細的統計分析（平均值、中位數、分佈等）
- ✅ 多種可視化圖表（熱圖、直方圖、時間線等）
- ✅ 異步並行數據收集（提高效率）
- ✅ 指數退避重試機制（處理 API 限制）
- ✅ 完整的錯誤處理和日誌記錄

## 🐛 常見問題

**Q: 分析時間要多久？**  
A: 通常 15-30 分鐘（取決於網絡速度和 API 限制）。

**Q: 如何檢查 quantrend 環境？**  
A: 執行 `conda info --envs`

**Q: 如何更新依賴套件？**  
A: 執行 `pip install --upgrade aiohttp pandas numpy matplotlib seaborn`

**Q: 遇到 API 限制怎麼辦？**  
A: 腳本會自動使用指數退避重試機制，無需干預。

## 📈 最近更新

- **2025-11-03**: 
  - 修復 interval 浮點數精度問題（全部取整）
  - 修復時間對齐問題（對齐到整點）
  - 簡化項目結構（刪除冗余文檔）
  - Bybit API 分頁支持（支持完整數據獲取）
  - 轉換為使用 conda 環境（放棄 venv）

## 📄 許可證

MIT

---

**上次更新**: 2025-11-03  
**環境**: Conda (quantrend)  
**Python 版本**: 3.11+  
**狀態**: ✅ 生產就緒

