#!/bin/bash

# Funding Interval Mismatch Arbitrage Analysis
# Run complete analysis pipeline

echo "======================================================================"
echo "  Funding Interval Mismatch Arbitrage - Existence Analysis"
echo "======================================================================"
echo ""

# Find the quantrend python interpreter
QUANTREND_PYTHON="/home/james/anaconda3/envs/quantrend/bin/python"

if [ ! -f "$QUANTREND_PYTHON" ]; then
    echo "❌ Error: quantrend environment not found at $QUANTREND_PYTHON"
    echo ""
    echo "Please create and setup the quantrend environment:"
    echo "  conda create -n quantrend python=3.11"
    echo "  conda activate quantrend"
    echo "  pip install aiohttp pandas numpy matplotlib seaborn requests python-dotenv"
    exit 1
fi

echo "✅ Found quantrend environment"
echo "📍 Using: $QUANTREND_PYTHON"
echo ""

# Change to script directory
cd "$(dirname "$0")" || exit

echo "Starting analysis..."
echo "--------------------------------------------------------------------"
echo ""

# Run the main analysis using the quantrend python
$QUANTREND_PYTHON main.py

if [ $? -eq 0 ]; then
    echo ""
    echo "======================================================================"
    echo "✅ Analysis completed successfully!"
    echo "======================================================================"
    echo "Results saved to:"
    echo "  📁 /home/james/research_output/funding_interval_arb/existence_analysis/"
    echo ""
    echo "View results:"
    echo "  📄 cat analysis_report.txt"
    echo "  📊 View plots in plots/ directory"
    echo "  📋 View data in data/ directory"
else
    echo ""
    echo "======================================================================"
    echo "❌ Analysis failed. Please check the error messages above."
    echo "======================================================================"
    exit 1
fi

