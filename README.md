# Stock Sentiment + Trend Predictor

Unified pipeline for banking-stock direction prediction using:
- Sentiment model from news headlines
- Trend model from market data
- Combined decision logic with confidence
- Capital-based backtesting with plots

## Project Structure

```
stock_sentiment_predictor/
├── combined_prediction_pipeline.py
├── combined_backtesting.py
├── data/
│   ├── raw/
│   └── processed/
│       ├── all_banking_news.csv
│       └── features.csv
├── results/
│   ├── reports/
│   └── combined_backtesting/
├── sentiment-predictor/
│   └── saved_models/
├── trend-predictor/
│   └── saved_model/
├── requirements.txt
└── README.md
```

## Setup

1. Create and activate environment

```powershell
python -m venv .StoEnv
.\.StoEnv\Scripts\Activate.ps1
```

2. Install dependencies

```powershell
pip install -r requirements.txt
```

## Combined Prediction

Run a single combined prediction:

```powershell
python -c "from combined_prediction_pipeline import CombinedPredictionPipeline; p=CombinedPredictionPipeline(); print(p.predict('HDFC Bank Q3 profit rises','2026-03-11','HDFCBANK',verbose=False))"
```

Inputs expected by pipeline:
- headline
- date (YYYY-MM-DD)
- ticker (HDFCBANK, SBIN, ICICIBANK, AXISBANK, KOTAKBANK; lowercase also supported)

## Combined Backtesting

Backtesting script samples rows from data/processed/all_banking_news.csv, predicts, applies trading rules, validates against next-day realized move, and reports net result.

Basic run:

```powershell
python combined_backtesting.py
```

Custom run:

```powershell
python combined_backtesting.py --samples 30 --seed 99 --threshold 0.45 --initial-capital 100000 --position-size 0.2
```

Main options:
- --samples: number of sampled rows
- --seed: random sample seed
- --start-date / --end-date: date filter
- --threshold: confidence threshold for trade decisions
- --initial-capital: starting cash
- --position-size: fraction of cash allocated per trade
- --output: output CSV path
- --plots-dir: output plot directory

## Outputs

Backtesting outputs:
- results/reports/combined_backtest_results.csv
- results/combined_backtesting/equity_curve.png
- results/combined_backtesting/decision_distribution.png
- results/combined_backtesting/pnl_distribution.png
- results/combined_backtesting/confidence_vs_actual_return.png

Console summary includes:
- Initial capital
- Final capital
- Net PnL
- Return percentage
- Trade counts and win rate

## Before Pushing to GitHub

This repository includes a .gitignore configured to avoid committing:
- local virtual environment
- model binaries and generated predictions
- generated backtesting artifacts and caches

Recommended pre-push checks:

```powershell
python combined_prediction_pipeline.py
python combined_backtesting.py --samples 10 --seed 42
```

Then commit only source + required lightweight data files.

## Notes

- If trend prediction is unavailable for some rows, combined logic falls back to sentiment-only.
- Backtesting uses yfinance for realized next-day movement verification.
