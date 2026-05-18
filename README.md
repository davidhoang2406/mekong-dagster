# mekong-dagster

Dagster orchestration layer for the Mekong market-data platform.

Schedules and monitors the batch pipeline:
```
price_snapshots (source) → ohlcv_daily_bars → technical_indicators
```

## Quick start

```bash
make install              # create venv + install dagster_project package
make dagster-up           # start webserver + daemon via mekong-infra → http://localhost:3000
```

Requires `mekong-infra` to be cloned as a sibling directory (for `make dagster-up`).

## Asset graph

| Asset | Type | Schedule |
|---|---|---|
| `price_snapshots` | Observable source | Checked for freshness |
| `ohlcv_daily_bars` | Daily partition | `0 16 * * 1-5` Asia/Ho_Chi_Minh (after HOSE close) |
| `technical_indicators` | Daily partition | Auto-materialises when `ohlcv_daily_bars` is ready |

## Tests

```bash
make test    # runs tests/ — no Docker needed
```

## Structure

```
dagster_project/
  assets/
    price_snapshots.py   # @observable_source_asset
    ohlcv.py             # @asset ohlcv_daily_bars
    technical.py         # @asset technical_indicators
  resources.py           # MinioResource, SparkClusterResource
  schedules.py           # daily_market_close schedule
  partitions.py          # DailyPartitionsDefinition (Asia/Ho_Chi_Minh)
dagster.yaml             # SQLite instance config
workspace.yaml           # code location pointer
```
