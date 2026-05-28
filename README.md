# mekong-dagster

Dagster orchestration layer for the Mekong market-data platform. Schedules
and monitors the batch pipeline, alerts on failures and pipeline health.

```
price_snapshots (observable source)
  └── ohlcv_daily_bars (Spark)
        ├── technical_indicators (Spark)
        ├── digest (Spark)
        └── screener_results (Spark, weekly)
```

## Structure

```
dagster_project/
  assets/
    price_snapshots.py      # @observable_source_asset — checks raw freshness
    ohlcv.py                # ohlcv_daily_bars + 3 asset_checks
    technical.py            # technical_indicators
    digest.py               # daily digest
    screener.py             # weekly fundamental screener
  resources.py              # MinioResource, SparkClusterResource, KafkaAdminResource, PodHealth
  schedules.py              # daily_market_close (16:00 ICT M-F), weekly_screener (Mon 08:00 ICT)
  partitions.py             # DailyPartitionsDefinition + WeeklyPartitionsDefinition (Asia/Ho_Chi_Minh)
  sensors.py                # Telegram on success/failure, raw expiry, Kafka pipeline health
  kafka_pipeline_jobs.py    # start/stop/restart producer + consumer deployments
dagster.yaml                # Postgres run storage + K8sRunLauncher
workspace.yaml              # code location pointer
Dockerfile                  # image used by webserver, daemon, and per-run pods
```

## Quick start (local)

```bash
make install              # create venv + install dagster_project package
dagster dev               # → http://localhost:3000 (uses SQLite locally)
```

## Production (Kubernetes)

Deployed by `mekong-infra/k8s/mekong-orchestration/`:

- `dagster-webserver` — UI at `http://dagster.mekong.local`
- `dagster-daemon` — scheduler + sensor evaluator
- `dagster-postgres` — run storage StatefulSet
- `K8sRunLauncher` — every run executes in its own pod using `job_image`

The webserver, daemon, and per-run pods all use the same image:

```
ghcr.io/davidhoang2406/mekong-dagster:latest
```

Built and pushed by CI on every push to `main`.

## Schedules

| Schedule | Cron | Asset | Partition |
|---|---|---|---|
| `daily_market_close` | `0 16 * * 1-5` (ICT) | `ohlcv_daily_bars` | Yesterday's trading day |
| `weekly_screener` | `0 8 * * 1` (ICT) | `screener_results` | Previous ISO week |

## Sensors

| Sensor | Interval | Purpose |
|---|---|---|
| `telegram_on_failure` | run status | Alert on any failed run |
| `telegram_on_success` | run status | Alert on any successful run |
| `raw_data_expiry_sensor` | hourly | Warn when raw snapshots near the 30-day MinIO TTL without `_SUCCESS` markers or OHLCV output |
| `kafka_pipeline_health_sensor` | 5 min | Pod phase + readiness + restart count, consumer lag (absolute + delta), producer topic offset advancement |

All Telegram-driven sensors require `TELEGRAM_BOT_TOKEN` and
`TELEGRAM_CHAT_ID` env vars; they log a warning and skip if missing.

## Jobs (manually triggerable)

| Job | Purpose |
|---|---|
| `ohlcv_daily_job` | Materialise the daily OHLCV partition |
| `weekly_screener_job` | Materialise the weekly screener partition |
| `start_kafka_pipeline_job` | Scale all 3 pipeline deployments to 1 replica |
| `stop_kafka_pipeline_job` | Scale all 3 pipeline deployments to 0 replicas |

## Environment

| Var | Purpose |
|---|---|
| `DAGSTER_PG_URL` | Postgres connection string for run storage |
| `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | MinIO access |
| `MINIO_BUCKET` / `MINIO_ANALYSIS_BUCKET` | Bucket overrides |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers (for health sensor) |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Telegram notifications |
| `DAGSTER_WEBSERVER_URL` | Used in Telegram messages so links resolve |
| `SPARK_IMAGE` | Image used for SparkApplication CRDs (default: latest mekong-spark) |

## Tests

```bash
make test    # asset + resource tests, no Docker needed
```

## Depends on

- `mekong-jobs` — supplies the Spark image referenced by `SparkClusterResource`
- `mekong-infra` — provides Postgres, RBAC, and the running spark-operator
