from datetime import date as _date, timedelta

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AutomationCondition,
    DataVersion,
    DataVersionsByPartition,
    MetadataValue,
    asset_check,
    observable_source_asset,
)

from dagster_project.partitions import daily_partitions
from dagster_project.resources import MinioResource


def _yesterday() -> str:
    return (_date.today() - timedelta(days=1)).isoformat()


def _ymd(date_str: str) -> tuple[str, str, str]:
    return date_str[:4], date_str[5:7], date_str[8:10]


# StorageConsumer layout: price.snapshot/asset_class=<ac>/symbol=<SYM>/year=/month=/day=/
_STOCK_BASE = "price.snapshot/asset_class=stock/"
_CRYPTO_BASE = "price.snapshot/asset_class=crypto/"


def _stock_exists(minio: MinioResource, date_str: str) -> bool:
    return minio.day_partition_exists(minio.market_data_bucket, _STOCK_BASE, *_ymd(date_str))


def _crypto_exists(minio: MinioResource, date_str: str) -> bool:
    return minio.day_partition_exists(minio.market_data_bucket, _CRYPTO_BASE, *_ymd(date_str))


@observable_source_asset(
    partitions_def=daily_partitions,
    # Auto-observe at 08:00 UTC (15:00 GMT+7) daily — 1 hour before ohlcv_daily_bars runs.
    # Gives eager() on downstream assets a valid observation to evaluate against.
    automation_condition=AutomationCondition.on_cron("0 8 * * *"),
    group_name="batch_pipeline",
    description="Raw price snapshots (Avro) produced by StorageConsumer — external to Dagster.",
    metadata={
        "storage": MetadataValue.text("MinIO — market-data bucket"),
        "format": MetadataValue.text("Avro"),
        "partition_scheme": MetadataValue.text("price.snapshot/asset_class={val}/symbol={sym}/year={y}/month={m}/day={d}/"),
        "producer": MetadataValue.text("mekong-kafka / StorageConsumer"),
        "asset_classes": MetadataValue.text("stock, crypto"),
    },
)
def price_snapshots(minio: MinioResource) -> DataVersionsByPartition:
    """Observe recent partitions and report whether stock/crypto data exists in MinIO.

    Partitioned observable source assets must return DataVersionsByPartition — the
    observation runs without a partition context (UI- or automation-triggered), so we
    scan the last 7 daily partitions and emit one DataVersion per partition key.
    """
    versions: dict[str, DataVersion] = {}

    today = _date.today()
    for offset in range(1, 8):  # yesterday → 7 days ago
        partition_key = (today - timedelta(days=offset)).isoformat()
        stock_exists = _stock_exists(minio, partition_key)
        crypto_exists = _crypto_exists(minio, partition_key)
        versions[partition_key] = DataVersion(
            f"stock={'exists' if stock_exists else 'missing'},"
            f"crypto={'exists' if crypto_exists else 'missing'}"
        )

    return DataVersionsByPartition(versions)


@asset_check(
    asset=price_snapshots,
    blocking=True,
    description="Fails when stock price snapshots are absent for yesterday's partition. Skipped on weekends.",
)
def price_snapshots_stock_exists(minio: MinioResource) -> AssetCheckResult:
    yesterday = _date.today() - timedelta(days=1)
    date = yesterday.isoformat()
    if yesterday.weekday() >= 5:  # 5=Saturday, 6=Sunday
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.ERROR,
            metadata={"partition": date, "skipped_reason": "weekend — stock market closed"},
        )
    exists = _stock_exists(minio, date)
    return AssetCheckResult(
        passed=exists,
        severity=AssetCheckSeverity.ERROR,
        metadata={"partition": date, "base_prefix": _STOCK_BASE, "exists": exists},
    )


@asset_check(
    asset=price_snapshots,
    blocking=False,
    description="Warns when crypto price snapshots are absent for yesterday's partition.",
)
def price_snapshots_crypto_exists(minio: MinioResource) -> AssetCheckResult:
    date = _yesterday()
    exists = _crypto_exists(minio, date)
    return AssetCheckResult(
        passed=exists,
        severity=AssetCheckSeverity.WARN,
        metadata={"partition": date, "base_prefix": _CRYPTO_BASE, "exists": exists},
    )
