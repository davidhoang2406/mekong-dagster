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


def _stock_prefix(date_str: str) -> str:
    year, month, day = _ymd(date_str)
    return f"price.snapshot/asset_class=stock/year={year}/month={month}/day={day}/"


def _crypto_prefix(date_str: str) -> str:
    year, month, day = _ymd(date_str)
    return f"price.snapshot/asset_class=crypto/year={year}/month={month}/day={day}/"


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
        "partition_scheme": MetadataValue.text("price.snapshot/asset_class={val}/year={y}/month={m}/day={d}/"),
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
        stock_exists = minio.partition_exists(minio.market_data_bucket, _stock_prefix(partition_key))
        crypto_exists = minio.partition_exists(minio.market_data_bucket, _crypto_prefix(partition_key))
        versions[partition_key] = DataVersion(
            f"stock={'exists' if stock_exists else 'missing'},"
            f"crypto={'exists' if crypto_exists else 'missing'}"
        )

    return DataVersionsByPartition(versions)


@asset_check(
    asset=price_snapshots,
    blocking=True,
    description="Fails when stock price snapshots are absent for yesterday's partition.",
)
def price_snapshots_stock_exists(minio: MinioResource) -> AssetCheckResult:
    date = _yesterday()
    prefix = _stock_prefix(date)
    exists = minio.partition_exists(minio.market_data_bucket, prefix)
    return AssetCheckResult(
        passed=exists,
        severity=AssetCheckSeverity.ERROR,
        metadata={"partition": date, "prefix": prefix, "exists": exists},
    )


@asset_check(
    asset=price_snapshots,
    blocking=False,
    description="Warns when crypto price snapshots are absent for yesterday's partition.",
)
def price_snapshots_crypto_exists(minio: MinioResource) -> AssetCheckResult:
    date = _yesterday()
    prefix = _crypto_prefix(date)
    exists = minio.partition_exists(minio.market_data_bucket, prefix)
    return AssetCheckResult(
        passed=exists,
        severity=AssetCheckSeverity.WARN,
        metadata={"partition": date, "prefix": prefix, "exists": exists},
    )
