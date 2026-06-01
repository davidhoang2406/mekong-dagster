from datetime import date as _date, timedelta

from dagster import AutomationCondition, DataVersion, DataVersionsByPartition, MetadataValue, observable_source_asset

from dagster_project.partitions import daily_partitions
from dagster_project.resources import MinioResource


@observable_source_asset(
    partitions_def=daily_partitions,
    # Auto-observe at 08:00 UTC (15:00 GMT+7) daily — 1 hour before ohlcv_daily_bars runs.
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
    """Observe all recent partitions and report whether data exists in MinIO."""
    versions: dict[str, DataVersion] = {}

    today = _date.today()
    for offset in range(1, 8):  # yesterday → 7 days ago
        target = today - timedelta(days=offset)
        partition_key = target.isoformat()
        year, month, day = target.strftime("%Y"), target.strftime("%m"), target.strftime("%d")

        prefix = f"price.snapshot/asset_class=stock/year={year}/month={month}/day={day}/"
        exists = minio.partition_exists(minio.market_data_bucket, prefix)
        versions[partition_key] = DataVersion("exists" if exists else "missing")

    return DataVersionsByPartition(versions)
