from dagster import AssetExecutionContext, AutomationCondition, MetadataValue, ObserveResult, observable_source_asset

from dagster_project.partitions import daily_partitions
from dagster_project.resources import MinioResource


@observable_source_asset(
    partitions_def=daily_partitions,
    # Auto-observe at 15:00 GMT+7 daily — 1 hour before ohlcv_daily_bars runs.
    # This gives eager() on downstream assets a valid observation to evaluate against.
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
def price_snapshots(context: AssetExecutionContext, minio: MinioResource) -> ObserveResult:
    from datetime import date as _date, timedelta
    date = context.partition_key if context.has_partition_key else (_date.today() - timedelta(days=1)).isoformat()
    year, month, day = date[:4], date[5:7], date[8:10]

    prefix = f"price.snapshot/asset_class=stock/year={year}/month={month}/day={day}/"
    exists = minio.partition_exists(minio.market_data_bucket, prefix)

    return ObserveResult(
        metadata={
            "partition_date":   MetadataValue.text(date),
            "partition_exists": MetadataValue.bool(exists),
            "minio_prefix":     MetadataValue.text(
                f"s3://{minio.market_data_bucket}/price.snapshot/"
            ),
        },
    )
