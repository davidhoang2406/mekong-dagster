from dagster import AssetExecutionContext, MetadataValue, ObserveResult, observable_source_asset

from dagster_project.partitions import daily_partitions
from dagster_project.resources import MinioResource


@observable_source_asset(
    partitions_def=daily_partitions,
    group_name="batch_pipeline",
    description="Raw price snapshots (Avro) produced by StorageConsumer — external to Dagster.",
)
def price_snapshots(context: AssetExecutionContext, minio: MinioResource) -> ObserveResult:
    date       = context.partition_key
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
