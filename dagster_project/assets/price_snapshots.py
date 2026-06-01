from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    MetadataValue,
    ObserveResult,
    asset_check,
    observable_source_asset,
)

from dagster_project.partitions import daily_partitions
from dagster_project.resources import MinioResource


@observable_source_asset(
    partitions_def=daily_partitions,
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

    stock_prefix  = f"price.snapshot/asset_class=stock/year={year}/month={month}/day={day}/"
    crypto_prefix = f"price.snapshot/asset_class=crypto/year={year}/month={month}/day={day}/"

    stock_exists  = minio.partition_exists(minio.market_data_bucket, stock_prefix)
    crypto_exists = minio.partition_exists(minio.market_data_bucket, crypto_prefix)

    return ObserveResult(
        metadata={
            "partition_date":   MetadataValue.text(date),
            "stock_exists":     MetadataValue.bool(stock_exists),
            "crypto_exists":    MetadataValue.bool(crypto_exists),
            "minio_prefix":     MetadataValue.text(
                f"s3://{minio.market_data_bucket}/price.snapshot/"
            ),
        },
    )


@asset_check(
    asset=price_snapshots,
    blocking=True,
    description="Fails when stock price snapshots are absent for the partition date.",
)
def price_snapshots_stock_exists(
    context: AssetCheckExecutionContext,
    minio: MinioResource,
) -> AssetCheckResult:
    from datetime import date as _date, timedelta
    date = context.partition_key if context.has_partition_key else (_date.today() - timedelta(days=1)).isoformat()
    year, month, day = date[:4], date[5:7], date[8:10]
    prefix = f"price.snapshot/asset_class=stock/year={year}/month={month}/day={day}/"
    exists = minio.partition_exists(minio.market_data_bucket, prefix)
    return AssetCheckResult(
        passed=exists,
        severity=AssetCheckSeverity.ERROR,
        metadata={"partition": date, "prefix": prefix, "exists": exists},
    )


@asset_check(
    asset=price_snapshots,
    blocking=False,
    description="Warns when crypto price snapshots are absent for the partition date.",
)
def price_snapshots_crypto_exists(
    context: AssetCheckExecutionContext,
    minio: MinioResource,
) -> AssetCheckResult:
    from datetime import date as _date, timedelta
    date = context.partition_key if context.has_partition_key else (_date.today() - timedelta(days=1)).isoformat()
    year, month, day = date[:4], date[5:7], date[8:10]
    prefix = f"price.snapshot/asset_class=crypto/year={year}/month={month}/day={day}/"
    exists = minio.partition_exists(minio.market_data_bucket, prefix)
    return AssetCheckResult(
        passed=exists,
        severity=AssetCheckSeverity.WARN,
        metadata={"partition": date, "prefix": prefix, "exists": exists},
    )
