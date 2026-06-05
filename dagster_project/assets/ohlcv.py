from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetDep,
    AssetExecutionContext,
    MetadataValue,
    RetryPolicy,
    asset,
    asset_check,
)

from dagster_project.partitions import daily_partitions
from dagster_project.resources import MinioResource, SparkClusterResource


@asset(
    partitions_def=daily_partitions,
    deps=[AssetDep("price_snapshots")],
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    group_name="batch_pipeline",
    description="Daily OHLCV bars derived from price snapshots via the ohlcv_daily_ingest Spark job.",
    metadata={
        "input_path": MetadataValue.text("s3a://market-data/price.snapshot/"),
        "output_path": MetadataValue.text("s3a://market-data/ohlcv.bar/"),
        "format": MetadataValue.text("Parquet"),
        "partition_scheme": MetadataValue.text("ohlcv.bar/asset_class={val}/year={y}/month={m}/day={d}/"),
        "spark_job": MetadataValue.text("ohlcv-daily-ingest"),
        "columns": MetadataValue.text("symbol, asset_class, open, high, low, close, volume, date"),
    },
)
def ohlcv_daily_bars(context: AssetExecutionContext, spark: SparkClusterResource) -> None:
    target_date = context.partition_key
    context.log.info("Running OHLCV ingest for partition %s", target_date)
    spark.submit(["ohlcv-daily-ingest", "--date", target_date], logger=context.log)


def _ohlcv_filter(partition_key: str):
    """Return a pyarrow dataset filter expression for a single daily partition."""
    import pyarrow.dataset as ds
    from datetime import date
    d = date.fromisoformat(partition_key)
    return (
        (ds.field("year")  == d.strftime("%Y")) &
        (ds.field("month") == d.strftime("%m")) &
        (ds.field("day")   == d.strftime("%d"))
    )


@asset_check(
    asset=ohlcv_daily_bars,
    blocking=True,
    description="Fails when the OHLCV partition for the given date contains no rows.",
)
def ohlcv_partition_not_empty(
    context: AssetCheckExecutionContext,
    minio: MinioResource,
) -> AssetCheckResult:
    table = minio.read_parquet(
        minio.market_analysis_bucket, "ohlcv.bar", _ohlcv_filter(context.partition_key)
    )
    row_count = len(table)
    return AssetCheckResult(
        passed=row_count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": row_count, "partition": context.partition_key},
    )


@asset_check(
    asset=ohlcv_daily_bars,
    blocking=True,
    description="Fails when any bar has open/high/low/close <= 0 or volume < 0.",
)
def ohlcv_no_invalid_prices(
    context: AssetCheckExecutionContext,
    minio: MinioResource,
) -> AssetCheckResult:
    import pyarrow.compute as pc

    table = minio.read_parquet(
        minio.market_analysis_bucket, "ohlcv.bar", _ohlcv_filter(context.partition_key)
    )
    le = pc.less_equal
    bad_prices = int(pc.sum(
        pc.or_(pc.or_(le(table.column("open"), 0), le(table.column("high"), 0)),
               pc.or_(le(table.column("low"),  0), le(table.column("close"), 0)))
    ).as_py() or 0)
    bad_volume = int(pc.sum(
        pc.less(table.column("volume"), 0)
    ).as_py() or 0)
    total_bad = bad_prices + bad_volume
    return AssetCheckResult(
        passed=total_bad == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "bad_price_rows": bad_prices,
            "bad_volume_rows": bad_volume,
            "total_rows": len(table),
        },
    )


@asset_check(
    asset=ohlcv_daily_bars,
    blocking=True,
    description=(
        "Fails when OHLCV relationships are violated: high < low, "
        "or open/close outside the [low, high] range."
    ),
)
def ohlcv_valid_relationships(
    context: AssetCheckExecutionContext,
    minio: MinioResource,
) -> AssetCheckResult:
    import pyarrow.compute as pc

    table = minio.read_parquet(
        minio.market_analysis_bucket, "ohlcv.bar", _ohlcv_filter(context.partition_key)
    )
    hi, lo, op, cl = (
        table.column("high"), table.column("low"),
        table.column("open"), table.column("close"),
    )
    bad_hl    = int(pc.sum(pc.less(hi, lo)).as_py() or 0)
    bad_open  = int(pc.sum(pc.or_(pc.less(op, lo), pc.greater(op, hi))).as_py() or 0)
    bad_close = int(pc.sum(pc.or_(pc.less(cl, lo), pc.greater(cl, hi))).as_py() or 0)
    total_bad = bad_hl + bad_open + bad_close
    return AssetCheckResult(
        passed=total_bad == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "high_lt_low": bad_hl,
            "open_outside_range": bad_open,
            "close_outside_range": bad_close,
            "total_rows": len(table),
        },
    )
