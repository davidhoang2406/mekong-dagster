from dagster import AssetDep, AssetExecutionContext, MetadataValue, RetryPolicy, asset

from dagster_project.partitions import daily_partitions
from dagster_project.resources import SparkClusterResource


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
    spark.submit(["ohlcv-daily-ingest", "--date", target_date])
