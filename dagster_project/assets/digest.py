from dagster import AssetDep, AssetExecutionContext, AutomationCondition, MetadataValue, RetryPolicy, asset

from dagster_project.partitions import daily_partitions
from dagster_project.resources import SparkClusterResource


@asset(
    partitions_def=daily_partitions,
    deps=[AssetDep("ohlcv_daily_bars")],
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    group_name="batch_pipeline",
    description="Daily digest of top gainers, losers, and volume leaders derived from OHLCV bars.",
    metadata={
        "input_path": MetadataValue.text("s3a://market-data/ohlcv.bar/"),
        "output_path": MetadataValue.text("s3a://market-analysis/digest/"),
        "format": MetadataValue.text("Parquet"),
        "partition_scheme": MetadataValue.text("digest/year={y}/month={m}/day={d}/"),
        "spark_job": MetadataValue.text("digest"),
        "config_file": MetadataValue.text("mekong-jobs/config/digest.json"),
        "rankings": MetadataValue.text("top_gainers, top_losers, top_volume (default top_n=10 each)"),
    },
)
def daily_digest(context: AssetExecutionContext, spark: SparkClusterResource) -> None:
    target_date = context.partition_key
    context.log.info("Running DigestJob for partition %s", target_date)
    spark.submit(["digest", "--date", target_date])
