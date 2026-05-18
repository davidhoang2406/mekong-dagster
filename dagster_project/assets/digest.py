from dagster import AssetDep, AssetExecutionContext, AutomationCondition, RetryPolicy, asset

from dagster_project.partitions import daily_partitions
from dagster_project.resources import SparkClusterResource


@asset(
    partitions_def=daily_partitions,
    deps=[AssetDep("ohlcv_daily_bars")],
    automation_condition=AutomationCondition.eager(),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    group_name="batch_pipeline",
    description="Daily digest of top gainers, losers, and volume leaders derived from OHLCV bars.",
)
def daily_digest(context: AssetExecutionContext, spark: SparkClusterResource) -> None:
    target_date = context.partition_key
    context.log.info("Running DigestJob for partition %s", target_date)
    spark.submit(["digest", "--date", target_date])
