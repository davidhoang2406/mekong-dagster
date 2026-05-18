from dagster import AssetExecutionContext, RetryPolicy, asset

from dagster_project.partitions import weekly_partitions
from dagster_project.resources import SparkClusterResource


@asset(
    partitions_def=weekly_partitions,
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    group_name="batch_pipeline",
    description=(
        "Weekly fundamental screener: fetches P/E, ROE, EPS, D/E ratios from vnstock "
        "and filters tracked stock symbols against config-driven thresholds."
    ),
)
def screener_results(context: AssetExecutionContext, spark: SparkClusterResource) -> None:
    target_date = context.partition_key  # Monday of the week (YYYY-MM-DD)
    context.log.info("Running ScreenerJob for week of %s", target_date)
    spark.submit(["screener", "--date", target_date])
