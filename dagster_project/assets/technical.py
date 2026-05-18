from dagster import (
    AssetDep,
    AssetExecutionContext,
    AutomationCondition,
    RetryPolicy,
    TimeWindowPartitionMapping,
    asset,
)

from dagster_project.partitions import daily_partitions
from dagster_project.resources import SparkClusterResource


@asset(
    partitions_def=daily_partitions,
    deps=[
        AssetDep(
            "ohlcv_daily_bars",
            partition_mapping=TimeWindowPartitionMapping(start_offset=-200, end_offset=0),
        )
    ],
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    automation_condition=AutomationCondition.eager(),
    group_name="batch_pipeline",
    description=(
        "SMA-20/50/200, RSI-14, MACD(12/26/9), and Bollinger Bands computed over "
        "up to 200 days of OHLCV data via the TechnicalJob Spark job."
    ),
)
def technical_indicators(context: AssetExecutionContext, spark: SparkClusterResource) -> None:
    context.log.info("Running TechnicalJob for partition %s", context.partition_key)
    spark.submit(["technical"])
