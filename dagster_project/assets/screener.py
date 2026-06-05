from dagster import AssetExecutionContext, MetadataValue, RetryPolicy, asset

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
    metadata={
        "data_source": MetadataValue.text("vnstock Finance API (VCI)"),
        "output_path": MetadataValue.text("s3a://market-analysis/screener/"),
        "format": MetadataValue.text("Parquet"),
        "partition_scheme": MetadataValue.text("screener/year={y}/week={w}/"),
        "spark_job": MetadataValue.text("screener"),
        "config_file": MetadataValue.text("mekong-jobs/config/screener.json"),
        "thresholds": MetadataValue.json({
            "pe_ratio": "<= 20",
            "roe": ">= 12%",
            "eps": ">= 1000",
            "de_ratio": "<= 2.0",
        }),
    },
)
def screener_results(context: AssetExecutionContext, spark: SparkClusterResource) -> None:
    target_date = context.partition_key  # Monday of the week (YYYY-MM-DD)
    context.log.info("Running ScreenerJob for week of %s", target_date)
    spark.submit(["screener", "--date", target_date], logger=context.log)
