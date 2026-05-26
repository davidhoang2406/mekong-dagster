from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetDep,
    AssetExecutionContext,
    AutomationCondition,
    MetadataValue,
    RetryPolicy,
    TimeWindowPartitionMapping,
    asset,
    asset_check,
)

from dagster_project.partitions import daily_partitions
from dagster_project.resources import MinioResource, SparkClusterResource


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
    metadata={
        "input_path": MetadataValue.text("s3a://market-data/ohlcv.bar/"),
        "output_path": MetadataValue.text("s3a://market-analysis/technical/"),
        "format": MetadataValue.text("Parquet"),
        "spark_job": MetadataValue.text("technical"),
        "indicators": MetadataValue.json({
            "SMA": [20, 50, 200],
            "RSI": {"period": 14},
            "MACD": {"fast": 12, "slow": 26, "signal": 9},
            "BollingerBands": {"period": 20, "std_dev": 2},
        }),
        "lookback_days": MetadataValue.int(200),
    },
)
def technical_indicators(context: AssetExecutionContext, spark: SparkClusterResource) -> None:
    context.log.info("Running TechnicalJob for partition %s", context.partition_key)
    args = ["technical"]
    if context.run.tags.get("full_recompute") == "true":
        args.append("--full-recompute")
        context.log.info("Full recompute requested — ignoring checkpoint")
    spark.submit(args)


@asset_check(
    asset=technical_indicators,
    blocking=False,
    description=(
        "Warns when more than 80% of SMA-200 values are null for the partition date. "
        "Expected early in the platform's life when fewer than 200 days of history exist."
    ),
)
def technical_sma200_completeness(
    context: AssetCheckExecutionContext,
    minio: MinioResource,
) -> AssetCheckResult:
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    from datetime import date

    d = date.fromisoformat(context.partition_key)
    year, month, day = d.strftime("%Y"), d.strftime("%m"), d.strftime("%d")

    table = minio.read_parquet(
        minio.market_analysis_bucket,
        "technical.indicators",
        (ds.field("year") == year) & (ds.field("month") == month) & (ds.field("day") == day),
    )
    total = len(table)
    if total == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={"reason": "partition is empty"},
        )

    null_count  = int(pc.sum(pc.is_null(table.column("sma200"))).as_py() or 0)
    null_rate   = null_count / total
    return AssetCheckResult(
        passed=null_rate < 0.80,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "total_rows":  total,
            "sma200_nulls": null_count,
            "null_rate_pct": round(null_rate * 100, 1),
        },
    )
