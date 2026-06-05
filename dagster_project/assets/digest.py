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
    deps=[AssetDep("ohlcv_daily_bars")],
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
    spark.submit(["digest", "--date", target_date], logger=context.log)


@asset_check(
    asset=daily_digest,
    blocking=False,
    description=(
        "Warns when any ranking category (gainer, loser, volume) is absent from the digest partition. "
        "An empty category means no data met the filter criteria for that day."
    ),
)
def digest_all_categories_present(
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
        "digest",
        (ds.field("year") == year) & (ds.field("month") == month) & (ds.field("day") == day),
    )

    expected = {"gainer", "loser", "volume"}
    if len(table) == 0:
        present = set()
    else:
        present = set(pc.unique(table.column("category")).to_pylist())

    missing = sorted(expected - present)
    counts  = {cat: int(pc.sum(pc.equal(table.column("category"), cat)).as_py() or 0)
               for cat in expected} if len(table) > 0 else {cat: 0 for cat in expected}

    return AssetCheckResult(
        passed=len(missing) == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "missing_categories": str(missing) if missing else "none",
            **{f"{cat}_count": counts[cat] for cat in sorted(expected)},
        },
    )
