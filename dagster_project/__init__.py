import os

from dagster import Definitions, load_assets_from_modules

from dagster_project.assets import ohlcv, price_snapshots, technical
from dagster_project.resources import MinioResource, SparkClusterResource
from dagster_project.schedules import daily_market_close, ohlcv_daily_job

defs = Definitions(
    assets=load_assets_from_modules([price_snapshots, ohlcv, technical]),
    resources={
        "minio": MinioResource(
            endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            market_data_bucket=os.getenv("MINIO_BUCKET", "market-data"),
            market_analysis_bucket=os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis"),
        ),
        "spark": SparkClusterResource(
            container_name=os.getenv("SPARK_CONTAINER_NAME", "spark-master"),
        ),
    },
    jobs=[ohlcv_daily_job],
    schedules=[daily_market_close],
)
