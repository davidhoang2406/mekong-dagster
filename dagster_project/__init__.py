import os

from dagster import Definitions, load_asset_checks_from_modules, load_assets_from_modules

from dagster_project.assets import digest, ohlcv, price_snapshots, screener, technical
from dagster_project.resources import KafkaAdminResource, MinioResource, SparkClusterResource
from dagster_project.kafka_pipeline_jobs import (start_kafka_pipeline_job,
                                                  stop_kafka_pipeline_job)
from dagster_project.schedules import (daily_market_close, ohlcv_daily_job,
                                        weekly_screener, weekly_screener_job)
from dagster_project.sensors import (kafka_pipeline_health_sensor,
                                      raw_data_expiry_sensor, telegram_failure_sensor,
                                      telegram_success_sensor)

_asset_modules = [price_snapshots, ohlcv, technical, digest, screener]

defs = Definitions(
    assets=load_assets_from_modules(_asset_modules),
    asset_checks=load_asset_checks_from_modules(_asset_modules),
    resources={
        "minio": MinioResource(
            endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            market_data_bucket=os.getenv("MINIO_BUCKET", "market-data"),
            market_analysis_bucket=os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis"),
        ),
        "spark": SparkClusterResource(
            spark_image=os.getenv("SPARK_IMAGE", "ghcr.io/davidhoang2406/mekong-spark:latest"),
        ),
        "kafka_admin": KafkaAdminResource(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        ),
    },
    jobs=[ohlcv_daily_job, weekly_screener_job, start_kafka_pipeline_job, stop_kafka_pipeline_job],
    schedules=[daily_market_close, weekly_screener],
    sensors=[telegram_failure_sensor, telegram_success_sensor,
             raw_data_expiry_sensor, kafka_pipeline_health_sensor],
)
