from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from dagster_project.partitions import daily_partitions

ohlcv_daily_job = define_asset_job(
    name="ohlcv_daily_job",
    selection=AssetSelection.assets("ohlcv_daily_bars"),
)

# Fires at 16:00 Asia/Ho_Chi_Minh on weekdays — 1 hour after HOSE closes at 15:00.
daily_market_close = ScheduleDefinition(
    name="daily_market_close",
    cron_schedule="0 16 * * 1-5",
    job=ohlcv_daily_job,
    execution_timezone="Asia/Ho_Chi_Minh",
)
