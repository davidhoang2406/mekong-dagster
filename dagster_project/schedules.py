from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from dagster_project.partitions import daily_partitions, weekly_partitions

ohlcv_daily_job = define_asset_job(
    name="ohlcv_daily_job",
    selection=AssetSelection.assets("ohlcv_daily_bars"),
    partitions_def=daily_partitions,
)

# Fires at 16:00 Asia/Ho_Chi_Minh on weekdays — 1 hour after HOSE closes at 15:00.
daily_market_close = ScheduleDefinition(
    name="daily_market_close",
    cron_schedule="0 16 * * 1-5",
    job=ohlcv_daily_job,
    execution_timezone="Asia/Ho_Chi_Minh",
)

weekly_screener_job = define_asset_job(
    name="weekly_screener_job",
    selection=AssetSelection.assets("screener_results"),
    partitions_def=weekly_partitions,
)

# Fires at 08:00 Asia/Ho_Chi_Minh every Monday — weekly fundamentals refresh.
weekly_screener = ScheduleDefinition(
    name="weekly_screener",
    cron_schedule="0 8 * * 1",
    job=weekly_screener_job,
    execution_timezone="Asia/Ho_Chi_Minh",
)
