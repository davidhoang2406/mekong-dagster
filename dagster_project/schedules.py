from datetime import timedelta

from dagster import RunRequest, define_asset_job, schedule, AssetSelection

from dagster_project.partitions import daily_partitions, weekly_partitions

ohlcv_daily_job = define_asset_job(
    name="ohlcv_daily_job",
    selection=AssetSelection.assets("ohlcv_daily_bars"),
    partitions_def=daily_partitions,
)

weekly_screener_job = define_asset_job(
    name="weekly_screener_job",
    selection=AssetSelection.assets("screener_results"),
    partitions_def=weekly_partitions,
)


# Fires at 16:00 Asia/Ho_Chi_Minh (GMT+7) every day.
# Partition key is today's date (the day whose market data just closed).
@schedule(
    cron_schedule="0 16 * * *",
    job=ohlcv_daily_job,
    execution_timezone="Asia/Ho_Chi_Minh",
)
def daily_market_close(context):
    partition_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(partition_key=partition_date)


# Fires at 08:00 Asia/Ho_Chi_Minh every Monday — weekly fundamentals refresh.
# Partition key is the Monday of the previous week (the week whose data just completed).
@schedule(
    cron_schedule="0 8 * * 1",
    job=weekly_screener_job,
    execution_timezone="Asia/Ho_Chi_Minh",
)
def weekly_screener(context):
    prev_monday = context.scheduled_execution_time - timedelta(weeks=1)
    return RunRequest(partition_key=prev_monday.strftime("%Y-%m-%d"))
