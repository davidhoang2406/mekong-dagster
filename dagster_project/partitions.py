from dagster import DailyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(
    start_date="2026-05-01",
    timezone="Asia/Ho_Chi_Minh",
    end_offset=1,
)
