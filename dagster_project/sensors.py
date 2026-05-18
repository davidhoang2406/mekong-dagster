import json
import os
import urllib.error
import urllib.request
from datetime import date, timedelta

from dagster import (
    DagsterRunStatus,
    RunStatusSensorContext,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    run_status_sensor,
    sensor,
)

from dagster_project.resources import MinioResource


def _send_telegram(token: str, chat_id: str, text: str) -> None:
    payload = json.dumps({"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Telegram API error {e.code}: {e.read().decode()}") from e


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    name="telegram_on_failure",
    description="Sends a Telegram message when any Dagster job run fails.",
)
def telegram_failure_sensor(context: RunStatusSensorContext) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        context.log.warning("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — skipping notification")
        return

    run = context.dagster_run
    partition = run.tags.get("dagster/partition", "—")
    dagster_url = os.getenv("DAGSTER_WEBSERVER_URL", "http://localhost:3000")

    text = (
        f"❌ *Dagster job failed*\n"
        f"*Job:* `{run.job_name}`\n"
        f"*Partition:* `{partition}`\n"
        f"*Run ID:* `{run.run_id[:8]}`\n"
        f"*Dashboard:* {dagster_url}/runs/{run.run_id}"
    )

    _send_telegram(token, chat_id, text)
    context.log.info("Telegram failure alert sent for run %s", run.run_id[:8])


@sensor(
    name="raw_data_expiry_sensor",
    description=(
        "Checks the last 7 days for raw price data missing a _SUCCESS marker or OHLCV output. "
        "Alerts via Telegram when either is absent, preventing silent data loss before the "
        "30-day MinIO lifecycle policy expires the raw Avro files."
    ),
    minimum_interval_seconds=3600,
    required_resource_keys={"minio"},
)
def raw_data_expiry_sensor(context: SensorEvaluationContext) -> SensorResult | SkipReason:
    minio: MinioResource = context.resources.minio
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    raw_bucket      = os.getenv("MINIO_BUCKET", "market-data")
    analysis_bucket = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")

    # Cursor is a JSON list of already-alerted keys, e.g. ["no_marker:2026-05-17", ...]
    # Prune keys older than 10 days so the cursor doesn't grow unboundedly.
    cutoff = (date.today() - timedelta(days=10)).isoformat()
    alerted: set[str] = {
        k for k in json.loads(context.cursor or "[]")
        if k.split(":")[-1] >= cutoff
    }

    new_alert_keys: list[str] = []
    today = date.today()

    for offset in range(1, 8):  # yesterday → 7 days ago
        target = today - timedelta(days=offset)
        ds     = target.isoformat()
        year, month, day = target.strftime("%Y"), target.strftime("%m"), target.strftime("%d")

        raw_prefix = f"price.snapshot/year={year}/month={month}/day={day}/"
        if not minio.partition_exists(raw_bucket, raw_prefix):
            continue  # no raw data for this date — nothing at risk

        marker_key     = f"_SUCCESS/year={year}/month={month}/day={day}"
        no_marker_key  = f"no_marker:{ds}"
        no_ohlcv_key   = f"no_ohlcv:{ds}"
        days_remaining = 30 - offset

        if not minio.object_exists(raw_bucket, marker_key):
            if no_marker_key not in alerted:
                text = (
                    f"⚠️ *Raw Data Expiry Risk*\n"
                    f"*Date:* `{ds}`\n"
                    f"Raw price snapshots exist but OHLCV ingest has not completed.\n"
                    f"Data expires in *{days_remaining} days* — run `ohlcv_daily_bars` for this partition."
                )
                if token and chat_id:
                    _send_telegram(token, chat_id, text)
                context.log.warning("No _SUCCESS marker for %s", ds)
                new_alert_keys.append(no_marker_key)
        else:
            # Marker present — verify downstream OHLCV output also exists.
            ohlcv_prefix = f"ohlcv.bar/asset_class=stock/year={year}/month={month}/day={day}/"
            if not minio.partition_exists(analysis_bucket, ohlcv_prefix):
                if no_ohlcv_key not in alerted:
                    text = (
                        f"⚠️ *Missing OHLCV Output*\n"
                        f"*Date:* `{ds}`\n"
                        f"`_SUCCESS` marker exists but OHLCV bars were not written.\n"
                        f"Check the `ohlcv_daily_bars` asset in Dagster."
                    )
                    if token and chat_id:
                        _send_telegram(token, chat_id, text)
                    context.log.warning("_SUCCESS marker present but no OHLCV output for %s", ds)
                    new_alert_keys.append(no_ohlcv_key)

    alerted.update(new_alert_keys)

    if not new_alert_keys:
        return SensorResult(
            run_requests=[],
            cursor=json.dumps(sorted(alerted)),
            skip_reason="All recent dates have _SUCCESS markers and OHLCV output.",
        )

    return SensorResult(
        run_requests=[],
        cursor=json.dumps(sorted(alerted)),
    )
