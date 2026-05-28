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

from dagster_project.resources import KafkaAdminResource, MinioResource


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


_KAFKA_DAEMONS        = ["stock-price-producer", "crypto-price-producer", "storage-consumer"]
_LAG_WARN_THRESHOLD   = 50_000   # absolute lag before alerting
_LAG_DELTA_WARN       = 1_000    # lag growth between ticks that signals a stall
_RESTART_WARN         = 3        # cumulative restarts considered unhealthy


def _load_cursor(raw: str | None) -> dict:
    """Parse sensor cursor, migrating from the old list format."""
    if not raw:
        return {"alerts": [], "prev_lag": -1, "restart_counts": {}}
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return {"alerts": data, "prev_lag": -1, "restart_counts": {}}
        return {
            "alerts":         data.get("alerts", []),
            "prev_lag":       data.get("prev_lag", -1),
            "restart_counts": data.get("restart_counts", {}),
        }
    except (json.JSONDecodeError, AttributeError):
        return {"alerts": [], "prev_lag": -1, "restart_counts": {}}


@sensor(
    name="kafka_pipeline_health_sensor",
    description=(
        "Checks pod phase, container readiness, restart count, and consumer group lag "
        "for all three Kafka pipeline daemons. Alerts via Telegram on state transitions; "
        "deduplicates via cursor so each issue fires only once until it recovers."
    ),
    minimum_interval_seconds=300,
    required_resource_keys={"kafka_admin"},
)
def kafka_pipeline_health_sensor(context: SensorEvaluationContext) -> SensorResult | SkipReason:
    kafka_admin: KafkaAdminResource = context.resources.kafka_admin
    token   = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    cursor_data   = _load_cursor(context.cursor)
    alerted       = set(cursor_data["alerts"])
    prev_lag      = cursor_data["prev_lag"]
    prev_restarts = cursor_data["restart_counts"]

    new_alerts:   list[str] = []
    new_restarts: dict      = {}

    # ── Pod health (phase + readiness + restart count) ────────────────────────
    for name in _KAFKA_DAEMONS:
        health = kafka_admin.pod_health(name)

        current_restarts      = health.restart_count if health else 0
        new_restarts[name]    = current_restarts
        down_key              = f"down:{name}"
        not_ready_key         = f"not_ready:{name}"
        restarted_key         = f"restarted:{name}"

        if health is None or health.phase != "running":
            status_str = health.phase if health else "not found"
            if down_key not in alerted:
                msg = f"⚠️ *Kafka daemon down*\nPod `{name}` status: `{status_str}`."
                if token and chat_id:
                    _send_telegram(token, chat_id, msg)
                context.log.warning("Pod %s status: %s", name, status_str)
                new_alerts.append(down_key)
            alerted.discard(not_ready_key)
            alerted.discard(restarted_key)
            continue

        alerted.discard(down_key)

        # Container ready flag — catches crash-loop before process exits cleanly
        if not health.ready:
            if not_ready_key not in alerted:
                msg = (
                    f"⚠️ *Kafka daemon not ready*\n"
                    f"Pod `{name}` is Running but container is not ready "
                    f"(restarts: `{health.restart_count}`)."
                )
                if token and chat_id:
                    _send_telegram(token, chat_id, msg)
                context.log.warning("Pod %s not ready, restart_count=%d", name, health.restart_count)
                new_alerts.append(not_ready_key)
        else:
            alerted.discard(not_ready_key)

        # Restart count — alert once when threshold crossed; clear only on pod replacement
        prev = prev_restarts.get(name, 0)
        if current_restarts > prev and current_restarts >= _RESTART_WARN:
            if restarted_key not in alerted:
                msg = (
                    f"⚠️ *Kafka daemon restarting*\n"
                    f"Pod `{name}` has restarted `{current_restarts}` times "
                    f"(+{current_restarts - prev} since last check)."
                )
                if token and chat_id:
                    _send_telegram(token, chat_id, msg)
                context.log.warning("Pod %s restart_count=%d (+%d)", name, current_restarts, current_restarts - prev)
                new_alerts.append(restarted_key)
        elif current_restarts < _RESTART_WARN:
            alerted.discard(restarted_key)

    # ── Consumer group lag ────────────────────────────────────────────────────
    lag              = kafka_admin.consumer_group_lag("storage")
    lag_key          = "high_lag:storage"
    lag_growing_key  = "lag_growing:storage"

    if lag >= _LAG_WARN_THRESHOLD:
        if lag_key not in alerted:
            msg = (
                f"⚠️ *Kafka Consumer Lag*\n"
                f"Group `storage` lag: `{lag:,}` messages — "
                f"storage consumer may be falling behind or stalled."
            )
            if token and chat_id:
                _send_telegram(token, chat_id, msg)
            context.log.warning("Storage consumer group lag: %d", lag)
            new_alerts.append(lag_key)
    elif lag >= 0:
        alerted.discard(lag_key)

    # Lag delta — catches a stalling consumer before it hits the absolute threshold
    if lag >= 0 and prev_lag >= 0 and (lag - prev_lag) >= _LAG_DELTA_WARN:
        if lag_growing_key not in alerted:
            msg = (
                f"⚠️ *Consumer Lag Growing*\n"
                f"Group `storage` lag grew by `{lag - prev_lag:,}` since last check "
                f"(now `{lag:,}`) — consumer may be stalled."
            )
            if token and chat_id:
                _send_telegram(token, chat_id, msg)
            context.log.warning("Storage lag growing: %d → %d (+%d)", prev_lag, lag, lag - prev_lag)
            new_alerts.append(lag_growing_key)
    elif lag >= 0 and prev_lag >= 0 and lag <= prev_lag:
        alerted.discard(lag_growing_key)

    alerted.update(new_alerts)
    new_cursor = json.dumps({
        "alerts":         sorted(alerted),
        "prev_lag":       lag if lag >= 0 else prev_lag,
        "restart_counts": new_restarts,
    })

    return SensorResult(
        run_requests=[],
        cursor=new_cursor,
        skip_reason=None if new_alerts else "All Kafka daemons healthy; consumer lag within threshold.",
    )
