import json
import os
import urllib.error
import urllib.request

from dagster import DagsterRunStatus, RunStatusSensorContext, run_status_sensor


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
