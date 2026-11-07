"""Local-only alert callback stubs; no messages or webhooks are sent."""
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path


def _send(kind, context):
    ti = context["task_instance"]
    msg = {
        "kind": kind, "dag_id": ti.dag_id, "task_id": ti.task_id,
        "run_id": context.get("run_id"), "try_number": ti.try_number,
        "error": str(context.get("exception"))[:500],
        "at": datetime.now(ZoneInfo("Asia/Dubai")).isoformat(timespec="seconds"),
    }
    directory = Path(os.environ.get("ALERT_DIR", Path(__file__).resolve().parents[1] / "alerts"))
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "alerts.log").open("a") as stream:
        stream.write(json.dumps(msg) + "\n")
    print(f"ALERT {kind}: {msg['dag_id']}.{msg['task_id']} try {msg['try_number']}")


def on_failure(context):
    _send("FAILED", context)


def on_retry(context):
    _send("RETRY", context)
