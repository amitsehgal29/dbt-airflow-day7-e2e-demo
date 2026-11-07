"""Invalidate previous run outputs before waiting for this run's input."""
from pathlib import Path

root = Path(__file__).resolve().parents[1] / "published"
root.mkdir(exist_ok=True)
for name in ("_SUCCESS", "metrics.json", "dq_report_raw.json", "dq_report_marts.json"):
    (root / name).unlink(missing_ok=True)
print("Previous readiness marker and per-run reports cleared")
