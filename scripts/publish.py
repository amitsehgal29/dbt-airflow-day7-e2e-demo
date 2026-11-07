"""Publish local metrics, then a readiness marker bound to this Airflow run."""
import json
import os
from pathlib import Path
import duckdb

root = Path(__file__).resolve().parents[1]
with duckdb.connect(str(root / "dbt/shop.duckdb"), read_only=True) as con:
    rows, revenue = con.execute("select count(*), sum(amount_aed) from fct_orders").fetchone()
metrics = {"run_id": os.environ["DEMO_RUN_ID"], "orders": rows, "revenue_aed": str(revenue)}
for stage in ("raw", "marts"):
    report = json.loads((root / f"published/dq_report_{stage}.json").read_text())
    assert report["errors"] == 0, f"{stage} gate must pass before publication"
(root / "published/metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
(root / "published/_SUCCESS").write_text(metrics["run_id"] + "\n")
print(json.dumps(metrics))
