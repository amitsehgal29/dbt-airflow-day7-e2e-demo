"""Query real warehouse + Airflow states; assert the expected demonstration result."""
import argparse
import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def capture(label):
    with duckdb.connect(str(ROOT / "dbt/shop.duckdb"), read_only=True) as con:
        rows, revenue, pending = con.execute("""
            select count(*), sum(amount_aed),
                   100.0 * count(*) filter (where order_status='pending') / count(*)
            from fct_orders
        """).fetchone()
        history = con.execute("""
            select customer_id, city, tier, cast(dbt_valid_from as varchar),
                   cast(dbt_valid_to as varchar)
            from snap_customers where customer_id in ('C002', 'C003')
            order by customer_id, dbt_valid_from
        """).fetchall()
        snapshot_rows, current_rows = con.execute("""
            select count(*), count(*) filter (where dbt_valid_to is null) from snap_customers
        """).fetchone()
        raw_rows = con.execute("select count(*) from raw_orders").fetchone()[0]
        order_1007 = con.execute("select order_status from fct_orders where order_id=1007").fetchone()[0]
        unique_orders = con.execute("select count(distinct order_id) from fct_orders").fetchone()[0]
    with sqlite3.connect(ROOT / "airflow_home/airflow.db") as con:
        run_id, state = con.execute("""
            select run_id, state from dag_run where dag_id='shop_analytics_e2e'
            order by id desc limit 1
        """).fetchone()
        tasks = dict(con.execute("""
            select task_id, state from task_instance
            where dag_id='shop_analytics_e2e' and run_id=?
        """, (run_id,)))
    reports = {}
    for stage in ("raw", "marts"):
        path = ROOT / f"published/dq_report_{stage}.json"
        reports[stage] = json.loads(path.read_text()) if path.exists() else None
    alert_file = ROOT / "alerts/alerts.log"
    alerts = [json.loads(line) for line in alert_file.read_text().splitlines()] if alert_file.exists() else []
    result = {
        "label": label, "run_id": run_id, "dag_state": state, "task_states": tasks,
        "raw_rows": raw_rows, "orders": rows, "order_1007_status": order_1007, "revenue_aed": str(revenue),
        "pending_percent": pending, "snapshot_rows": snapshot_rows,
        "current_snapshot_rows": current_rows,
        "history_C002_C003": history, "gates": reports,
        "published": (ROOT / "published/_SUCCESS").exists(),
        "alerts": [a for a in alerts if a["run_id"] == run_id],
    }
    expected = {"batch1": (8, Decimal("2201.00"), 3, 3),
                "batch2": (10, Decimal("2464.75"), 6, 4),
                "duplicate": (10, Decimal("2464.75"), 6, 4)}[label]
    assert (rows, revenue, snapshot_rows, current_rows) == expected, result
    assert unique_orders == rows, result
    assert order_1007 == ("pending" if label == "batch1" else "completed"), result
    expected_history = [
        ("C002", "Abu Dhabi", "bronze", "2026-10-01 00:00:00", None),
        ("C003", "Sharjah", "gold", "2026-10-01 00:00:00", None),
    ] if label == "batch1" else [
        ("C002", "Abu Dhabi", "bronze", "2026-10-01 00:00:00", "2026-10-10 08:00:00"),
        ("C002", "Dubai", "bronze", "2026-10-10 08:00:00", None),
        ("C003", "Sharjah", "gold", "2026-10-01 00:00:00", "2026-10-12 14:30:00"),
        ("C003", "Sharjah", "platinum", "2026-10-12 14:30:00", None),
    ]
    assert history == expected_history, result
    if label == "duplicate":
        assert state == "failed" and tasks["dq_gate_raw"] == "failed", result
        for task in ("dbt_run", "dbt_snapshot", "dbt_test", "dq_gate_marts", "publish"):
            assert tasks[task] == "upstream_failed", (task, tasks)
        assert not result["published"] and reports["marts"] is None, result
        assert reports["raw"]["errors"] == 1, result
        assert any(a["kind"] == "FAILED" and a["task_id"] == "dq_gate_raw" for a in result["alerts"]), result
    else:
        assert state == "success" and all(s == "success" for s in tasks.values()), result
        assert all(r["errors"] == 0 for r in reports.values()), result
        assert (ROOT / "published/_SUCCESS").read_text().strip() == run_id, result
        tests = json.loads((ROOT / "dbt/target/run_results.json").read_text())["results"]
        assert len(tests) == 14 and all(t["status"] == "pass" for t in tests), tests
        result["dbt_tests_passed"] = len(tests)
    out = ROOT / "published/demo"
    out.mkdir(exist_ok=True)
    (out / f"{label}.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def display(result):
    print(f"\n{result['label']}: DAG={result['dag_state']} orders={result['orders']} "
          f"revenue={result['revenue_aed']} AED pending={result['pending_percent']:g}% "
          f"snapshot={result['snapshot_rows']} ({result['current_snapshot_rows']} current) "
          f"published={result['published']}")
    for stage, report in result["gates"].items():
        print(f"  {stage} gate: " + (f"{len(report['results'])} rules, {report['errors']} errors, "
              f"{report['warnings']} warnings" if report else "NOT RUN"))
    for row in result["history_C002_C003"]:
        print("  history:", *row)
    for alert in result["alerts"]:
        print("  alert:", json.dumps(alert))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("label", choices=["batch1", "batch2", "duplicate", "all"])
    args = parser.parse_args()
    if args.label == "all":
        for label in ("batch1", "batch2", "duplicate"):
            display(json.loads((ROOT / f"published/demo/{label}.json").read_text()))
    else:
        display(capture(args.label))
