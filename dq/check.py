"""Run one stage of data-quality rules (dq/rules.yml) against the DuckDB file.

Usage: python3 dq/check.py <stage> [--db PATH] [--drill duplicate|bad_amount]
Exit code 0 = gate passed (warnings allowed), 1 = at least one error rule failed.
--drill injects bad rows into raw_orders first, to show the gate stopping the DAG.
The next `dbt seed` reloads raw_orders, so a drill never sticks.
"""
import argparse
import json
import os
import sys

import duckdb
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))


def passes(value, expect: str) -> bool:
    parts = expect.split()
    if parts[0] == "between":
        return float(parts[1]) <= value <= float(parts[2])
    op, target = parts[0], float(parts[1])
    return {"==": value == target, ">=": value >= target, ">": value > target,
            "<=": value <= target, "<": value < target}[op]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("stage")
    ap.add_argument("--db", default=os.path.join(HERE, "..", "dbt", "shop.duckdb"))
    ap.add_argument("--rules", default=os.path.join(HERE, "rules.yml"))
    ap.add_argument("--drill", choices=["", "duplicate", "bad_amount"], default="")
    args = ap.parse_args()

    rules = yaml.safe_load(open(args.rules))[args.stage]
    con = duckdb.connect(args.db)

    if args.drill == "duplicate":
        con.execute("insert into raw_orders select * from raw_orders where order_id = '1001'")
        print("DRILL: inserted a duplicate of order 1001 into raw_orders")
    elif args.drill == "bad_amount":
        con.execute("update raw_orders set amount_aed = 'N/A' where order_id = '1004'")
        print("DRILL: set amount_aed = 'N/A' on order 1004")

    errors, warnings, results = 0, 0, []
    for r in rules:
        value = con.execute(r["sql"]).fetchone()[0]
        value = float(value) if value is not None else 0.0
        ok = passes(value, r["expect"])
        status = "PASS" if ok else ("FAIL" if r["severity"] == "error" else "WARN")
        errors += status == "FAIL"
        warnings += status == "WARN"
        results.append({"rule": r["name"], "value": round(value, 2), "expect": r["expect"], "status": status})
        print(f"{status:4}  {r['name']:<40} value={value:g}  expect {r['expect']}")

    out_dir = os.path.join(HERE, "..", "published")
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"dq_report_{args.stage}.json"), "w") as f:
        json.dump({"stage": args.stage, "errors": errors, "warnings": warnings, "results": results}, f, indent=2)

    print(f"\n{args.stage} gate: {len(rules)} rules, {errors} errors, {warnings} warnings "
          f"-> {'BLOCKED' if errors else 'PASSED'}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
