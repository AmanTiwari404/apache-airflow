"""
learning_pipeline.py
====================================================================
A complete, self-contained Airflow tutorial DAG: a simulated
"daily sales report" pipeline.

Read this file top-to-bottom — every non-obvious line is commented.

Pipeline shape (arrows = dependencies):

    wait_for_file            (FileSensor  — wait until sales.csv exists)
          |
    extract_data             (PythonOperator — read CSV -> XCom)
        /     \\
 validate_data  calculate_summary   (run IN PARALLEL)
        \\     /
    branch_on_sales          (BranchPythonOperator — pick a path)
        /        \\
 high_sales_alert  normal_sales_log  (BashOperator — only ONE runs)
        \\        /
     write_report            (PythonOperator — write JSON to /output/)

Concepts demonstrated: FileSensor, PythonOperator, BashOperator,
XCom, parallelism, branching, retries, @daily schedule, catchup=False.
"""

from __future__ import annotations  # lets us use modern type hints on old pythons

import csv          # standard-library CSV reader (no pandas needed)
import json         # to write the final report
import os           # to build filesystem paths
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
#  Paths INSIDE the container. These map to your host folders via the volume
#  mounts in docker-compose.yml (./sample_data and ./output).
# ---------------------------------------------------------------------------
DATA_DIR = "/opt/airflow/sample_data"
OUTPUT_DIR = "/opt/airflow/output"
CSV_PATH = os.path.join(DATA_DIR, "sales.csv")

# ---------------------------------------------------------------------------
#  default_args are applied to EVERY task in the DAG unless a task overrides
#  them. This is the idiomatic place to configure retries.
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,                          # each task retries once on failure...
    "retry_delay": timedelta(seconds=30),  # ...waiting 30s before the retry
}


# ===========================================================================
#  TASK FUNCTIONS
#  Each Python function below becomes the body of one PythonOperator task.
#  The `**context` / `ti` argument gives access to the task instance, which
#  is how we push and pull XComs (Airflow's cross-task data channel).
# ===========================================================================

def extract_data(ti, **context):
    """Read sales.csv and push the parsed rows to XCom for downstream tasks."""
    rows = []
    with open(CSV_PATH, newline="") as f:
        # DictReader turns each CSV line into a dict keyed by the header row.
        for row in csv.DictReader(f):
            rows.append({
                "product": row["product"],
                "quantity": int(row["quantity"]),   # CSV values are strings...
                "price": float(row["price"]),        # ...so cast to numbers here
            })
    print(f"Extracted {len(rows)} rows from {CSV_PATH}")
    # `return` from a PythonOperator is automatically pushed to XCom under the
    # key "return_value". Downstream tasks pull it with ti.xcom_pull(...).
    return rows


def validate_data(ti, **context):
    """Sanity-check the extracted rows. Runs in PARALLEL with calculate_summary."""
    # Pull the list of rows that extract_data returned (its task_id is the key).
    rows = ti.xcom_pull(task_ids="extract_data")
    if not rows:
        # Raising an exception fails the task (and triggers its retry).
        raise ValueError("Validation failed: no rows extracted!")
    for r in rows:
        if r["quantity"] < 0 or r["price"] < 0:
            raise ValueError(f"Validation failed: negative value in {r}")
    print(f"Validation passed: {len(rows)} rows look good.")
    return "valid"


def calculate_summary(ti, **context):
    """Compute total sales + per-product totals. Runs in PARALLEL with validate."""
    rows = ti.xcom_pull(task_ids="extract_data")
    total_sales = 0.0
    per_product = {}
    for r in rows:
        line_total = r["quantity"] * r["price"]  # revenue for this line item
        total_sales += line_total
        per_product[r["product"]] = per_product.get(r["product"], 0.0) + line_total
    summary = {
        "total_sales": round(total_sales, 2),
        "line_items": len(rows),
        "per_product": {k: round(v, 2) for k, v in per_product.items()},
    }
    print(f"Summary: {summary}")
    # Push the whole summary dict so write_report can use it later.
    return summary


def choose_branch(ti, **context):
    """
    BranchPythonOperator callback.

    IMPORTANT: it must RETURN the task_id (or list of task_ids) of the branch
    to run next. Every other directly-downstream task is skipped automatically.
    """
    summary = ti.xcom_pull(task_ids="calculate_summary")
    total = summary["total_sales"]
    print(f"Total sales = {total}; deciding which alert to send...")
    if total > 1000:
        return "high_sales_alert"   # <- must match the task_id exactly
    return "normal_sales_log"


def write_report(ti, **context):
    """Write the final JSON report to /output/. Has EXTRA retries configured."""
    summary = ti.xcom_pull(task_ids="calculate_summary")
    # `ds` is the DAG run's logical date as a YYYY-MM-DD string (from context).
    run_date = context["ds"]
    os.makedirs(OUTPUT_DIR, exist_ok=True)  # ensure the folder exists
    report = {
        "report_date": run_date,
        "generated_by": "learning_pipeline",
        "summary": summary,
        "status": "HIGH_SALES" if summary["total_sales"] > 1000 else "NORMAL_SALES",
    }
    out_path = os.path.join(OUTPUT_DIR, f"sales_report_{run_date}.json")
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Report written to {out_path}")
    return out_path


# ===========================================================================
#  DAG DEFINITION
#  The `with DAG(...) as dag:` block is where tasks are declared and wired.
# ===========================================================================
with DAG(
    dag_id="learning_pipeline",
    description="A daily sales report pipeline that teaches core Airflow concepts.",
    default_args=default_args,
    # start_date must be in the PAST for the DAG to ever be scheduled.
    start_date=datetime(2024, 1, 1),
    schedule="@daily",       # run once per day (Airflow 2.4+ arg name)
    catchup=False,           # do NOT back-fill every day since start_date
    max_active_runs=1,       # only one run of this DAG at a time
    tags=["learning", "tutorial", "sales"],
) as dag:

    # --- Task 1: wait for the input file -----------------------------------
    # FileSensor "pokes" the filesystem until the file appears, then succeeds.
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=CSV_PATH,           # absolute path (uses the default fs_default conn)
        poke_interval=10,            # check every 10 seconds
        timeout=120,                 # give up (fail) after 2 minutes of waiting
        mode="reschedule",           # free the worker slot between pokes (efficient)
    )

    # --- Task 2: extract (PythonOperator) ----------------------------------
    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    # --- Task 3a + 3b: validate & summarise (run in PARALLEL) --------------
    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )
    summarize = PythonOperator(
        task_id="calculate_summary",
        python_callable=calculate_summary,
    )

    # --- Task 4: branch (BranchPythonOperator) -----------------------------
    branch = BranchPythonOperator(
        task_id="branch_on_sales",
        python_callable=choose_branch,
    )

    # --- Task 5a + 5b: the two branches (BashOperator) ---------------------
    # Only ONE of these runs each execution — the branch task decides which.
    high_sales_alert = BashOperator(
        task_id="high_sales_alert",
        bash_command='echo "🚀 HIGH SALES alert! Total sales exceeded 1000."',
    )
    normal_sales_log = BashOperator(
        task_id="normal_sales_log",
        bash_command='echo "📊 Normal sales day. Nothing unusual to report."',
    )

    # --- Task 6: write the report (extra retries) --------------------------
    report = PythonOperator(
        task_id="write_report",
        python_callable=write_report,
        retries=3,                          # override default_args: try harder here
        retry_delay=timedelta(seconds=15),
        # Because one upstream branch is always SKIPPED, the default trigger
        # rule ("all_success") would block this task. This rule runs it as long
        # as nothing failed and at least one parent succeeded.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # =======================================================================
    #  DEPENDENCIES — the `>>` operator means "then".
    # =======================================================================
    # File must exist before we extract.
    wait_for_file >> extract
    # After extract, fan OUT to two parallel tasks.
    extract >> [validate, summarize]
    # Both parallel tasks must finish before we branch (fan IN).
    [validate, summarize] >> branch
    # Branch chooses one of the two alert tasks...
    branch >> [high_sales_alert, normal_sales_log]
    # ...and both alert branches lead into the final report task.
    [high_sales_alert, normal_sales_log] >> report
