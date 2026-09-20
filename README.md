# Airflow Learning Project 🪁

A complete, runnable Apache Airflow environment (Docker Compose, **CeleryExecutor**)
with one hands-on DAG that teaches the core concepts by running a real
**daily sales report pipeline** you can watch execute live in the UI.

## What's inside

```
airflow-learning/
├── docker-compose.yml     # webserver, scheduler, worker, postgres, redis, init
├── .env.example           # template: AIRFLOW_UID, Fernet key, admin creds, image version
├── Makefile               # make start / stop / logs / trigger shortcuts
├── README.md              # you are here
├── dags/
│   └── learning_pipeline.py   # the fully-commented tutorial DAG
├── sample_data/
│   └── sales.csv          # input file the FileSensor waits for
└── output/                # the DAG writes sales_report_<date>.json here
```

## The pipeline

```
wait_for_file  ──►  extract_data  ──►  ┌ validate_data ┐  ──►  branch_on_sales ──► ┌ high_sales_alert ┐ ──► write_report
 (FileSensor)      (PythonOperator)    └ calculate_summary┘   (Branch decides)     └ normal_sales_log ┘   (JSON to /output)
                                        (run in parallel)                           (only one runs)
```

Concepts you'll see: **FileSensor**, **PythonOperator**, **BashOperator**,
**XCom** (passing data between tasks), **parallel** tasks, **BranchPythonOperator**,
**retries**, `@daily` schedule with `catchup=False`.

---

## Prerequisites

- Docker + Docker Compose installed (`docker compose version` should work)
- ~4 GB RAM free for Docker
- Create your local env file from the template —
  ```bash
  cp .env.example .env
  ```
  Then edit `.env` and set your own `AIRFLOW_FERNET_KEY` and admin password.
  `.env` is gitignored, so your credentials stay local.
- On **Linux / WSL2**: set your user id so files stay editable —
  ```bash
  echo "AIRFLOW_UID=$(id -u)" >> .env    # optional but recommended on Linux
  ```
  (On macOS/Windows the default `50000` is fine.)

---

## 1. Start everything

From inside the `airflow-learning/` folder:

```bash
docker compose up -d          # or:  make start
```

The first run downloads images and initialises the database (~2–4 min).
Check progress until the webserver is **healthy**:

```bash
docker compose ps             # or:  make status
```

You want `airflow-webserver` to show `(healthy)`.

## 2. Open the UI

Go to **http://localhost:8080**

- **Username:** `airflow`
- **Password:** `airflow`

You'll see the `learning_pipeline` DAG in the list.

## 3. Trigger the DAG

The `sample_data/sales.csv` file already exists, so the FileSensor will pass
immediately. Trigger a run one of two ways:

**From the UI:** toggle the DAG **on** (the switch on the left), then click the
▶ **Trigger DAG** button on the right.

**From the command line:**

```bash
make trigger
# equivalent to:
#   docker compose exec airflow-scheduler airflow dags unpause learning_pipeline
#   docker compose exec airflow-scheduler airflow dags trigger learning_pipeline
```

## 4. Watch it run

In the UI, click the DAG → **Grid** or **Graph** view. Refresh and watch tasks
turn from white → running (light green) → success (dark green). You'll see
`validate_data` and `calculate_summary` run side-by-side, and only one of
`high_sales_alert` / `normal_sales_log` execute (the other is greyed-out
**skipped** — that's the branch working).

## 5. View logs

**In the UI:** click any task box → **Logs**.

**From the command line:**

```bash
make logs        # follow the scheduler logs
# or a specific task's log via the CLI:
docker compose exec airflow-scheduler \
  airflow tasks logs learning_pipeline write_report <execution_date>
```

## 6. See the output

The final task writes a JSON report to the `output/` folder on your host:

```bash
cat output/sales_report_*.json
```

With the bundled `sales.csv`, total sales = **4140.00**, so the pipeline takes
the **HIGH_SALES** branch.

## 7. Stop everything

```bash
docker compose down          # or:  make stop      (keeps the database)
docker compose down -v       # or:  make clean     (full reset, wipes the DB)
```

---

## Experiments to try

- **Trigger the branch's other path:** edit `sample_data/sales.csv` so the total
  is under 1000 (e.g. one cheap row), trigger again, and watch `normal_sales_log`
  run instead.
- **Watch the sensor wait:** rename `sales.csv`, trigger the DAG, and see
  `wait_for_file` stay in the running/up-for-reschedule state. Put the file back
  and it proceeds. (It fails after the 120s timeout if the file never appears.)
- **Break a task on purpose:** put a negative price in the CSV — `validate_data`
  will fail, retry once (per `default_args`), then fail the run.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Port 8080 already in use | Stop the other service, or change `"8080:8080"` in `docker-compose.yml`. |
| Webserver never healthy | Give it more RAM in Docker settings; check `docker compose logs airflow-init`. |
| Permission errors on `logs/`/`output/` | On Linux set `AIRFLOW_UID=$(id -u)` in `.env`, then `make clean && make start`. |
| DAG not showing up | Wait ~30s (scheduler scan interval) or check `docker compose logs airflow-scheduler` for import errors. |
