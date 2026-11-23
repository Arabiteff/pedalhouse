# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 05 · Lakeflow Jobs: orchestrate the whole lakehouse
# MAGIC
# MAGIC Course: **Deploy Workloads with Lakeflow Jobs**
# MAGIC
# MAGIC ```
# MAGIC generate_data ─► run_pipeline ─► quality_gate ─► is_quality_ok?  (If/else)
# MAGIC  (notebook)      (pipeline)       (notebook)       ├─ true  ─► city_reports    (For each city)
# MAGIC                                                    └─ false ─► quality_alert   (fails on purpose)
# MAGIC                                       run_summary  (Run if: ALL_DONE, runs whatever happened)
# MAGIC ```
# MAGIC
# MAGIC Part A: build a small job by hand to learn the UI.
# MAGIC Part B: deploy and run the full `pedalhouse-daily` job, then break it in useful ways.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A · Build a two-task job by hand (10 minutes)
# MAGIC
# MAGIC 1. **Jobs & Pipelines > Create > Job**. Name it `my-first-pedalhouse-job`.
# MAGIC 2. Task 1: key `generate_data`, type **Notebook**, path `src/jobs/01_generate_data`.
# MAGIC    Compute: **Serverless**.
# MAGIC 3. Task 2: **+ Add task > Pipeline**, key `run_pipeline`, pick your `pedalhouse-pipeline`.
# MAGIC    Depends on: `generate_data`.
# MAGIC 4. Job details panel > **Job parameters**: add `catalog` and `schema` with your values.
# MAGIC    Job parameters are pushed down to every task as widgets with the same name.
# MAGIC 5. **Run now**. Open the run, click each task, read its output.
# MAGIC 6. Open the job's **⋮ > Edit as YAML** (or "View as code"): this is the same format the
# MAGIC    bundle uses in `resources/`. UI and code are two views of one definition.
# MAGIC
# MAGIC ## Part B · Deploy `pedalhouse-daily` from the bundle
# MAGIC
# MAGIC Lab 06 explains bundles in depth. For now, the fastest route:
# MAGIC 1. Open this repo's root folder in the workspace (it must be a Git folder or contain `databricks.yml`).
# MAGIC 2. Open `databricks.yml`, then the **Deployments** panel (rocket icon) on the left.
# MAGIC 3. Target **dev**, click **Deploy**. You get a job `[dev <you>] pedalhouse-daily`, a
# MAGIC    second job with a file arrival trigger, and a pipeline `[dev <you>] pedalhouse-pipeline`.
# MAGIC
# MAGIC Or from a terminal: `databricks bundle deploy -t dev`.
# MAGIC
# MAGIC The cell below finds the deployed job with the **Databricks SDK**: jobs are also an API.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
daily_jobs = [j for j in w.jobs.list() if j.settings and "pedalhouse-daily" in (j.settings.name or "")]
for j in daily_jobs:
    print(j.job_id, j.settings.name)
if not daily_jobs:
    print("No pedalhouse-daily job yet. Deploy the bundle (Part B), then run this cell again.")
job_id = daily_jobs[0].job_id if daily_jobs else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1 · The happy path
# MAGIC
# MAGIC Run the job with its defaults (button **Run now**, or the cell below). While it runs,
# MAGIC open the run page and watch:
# MAGIC * `generate_data` publishes **task values** `batch_no` and `cities` (Output tab).
# MAGIC * `is_quality_ok` is a **condition task**: it shows `true`.
# MAGIC * `city_reports` is a **For each** task: open it to see one iteration per city,
# MAGIC   built from the `cities` task value. Concurrency is 3.
# MAGIC * `quality_alert` is **Excluded** (the false branch).
# MAGIC * `run_summary` still runs, because its rule is **Run if: All done**.

# COMMAND ----------

def run_daily(**job_parameters):
    """Start pedalhouse-daily with optional job parameter overrides, wait, print each task."""
    if job_id is None:
        raise ValueError("Deploy the bundle first (Part B).")
    params = {"catalog": paths.catalog, "schema": paths.schema, **job_parameters}
    run = w.jobs.run_now_and_wait(job_id=job_id, job_parameters=params)
    print(f"Run {run.run_id}: {run.state.result_state}  {run.run_page_url}")
    for task in sorted(run.tasks or [], key=lambda t: t.start_time or 0):
        print(f"  {task.task_key:<15} {task.state.result_state}")
    return run

# Uncomment to run from here instead of the UI:
# run_daily()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_daily_report ORDER BY ride_date DESC, city

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2 · Take the false branch (and get an email)
# MAGIC
# MAGIC Click **Run now with different parameters** and set `bad_rate` to `0.25`.
# MAGIC * The quality gate measures about 25% quarantined trips, above `max_bad_pct = 8`.
# MAGIC * The condition is `false`: `city_reports` is skipped, `quality_alert` runs and **fails
# MAGIC   on purpose**.
# MAGIC * The job fails, so `email_notifications.on_failure` sends you an email.
# MAGIC * `run_summary` still runs (All done) and logs the run.

# COMMAND ----------

# run_daily(bad_rate="0.25")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM quality_alerts ORDER BY raised_at DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3 · Repair run
# MAGIC
# MAGIC 1. Run with `fail_city = seoul`. Only the **Seoul iteration** of the For each fails;
# MAGIC    Lisbon and Montreal succeed.
# MAGIC 2. On the failed run page, click **Repair run**. Change the job parameter `fail_city`
# MAGIC    to `none` in the repair dialog.
# MAGIC 3. Notice what is selected: the failed task and what depends on it. The data
# MAGIC    generation and the pipeline are **not** run again. That is the point of repair:
# MAGIC    you don't pay twice for work that succeeded.

# COMMAND ----------

# run_daily(fail_city="seoul")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4 · Triggers
# MAGIC
# MAGIC * **Schedule:** open `pedalhouse-daily` > Schedules & Triggers. It runs daily at 06:30
# MAGIC   UTC but is **paused** (dev mode pauses everything). Look at the cron, don't unpause it.
# MAGIC * **File arrival:** open `pedalhouse-on-file-arrival`. Its trigger watches the landing
# MAGIC   volume's `trips/` folder. Unpause it, then run the next cell to drop a new day of
# MAGIC   files. Within a few minutes, the job starts by itself and refreshes the pipeline.
# MAGIC   **Pause it again afterwards** to save compute.
# MAGIC
# MAGIC | Trigger | Use when |
# MAGIC |---|---|
# MAGIC | Scheduled | data arrives at known times |
# MAGIC | File arrival | files land at unpredictable times |
# MAGIC | Table update | a job should run when a source table changes |
# MAGIC | Continuous | the job must always be running |

# COMMAND ----------

print("Generated batch", datagen.generate_batch(paths.landing)["batch_no"], "- watch the file arrival job.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5 · Read the run log
# MAGIC
# MAGIC `run_summary` received `{{job.run_id}}` and `{{job.start_time.iso_datetime}}`,
# MAGIC **dynamic value references** filled in at run time.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM job_run_log ORDER BY logged_at DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC | Feature | Where in pedalhouse-daily |
# MAGIC |---|---|
# MAGIC | Notebook and pipeline tasks | `generate_data`, `run_pipeline` |
# MAGIC | Job parameters | `catalog`, `schema`, `bad_rate`, `fail_city`... |
# MAGIC | Task values | `batch_no`, `cities`, `quality_ok`, `bad_pct` |
# MAGIC | If/else condition | `is_quality_ok` |
# MAGIC | For each | `city_reports` over `{{tasks.generate_data.values.cities}}` |
# MAGIC | Run if | `run_summary`: ALL_DONE |
# MAGIC | Repair run | Exercise 3 |
# MAGIC | Notifications | `on_failure` email |
# MAGIC | Schedule, file arrival trigger | Exercise 4 |
# MAGIC | Serverless performance target | `performance_target: STANDARD` in the YAML |
# MAGIC
# MAGIC **Challenge:** add a **SQL task** to your hand-made job from Part A that runs
# MAGIC `SELECT * FROM gold_station_daily ORDER BY trips DESC LIMIT 10` on your SQL warehouse
# MAGIC after the pipeline. Then add a **dashboard** on the gold tables and a dashboard task.
