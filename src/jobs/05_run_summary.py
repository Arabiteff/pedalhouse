# Databricks notebook source
# MAGIC %md
# MAGIC # Job task · Run summary (`run_if: ALL_DONE`)
# MAGIC
# MAGIC This task depends on BOTH branches of the If/else, and one of them is always
# MAGIC skipped. With the default "all succeeded" rule it would never run. With
# MAGIC **Run if = All done** it runs whatever happened, like a `finally` block, and
# MAGIC logs the run.
# MAGIC
# MAGIC It also shows **dynamic value references**: `{{job.run_id}}` and
# MAGIC `{{job.start_time.iso_datetime}}` are filled in by Lakeflow Jobs at run time.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "pedalhouse_dev")
dbutils.widgets.text("job_run_id", "manual")
dbutils.widgets.text("job_start", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
job_run_id = dbutils.widgets.get("job_run_id")
job_start = dbutils.widgets.get("job_start")

batch_no = int(dbutils.jobs.taskValues.get(taskKey="generate_data", key="batch_no",
                                           default=0, debugValue=0))
quality_ok = dbutils.jobs.taskValues.get(taskKey="quality_gate", key="quality_ok",
                                         default="unknown", debugValue="unknown")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.job_run_log (
      job_run_id  STRING,
      job_start   STRING,
      batch_no    INT,
      quality_ok  STRING,
      logged_at   TIMESTAMP
    )
""")
spark.sql(
    f"INSERT INTO {catalog}.{schema}.job_run_log VALUES "
    "(:job_run_id, :job_start, :batch_no, :quality_ok, current_timestamp())",
    args={"job_run_id": job_run_id, "job_start": job_start,
          "batch_no": batch_no, "quality_ok": str(quality_ok)},
)
print(f"Logged run {job_run_id}: batch {batch_no}, quality_ok={quality_ok}")
