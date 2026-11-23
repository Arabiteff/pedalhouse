# Databricks notebook source
# MAGIC %md
# MAGIC # Job task 3 · Quality gate
# MAGIC
# MAGIC Runs after the pipeline. Measures how much of **today's** batch went to
# MAGIC quarantine and publishes the answer as task values. The next task is an
# MAGIC **If/else condition** that reads `quality_ok` and picks a branch:
# MAGIC
# MAGIC * `true`  -> city reports (a For each loop)
# MAGIC * `false` -> quality alert (fails on purpose, so the failure email is sent)

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "pedalhouse_dev")
dbutils.widgets.text("max_bad_pct", "8")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
max_bad_pct = float(dbutils.widgets.get("max_bad_pct"))

# Read a value published by an earlier task. debugValue is used when you run
# this notebook by hand, outside a job.
batch_no = int(dbutils.jobs.taskValues.get(taskKey="generate_data", key="batch_no",
                                           default=1, debugValue=1))
batch_prefix = f"batch_{batch_no:04d}"

# COMMAND ----------

row = spark.sql(f"""
    SELECT COUNT(*)                                          AS total,
           COALESCE(SUM(CASE WHEN is_quarantined THEN 1 ELSE 0 END), 0) AS quarantined
    FROM {catalog}.{schema}.trips_flagged
    WHERE source_file LIKE '{batch_prefix}%'
""").first()

total, quarantined = row["total"], row["quarantined"]
if total == 0:
    raise ValueError(f"No trips found for {batch_prefix}. Did the pipeline task ingest the new files?")

bad_pct = round(100.0 * quarantined / total, 2)
quality_ok = bad_pct <= max_bad_pct

print(f"{batch_prefix}: {quarantined} of {total} trips quarantined ({bad_pct}%), "
      f"threshold {max_bad_pct}% -> quality_ok = {quality_ok}")

# COMMAND ----------

# Task values are rendered as text in {{tasks.quality_gate.values.quality_ok}},
# so publish a plain "true"/"false" string for the condition task to compare.
dbutils.jobs.taskValues.set(key="quality_ok", value="true" if quality_ok else "false")
dbutils.jobs.taskValues.set(key="bad_pct", value=bad_pct)
