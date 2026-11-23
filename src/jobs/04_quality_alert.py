# Databricks notebook source
# MAGIC %md
# MAGIC # Job task · Quality alert (the **false** branch of the If/else)
# MAGIC
# MAGIC Records the incident in `quality_alerts`, then **fails on purpose**. A failed
# MAGIC task makes the job run fail, which triggers the job's `on_failure` email
# MAGIC notification. That is how a data quality problem reaches a human.
# MAGIC
# MAGIC To see this branch: run the job with `bad_rate = 0.25`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "pedalhouse_dev")
dbutils.widgets.text("max_bad_pct", "8")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
max_bad_pct = float(dbutils.widgets.get("max_bad_pct"))

batch_no = int(dbutils.jobs.taskValues.get(taskKey="generate_data", key="batch_no",
                                           default=0, debugValue=0))
bad_pct = float(dbutils.jobs.taskValues.get(taskKey="quality_gate", key="bad_pct",
                                            default=0.0, debugValue=0.0))

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.quality_alerts (
      batch_no     INT,
      bad_pct      DOUBLE,
      threshold    DOUBLE,
      raised_at    TIMESTAMP
    )
""")
spark.sql(f"""
    INSERT INTO {catalog}.{schema}.quality_alerts
    VALUES ({batch_no}, {bad_pct}, {max_bad_pct}, current_timestamp())
""")

raise RuntimeError(
    f"Quality gate failed for batch {batch_no}: {bad_pct}% of trips quarantined "
    f"(threshold {max_bad_pct}%). See {catalog}.{schema}.gold_quarantine_reasons."
)
