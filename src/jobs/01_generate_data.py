# Databricks notebook source
# MAGIC %md
# MAGIC # Job task 1 · Generate one day of raw data
# MAGIC
# MAGIC Simulates the three cities sending yesterday's files. In a real company this
# MAGIC task would not exist: files would arrive from the source systems. Here it makes
# MAGIC the whole project self-contained.
# MAGIC
# MAGIC **Task values it publishes** (read by later tasks):
# MAGIC
# MAGIC | key | example | used by |
# MAGIC |---|---|---|
# MAGIC | `batch_no` | `4` | quality gate, run summary |
# MAGIC | `cities` | `["lisbon","montreal","seoul"]` | the **For each** task |

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))  # this notebook lives in src/jobs, the package in src/

from pedalhouse.config import LakehousePaths, setup_sql
from pedalhouse.datagen import generate_batch

# Job parameters are pushed down to every task as widgets with the same name.
# The defaults below are only used when you run this notebook by hand.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "pedalhouse_dev")
dbutils.widgets.text("trips_per_city", "250")
dbutils.widgets.text("bad_rate", "0.04")

paths = LakehousePaths(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
trips_per_city = int(dbutils.widgets.get("trips_per_city"))
bad_rate = float(dbutils.widgets.get("bad_rate"))

# COMMAND ----------

# Idempotent setup: safe to run every day.
for statement in setup_sql(paths):
    spark.sql(statement)

summary = generate_batch(paths.landing, trips_per_city=trips_per_city, bad_rate=bad_rate)

print(f"Batch {summary['batch_no']} for {summary['day']} written to {paths.landing}")
for city, c in summary["cities"].items():
    print(f"  {city:<9} {c['rows']:>4} trips, {c['bad']:>3} deliberately bad, file {c['file']}")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="batch_no", value=summary["batch_no"])
dbutils.jobs.taskValues.set(key="cities", value=sorted(summary["cities"]))
