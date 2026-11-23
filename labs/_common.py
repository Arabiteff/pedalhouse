# Databricks notebook source
# MAGIC %md
# MAGIC # Shared setup for every lab
# MAGIC
# MAGIC Each lab starts with `%run ./_common`. This notebook:
# MAGIC 1. makes the `pedalhouse` Python package importable
# MAGIC 2. creates two widgets at the top of the notebook: **catalog** and **schema**
# MAGIC 3. creates the schema and volumes if needed (safe to repeat)
# MAGIC 4. points SQL cells at your schema with `USE`, so they can use short table names
# MAGIC
# MAGIC Change the widgets if you want another catalog (for example `learning`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from pedalhouse import datagen  # noqa: E402
from pedalhouse.config import CITIES, LakehousePaths, setup_sql  # noqa: E402

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "pedalhouse_dev", "Schema")

paths = LakehousePaths(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

for _statement in setup_sql(paths):
    spark.sql(_statement)

spark.sql(f"USE CATALOG {paths.catalog}")
spark.sql(f"USE SCHEMA {paths.schema}")

print(f"Working in {paths.fq_schema}")
print(f"Landing volume: {paths.landing}")
