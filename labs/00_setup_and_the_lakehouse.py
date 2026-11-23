# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 00 · Welcome to Pedalhouse
# MAGIC
# MAGIC **Pedalhouse** is a (fictional) bike-share company running in **Lisbon**, **Montreal**
# MAGIC and **Seoul**. Every day each city sends its trips, in its own format. The
# MAGIC membership system sends a feed of member changes. The business wants clean,
# MAGIC trustworthy numbers every morning.
# MAGIC
# MAGIC You are the data engineer. Over seven labs you will build their lakehouse:
# MAGIC
# MAGIC | Lab | You practise | Course |
# MAGIC |---|---|---|
# MAGIC | 00 | Setup, what a lakehouse actually is | all |
# MAGIC | 01 | Delta Lake: ACID, history, time travel, restore, constraints, clustering, CDF, governance | foundations |
# MAGIC | 02 | Ingestion: `read_files`, CTAS, COPY INTO, Auto Loader, schema drift, rescued data | Data Ingestion with Lakeflow Connect |
# MAGIC | 03 | Declarative Pipelines: streaming tables, materialized views, multi-flow, expectations, quarantine | Build Data Pipelines with SDP |
# MAGIC | 04 | Change data capture: AUTO CDC, SCD Type 1 and 2, point-in-time joins | Build Data Pipelines with SDP |
# MAGIC | 05 | Lakeflow Jobs: task values, If/else, For each, Run if, repair, triggers, notifications | Deploy Workloads with Lakeflow Jobs |
# MAGIC | 06 | DevOps: Git folders, bundles, dev/prod targets, unit tests, CI/CD | DevOps Essentials for Data Engineering |
# MAGIC | 99 | **Check my work**: an automatic grader for every lab | |
# MAGIC
# MAGIC Runs on **Databricks Free Edition** (serverless). Run the cells from top to bottom.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate the first day of data
# MAGIC
# MAGIC `datagen.generate_batch` simulates one day of operations and writes raw files into
# MAGIC the **landing volume**. Each run writes the next day. It is deliberately messy:
# MAGIC some trips are broken, one station has no GPS, Lisbon changes its file layout from
# MAGIC day 3, and member changes arrive out of order. You will meet each of those later.

# COMMAND ----------

state = datagen.load_state(paths.landing)
if state["next_batch"] == 1:
    summary = datagen.generate_batch(paths.landing)
    print(f"Generated batch {summary['batch_no']} ({summary['day']})")
else:
    print(f"Data already generated up to batch {state['next_batch'] - 1}. Nothing to do.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The "lake" part: plain files in a volume
# MAGIC
# MAGIC A **volume** is a Unity Catalog object that holds files (CSV, JSON, images...).
# MAGIC It is governed like a table: it has an owner and permissions, but the content is
# MAGIC just files. Open **Catalog** in the left sidebar, find your schema, and look at
# MAGIC the `landing` volume. Then run the cells below.

# COMMAND ----------

def tree(path, indent=""):
    for item in sorted(dbutils.fs.ls(path), key=lambda f: f.name):
        size = f"  ({item.size:,} bytes)" if not item.isDir() else ""
        print(f"{indent}{item.name}{size}")
        if item.isDir() and item.name != "_state/":
            tree(item.path, indent + "    ")

tree(paths.landing)

# COMMAND ----------

# Peek at raw files exactly as they arrived: text, nothing parsed yet.
for city, ext in [("lisbon", "csv"), ("seoul", "json")]:
    print(f"--- {city} ---")
    with open(f"{paths.landing}/trips/{city}/batch_0001.{ext}") as f:
        for _ in range(3):
            print(f.readline().rstrip())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The "house" part: query files with SQL, then make a table
# MAGIC
# MAGIC Same files, queried with SQL directly (schema-on-read). No table exists yet.

# COMMAND ----------

display(spark.sql(f"""
    SELECT *
    FROM read_files('{paths.landing}/stations/', format => 'csv', header => true)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Now save it as a **Delta table**. The data is copied into managed storage as
# MAGIC Parquet files plus a transaction log (`_delta_log`). From now on you get ACID
# MAGIC transactions, history, time travel and governance on it.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE lab00_stations
    COMMENT 'Lab 00: my first Delta table'
    AS SELECT * FROM read_files('{paths.landing}/stations/', format => 'csv', header => true)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A table is files + metadata. DESCRIBE DETAIL shows the "files" side.
# MAGIC DESCRIBE DETAIL lab00_stations

# COMMAND ----------

# MAGIC %md
# MAGIC ## What a lakehouse is, in one sentence
# MAGIC
# MAGIC > Cheap, open files in cloud storage (the **lake**) with a transaction log and a
# MAGIC > catalog on top (Delta Lake + Unity Catalog), so they behave like reliable,
# MAGIC > governed tables (the ware**house**), for SQL, Python, BI and AI on the same copy.
# MAGIC
# MAGIC You just saw both halves: files in `landing` and a Delta table built from them.
# MAGIC
# MAGIC **Next:** Lab 01, where you break and repair a Delta table on purpose.
