# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 03 · Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC Course: **Build Data Pipelines with Lakeflow Spark Declarative Pipelines**
# MAGIC
# MAGIC The pipeline code is in `src/pipeline/`. Open those files side by side with this
# MAGIC notebook: they are heavily commented.
# MAGIC
# MAGIC ```
# MAGIC BRONZE (raw)                   SILVER (clean)                        GOLD (business)
# MAGIC stations_bronze  (MV) ───────► stations_silver (MV, DROP) ──┐
# MAGIC trips_bronze     (ST) ───────► trips_flagged (ST, WARN) ────┼─► trips_enriched (ST) ─► gold_station_daily
# MAGIC   ▲ ▲ ▲  3 flows                 ├─► trips_silver (ST)      │   stream-static join    gold_revenue_by_plan
# MAGIC   │ │ └ seoul (JSON)             └─► trips_quarantine (ST)  │                         gold_city_quality
# MAGIC   │ └── montreal (CSV)                                      │                         gold_quarantine_reasons
# MAGIC   └──── lisbon (CSV, FAIL rule on target)                   │
# MAGIC members_cdc_bronze (ST) ─AUTO CDC─► members_current (SCD1) ─┴──────────────────────► gold_members_by_plan
# MAGIC                         └AUTO CDC─► members_history (SCD2) ───────────────────────► (point-in-time join)
# MAGIC ```
# MAGIC ST = streaming table, MV = materialized view.
# MAGIC
# MAGIC **What you will see:** streaming tables vs materialized views in action, a
# MAGIC multi-flow table, all three expectation actions (warn, drop, fail), the quarantine
# MAGIC pattern, a stream-static join, schema drift, the event log, and recovering a
# MAGIC failed pipeline.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the pipeline
# MAGIC
# MAGIC **Option A, in the UI (recommended the first time):**
# MAGIC 1. Sidebar **Jobs & Pipelines > Create > ETL pipeline**.
# MAGIC 2. Name: `pedalhouse-pipeline`. Catalog and schema: the values printed below.
# MAGIC 3. Choose **Add existing assets**. Root folder: the `src/pipeline` folder of this
# MAGIC    repo. Source code paths: the same folder.
# MAGIC 4. Open **Settings > Configuration** and add the two key/value pairs printed below.
# MAGIC 5. Keep **Serverless** on. Save.
# MAGIC
# MAGIC **Option B, as code:** deploy the bundle (Lab 06 explains it). The file
# MAGIC `resources/pedalhouse.pipeline.yml` contains exactly the same settings.
# MAGIC
# MAGIC Free Edition allows one *running* pipeline at a time, which is all we need.

# COMMAND ----------

src_path = os.path.abspath("../src")
print("Catalog              :", paths.catalog)
print("Schema               :", paths.schema)
print("Pipeline source path :", f"{src_path}/pipeline")
print()
print("Configuration (Settings > Configuration > Add):")
print(f"  landing_path = {paths.landing}")
print(f"  src_path     = {src_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. First run
# MAGIC
# MAGIC Make sure at least three batches exist (Lab 02 created them; this cell catches up).
# MAGIC Then click **Run pipeline** in the pipeline editor and watch the graph build.

# COMMAND ----------

while datagen.load_state(paths.landing)["next_batch"] <= 3:
    print("Generated batch", datagen.generate_batch(paths.landing)["batch_no"])
print("Batches available:", datagen.load_state(paths.landing)["next_batch"] - 1)

# COMMAND ----------

# MAGIC %md
# MAGIC When the run is green, look at every table:

# COMMAND ----------

tables = ["stations_bronze", "stations_silver", "trips_bronze", "trips_flagged",
          "trips_quarantine", "trips_silver", "trips_enriched", "members_cdc_bronze",
          "members_current", "members_history", "gold_station_daily", "gold_city_quality",
          "gold_quarantine_reasons", "gold_revenue_by_plan", "gold_members_by_plan"]

def table_counts():
    rows = []
    for t in tables:
        n = spark.table(t).count() if spark.catalog.tableExists(paths.table(t)) else None
        rows.append((t, n))
    return spark.createDataFrame(rows, "table STRING, rows BIGINT")

counts_after_run_1 = table_counts()
display(counts_after_run_1)

# COMMAND ----------

# MAGIC %md
# MAGIC Checks you can make yourself:
# MAGIC * `stations_bronze` = 25, `stations_silver` = 24: the **DROP** expectation removed `SEL-99`.
# MAGIC * `trips_bronze` = `trips_silver` + `trips_quarantine`: the quarantine pattern loses nothing.
# MAGIC * `trips_enriched` = `trips_silver`: the stream-static join added columns, not rows.
# MAGIC
# MAGIC Compare the quarantine with what the generator broke on purpose:

# COMMAND ----------

expected = datagen.expected_totals(paths.landing)
print(f"Generator: {expected['rows']} trips, {expected['bad']} broken on purpose, "
      f"{expected['outliers']} suspiciously long")
display(spark.sql("SELECT city, broken_rule, trips FROM gold_quarantine_reasons ORDER BY city, trips DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Streaming table vs materialized view, live
# MAGIC
# MAGIC A **streaming table** processes each new row once and appends it.
# MAGIC A **materialized view** stores a query result and keeps it correct.
# MAGIC
# MAGIC Experiment: add one more day, run the pipeline again, then read the **event log**,
# MAGIC the pipeline's own journal stored as a table.

# COMMAND ----------

print("Generated batch", datagen.generate_batch(paths.landing)["batch_no"],
      "-> now click Run pipeline again, wait for it to finish, then run the next cell.")

# COMMAND ----------

# How many rows did each flow write in each update?
# Streaming flows (trips_*, members_*) only processed the new day.
display(spark.sql("""
    SELECT timestamp,
           origin.update_id,
           origin.flow_name,
           details:flow_progress.metrics.num_output_rows::BIGINT AS rows_written
    FROM event_log(TABLE(trips_bronze))
    WHERE event_type = 'flow_progress'
      AND details:flow_progress.status = 'COMPLETED'
    ORDER BY timestamp DESC
"""))

# COMMAND ----------

# How was each materialized view refreshed? Look for incremental techniques
# versus a full recompute in the planning information.
display(spark.sql("""
    SELECT timestamp, origin.flow_name, details:planning_information AS planning
    FROM event_log(TABLE(trips_bronze))
    WHERE event_type = 'planning_information'
    ORDER BY timestamp DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Expectations: warn, drop, fail
# MAGIC
# MAGIC | Rule | Where | Action | Effect |
# MAGIC |---|---|---|---|
# MAGIC | `reasonable_duration` | trips_flagged | warn | row kept, counted |
# MAGIC | `no_rescued_data` | trips_flagged | warn | row kept, counted |
# MAGIC | `valid_coordinates` | stations_silver | DROP ROW | row removed, counted |
# MAGIC | `trip_id_present` | trips_bronze (target of 3 flows) | FAIL UPDATE | update stops |
# MAGIC
# MAGIC The results of every expectation are in the event log:

# COMMAND ----------

display(spark.sql("""
    WITH dq AS (
      SELECT explode(from_json(
               details:flow_progress.data_quality.expectations,
               'array<struct<name: string, dataset: string,
                             passed_records: bigint, failed_records: bigint>>'
             )) AS e
      FROM event_log(TABLE(trips_bronze))
      WHERE event_type = 'flow_progress'
    )
    SELECT e.dataset, e.name AS expectation,
           SUM(e.passed_records) AS passed, SUM(e.failed_records) AS failed
    FROM dq
    GROUP BY e.dataset, e.name
    ORDER BY e.dataset, e.name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Drill: a FAIL UPDATE stops the pipeline, then you recover
# MAGIC
# MAGIC Lisbon sends a file where three trips have **no trip_id**. The constraint on
# MAGIC `trips_bronze` says `ON VIOLATION FAIL UPDATE`.
# MAGIC
# MAGIC 1. Run the next cell to drop the bad file.
# MAGIC 2. Run the pipeline. It **fails**. Click the red flow and read the error: it names
# MAGIC    the constraint `trip_id_present`. Nothing from that micro-batch was committed
# MAGIC    (Delta transactions are all-or-nothing).

# COMMAND ----------

poison = datagen.write_poison_file(paths.landing)
print("Bad file written:", poison)

# COMMAND ----------

display(spark.sql("""
    SELECT timestamp, origin.flow_name, level, message
    FROM event_log(TABLE(trips_bronze))
    WHERE level = 'ERROR'
    ORDER BY timestamp DESC
    LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC 3. The provider re-sends the file, corrected, under the same name. Run the next cell,
# MAGIC    then **run the pipeline again**. The failed micro-batch is replayed from the same
# MAGIC    file path, so it now succeeds. No full refresh needed.

# COMMAND ----------

print("Fixed:", datagen.fix_poison_files(paths.landing))

# COMMAND ----------

display(spark.sql("SELECT trip_id, source_file FROM trips_bronze WHERE source_file LIKE 'poison%'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schema drift inside the pipeline
# MAGIC
# MAGIC Lisbon's `helmet_used` column (from batch 3) did not break anything: when Auto Loader
# MAGIC saw the new column, the `trips_lisbon` flow restarted itself with the new schema. Our
# MAGIC flow selects explicit columns, so the new one is ignored until we decide to use it.
# MAGIC Search the event log for the restart:

# COMMAND ----------

display(spark.sql("""
    SELECT timestamp, origin.flow_name, level, message
    FROM event_log(TABLE(trips_bronze))
    WHERE origin.flow_name = 'trips_lisbon' AND level IN ('WARN', 'ERROR')
    ORDER BY timestamp
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Full refresh, and the protected bronze table
# MAGIC
# MAGIC In the pipeline editor, open the dropdown next to **Run pipeline** and choose
# MAGIC **Full refresh all**. Then compare counts with the previous run (next cell).
# MAGIC
# MAGIC `trips_bronze` has `'pipelines.reset.allowed' = 'false'`, so it is **not** truncated:
# MAGIC its raw history is kept. Everything downstream is rebuilt from it. Use
# MAGIC **Full refresh selection** when only one table needs rebuilding.

# COMMAND ----------

display(table_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Settings worth knowing (read, don't change)
# MAGIC
# MAGIC * **Development vs production mode**: development keeps compute warm and does not retry
# MAGIC   on failure, for fast iteration. Bundles switch it per target.
# MAGIC * **Triggered vs continuous**: ours is triggered (process what is new, then stop).
# MAGIC   Continuous keeps running for low latency, at a higher cost. Same code either way.
# MAGIC * **Configuration**: `${landing_path}` in SQL and `spark.conf.get("src_path")` in Python
# MAGIC   come from the pipeline settings, so dev and prod use the same code.
# MAGIC
# MAGIC ## Challenge
# MAGIC
# MAGIC Add a gold materialized view `gold_hourly_demand`: trips per city per hour of the day
# MAGIC (0 to 23) and bike type, from `trips_enriched`. Create the file
# MAGIC `src/pipeline/gold/05_gold_hourly_demand.sql`, run the pipeline, and check it appears
# MAGIC in the graph. Solution below.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Solution: src/pipeline/gold/05_gold_hourly_demand.sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_hourly_demand
# MAGIC COMMENT "Trips per city, hour of day and bike type."
# MAGIC AS SELECT
# MAGIC   city,
# MAGIC   HOUR(started_at) AS hour_of_day,
# MAGIC   bike_type,
# MAGIC   COUNT(*)         AS trips
# MAGIC FROM trips_enriched
# MAGIC GROUP BY city, HOUR(started_at), bike_type;
# MAGIC ```
