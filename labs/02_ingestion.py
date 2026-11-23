# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 02 · Ingestion: four ways to get files into Delta
# MAGIC
# MAGIC Course: **Data Ingestion with Lakeflow Connect**
# MAGIC
# MAGIC | Method | Loads | Re-run safe? | You use it here for |
# MAGIC |---|---|---|---|
# MAGIC | `read_files` | nothing, it just queries files | n/a | exploring raw data |
# MAGIC | `CREATE TABLE AS` (CTAS) | everything, every time | no, full reload | small reference data (stations) |
# MAGIC | `COPY INTO` | only files not loaded yet | yes, idempotent | Montreal trips |
# MAGIC | **Auto Loader** | only new files, streaming, scales to millions | yes, checkpoint | Lisbon trips + schema drift |
# MAGIC
# MAGIC In Lab 03 the same Auto Loader engine runs inside a Declarative Pipeline as
# MAGIC `STREAM read_files(...)`.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# Make sure day 1 exists (in case you skipped Lab 00).
if datagen.load_state(paths.landing)["next_batch"] == 1:
    datagen.generate_batch(paths.landing)
print("Batches generated so far:", datagen.load_state(paths.landing)["next_batch"] - 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `read_files`: query files before loading anything
# MAGIC
# MAGIC Seoul sends **nested** JSON. Dot notation reaches inside the structs.

# COMMAND ----------

display(spark.sql(f"""
    SELECT trip_id,
           rider.member_id,
           rider.rider_type,
           bike.type          AS bike_type,
           route.from_station,
           times.start        AS started_at
    FROM read_files('{paths.landing}/trips/seoul/', format => 'json')
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. CTAS: a one-shot full load
# MAGIC
# MAGIC Perfect for small reference data. `schemaHints` fixes the types you care about,
# MAGIC and `_metadata` exposes information about the source file of every row.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE stations_ctas
    COMMENT 'Lab 02: stations loaded with CTAS (full reload every run)'
    AS SELECT *,
              _metadata.file_name              AS source_file,
              _metadata.file_modification_time AS file_modified_at
    FROM read_files('{paths.landing}/stations/',
                    format => 'csv', header => true,
                    schemaHints => 'lat DOUBLE, lon DOUBLE, capacity INT')
""")
display(spark.table("stations_ctas"))

# COMMAND ----------

# MAGIC %md
# MAGIC Notice `SEL-99`: a station without coordinates. Remember it, the pipeline will
# MAGIC drop it with an expectation in Lab 03.
# MAGIC
# MAGIC ## 3. COPY INTO: incremental and idempotent
# MAGIC
# MAGIC Create an empty table with no columns: the first load defines the schema
# MAGIC (`mergeSchema`). COPY INTO remembers which files it already loaded.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS trips_montreal_copy
# MAGIC COMMENT 'Lab 02: Montreal trips loaded with COPY INTO'

# COMMAND ----------

def copy_montreal():
    result = spark.sql(f"""
        COPY INTO trips_montreal_copy
        FROM (
          SELECT *, _metadata.file_name AS source_file
          FROM '{paths.landing}/trips/montreal/'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')
    """)
    display(result)

copy_montreal()

# COMMAND ----------

# MAGIC %md
# MAGIC **Run the same COPY INTO again.** `num_inserted_rows` should be **0**: the file is
# MAGIC already loaded, so nothing is duplicated.

# COMMAND ----------

copy_montreal()

# COMMAND ----------

# MAGIC %md
# MAGIC A new day arrives. Generate batch 2, then COPY INTO once more: only the new file loads.

# COMMAND ----------

summary = datagen.generate_batch(paths.landing)
print(f"Generated batch {summary['batch_no']}")
copy_montreal()

display(spark.sql("""
    SELECT source_file, COUNT(*) AS rows
    FROM trips_montreal_copy GROUP BY source_file ORDER BY source_file
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Auto Loader: streaming ingestion with schema evolution
# MAGIC
# MAGIC Auto Loader (`format("cloudFiles")`) discovers new files and processes each one
# MAGIC exactly once, tracked by a **checkpoint**. With `trigger(availableNow=True)` it runs
# MAGIC like a batch job: process everything new, then stop. That is how it is usually
# MAGIC scheduled in jobs.
# MAGIC
# MAGIC Things to notice in the code:
# MAGIC * `cloudFiles.schemaLocation`: where Auto Loader stores the inferred schema
# MAGIC * `cloudFiles.schemaHints`: types we insist on
# MAGIC * `_rescued_data`: values that do not fit the schema are kept here, not lost
# MAGIC * the city is taken from the **file path**, through `_metadata.file_path`

# COMMAND ----------

from pyspark.sql import functions as F

lisbon_source = f"{paths.landing}/trips/lisbon/"
lisbon_checkpoint = f"{paths.checkpoints}/lab02_lisbon_autoloader"

def run_lisbon_autoloader():
    stream = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("header", "true")
              .option("cloudFiles.schemaLocation", lisbon_checkpoint)
              .option("cloudFiles.schemaHints",
                      "started_at TIMESTAMP, ended_at TIMESTAMP, trip_id STRING, member_id STRING")
              .option("pathGlobFilter", "batch_*.csv")
              .load(lisbon_source)
              .withColumn("city", F.regexp_extract("_metadata.file_path", r"/trips/([a-z]+)/", 1))
              .withColumn("source_file", F.col("_metadata.file_name"))
              .withColumn("ingested_at", F.current_timestamp()))

    query = (stream.writeStream
             .option("checkpointLocation", lisbon_checkpoint)
             .option("mergeSchema", "true")
             .trigger(availableNow=True)
             .toTable("trips_lisbon_autoloader"))
    query.awaitTermination()
    print("Rows in trips_lisbon_autoloader:", spark.table("trips_lisbon_autoloader").count())

run_lisbon_autoloader()

# COMMAND ----------

# Run it again: no new files, so no new rows. The checkpoint remembers.
run_lisbon_autoloader()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema drift
# MAGIC
# MAGIC From day 3, Lisbon's system adds a column: `helmet_used`. Generate batch 3 and run
# MAGIC the stream. With the default mode (`addNewColumns`), Auto Loader **stops** on purpose
# MAGIC the first time it sees the new column, after recording it in the schema location.
# MAGIC That is expected: it would rather stop than silently change your table.

# COMMAND ----------

summary = datagen.generate_batch(paths.landing)
print(f"Generated batch {summary['batch_no']} (schema drift: {summary['schema_drift']})")

try:
    run_lisbon_autoloader()
except Exception as error:
    print("The stream stopped, as expected:")
    print("   ", str(error).splitlines()[0][:300])
else:
    print("No stop this time: the stream already knew helmet_used "
          "(it first ran after batch 3 existed). Move on to the next cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC Now simply **run it again**. The schema was already updated, so the stream restarts
# MAGIC with the new column. In a job, a retry policy does this automatically. In a
# MAGIC Declarative Pipeline, the pipeline restarts the flow for you.

# COMMAND ----------

run_lisbon_autoloader()
display(spark.sql("""
    SELECT source_file,
           COUNT(*)                     AS rows,
           COUNT(helmet_used)           AS rows_with_helmet_info
    FROM trips_lisbon_autoloader
    GROUP BY source_file ORDER BY source_file
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rescued data
# MAGIC
# MAGIC Some Lisbon trips have `ended_at = 'not-a-time'`. The hint says TIMESTAMP, so the
# MAGIC column is NULL, and the original text is in `_rescued_data` (as JSON, with the
# MAGIC source file). Nothing was lost, and nothing failed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trip_id, started_at, ended_at, _rescued_data
# MAGIC FROM trips_lisbon_autoloader
# MAGIC WHERE _rescued_data IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Lakeflow Connect in the UI (do this by hand)
# MAGIC
# MAGIC Not every ingestion is code. Try the **standard connector** for file upload:
# MAGIC
# MAGIC 1. Download `stations.csv`: Catalog > your schema > Volumes > `landing` > `stations`,
# MAGIC    click the file, then **Download**.
# MAGIC 2. Sidebar **New > Add or upload data > Create or modify a table**.
# MAGIC 3. Drop the file, choose your catalog and schema, name the table `stations_ui`.
# MAGIC 4. Look at the preview: you can change column types before creating the table.
# MAGIC 5. Click **Create table**, then run the cell below.
# MAGIC
# MAGIC **Managed connectors** (Salesforce, SQL Server, Workday, Google Analytics...) are in
# MAGIC **New > Add or upload data** too. They need credentials for the source system and
# MAGIC are usually not available on Free Edition, so here you only look at the list.

# COMMAND ----------

if spark.catalog.tableExists(paths.table("stations_ui")):
    display(spark.table("stations_ui"))
else:
    print("stations_ui not found yet. Follow the steps above, then run this cell again.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap and challenge
# MAGIC
# MAGIC * `read_files` explores, CTAS reloads everything, COPY INTO and Auto Loader load only new files.
# MAGIC * Auto Loader keeps state in a checkpoint and a schema location, and handles schema drift.
# MAGIC * `_rescued_data` keeps bad values, `_metadata` tells you where each row came from.
# MAGIC
# MAGIC **Challenge:** write an Auto Loader stream for **Seoul** (JSON) into
# MAGIC `trips_seoul_autoloader`, flattening `rider`, `bike`, `route` and `times` into the same
# MAGIC columns as Lisbon. Hint: `try_to_timestamp`. Solution in the next cell.

# COMMAND ----------

seoul_checkpoint = f"{paths.checkpoints}/lab02_seoul_autoloader"
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", seoul_checkpoint)
    .load(f"{paths.landing}/trips/seoul/")
    .select(
        F.col("trip_id"),
        F.col("rider.member_id").alias("member_id"),
        F.col("bike.bike_id").alias("bike_id"),
        F.col("bike.type").alias("bike_type"),
        F.col("route.from_station").alias("start_station_id"),
        F.col("route.to_station").alias("end_station_id"),
        F.expr("try_to_timestamp(times.start)").alias("started_at"),
        F.expr("try_to_timestamp(times.end)").alias("ended_at"),
        F.col("_metadata.file_name").alias("source_file"),
    )
    .writeStream
    .option("checkpointLocation", seoul_checkpoint)
    .trigger(availableNow=True)
    .toTable("trips_seoul_autoloader")
    .awaitTermination())
display(spark.table("trips_seoul_autoloader").limit(10))
