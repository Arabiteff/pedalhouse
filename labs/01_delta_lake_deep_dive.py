# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 01 · Delta Lake: break it, travel in time, repair it
# MAGIC
# MAGIC Pedalhouse keeps a small table of its bike **fleet**. You will use it to see, with
# MAGIC your own eyes, what Delta Lake adds on top of plain Parquet files:
# MAGIC
# MAGIC 1. ACID writes: INSERT, UPDATE, DELETE, MERGE, each one a new **version**
# MAGIC 2. `DESCRIBE HISTORY` and **time travel**
# MAGIC 3. An accident, then `RESTORE`
# MAGIC 4. **Schema enforcement** (bad writes are rejected) and **schema evolution**
# MAGIC 5. `CHECK` constraints
# MAGIC 6. **Liquid clustering** and `OPTIMIZE`
# MAGIC 7. **Change Data Feed**: which rows changed between versions
# MAGIC 8. Unity Catalog governance: comments, tags, grants, column masks, row filters
# MAGIC
# MAGIC Cells marked `[databricks-only]` use Unity Catalog features. Everything else also
# MAGIC runs on open-source Spark + Delta (the project's tests run this notebook locally).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the table
# MAGIC
# MAGIC `CLUSTER BY (city)` turns on **liquid clustering**: Delta keeps rows of the same city
# MAGIC close together in the files, so a filter on `city` can skip (prune) whole files.
# MAGIC It replaces the old choice between `PARTITIONED BY` and `ZORDER`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bikes (
# MAGIC   bike_id       STRING  NOT NULL COMMENT 'Unique bike identifier',
# MAGIC   city          STRING           COMMENT 'Home city of the bike',
# MAGIC   bike_type     STRING           COMMENT 'classic or electric',
# MAGIC   battery_pct   INT              COMMENT 'Battery level, NULL for classic bikes',
# MAGIC   status        STRING           COMMENT 'available, maintenance or retired',
# MAGIC   last_service  DATE
# MAGIC )
# MAGIC CLUSTER BY (city)
# MAGIC COMMENT 'Lab 01: the Pedalhouse fleet'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Four kinds of writes, four new versions

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bikes VALUES
# MAGIC   ('LIS-B101', 'lisbon',   'electric', 82,   'available', DATE'2026-10-01'),
# MAGIC   ('LIS-B102', 'lisbon',   'classic',  NULL, 'available', DATE'2026-09-12'),
# MAGIC   ('LIS-B103', 'lisbon',   'electric', 12,   'available', DATE'2026-08-30'),
# MAGIC   ('MTL-B201', 'montreal', 'classic',  NULL, 'retired',   DATE'2025-04-02'),
# MAGIC   ('MTL-B202', 'montreal', 'electric', 64,   'available', DATE'2026-10-20'),
# MAGIC   ('SEL-B301', 'seoul',    'electric', 9,    'available', DATE'2026-07-15')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bikes VALUES
# MAGIC   ('SEL-B302', 'seoul',    'classic',  NULL, 'available', DATE'2026-10-02'),
# MAGIC   ('MTL-B203', 'montreal', 'electric', 91,   'available', DATE'2026-11-01')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATE: low batteries go to maintenance
# MAGIC UPDATE bikes SET status = 'maintenance' WHERE battery_pct < 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE: retired bikes leave the fleet
# MAGIC DELETE FROM bikes WHERE status = 'retired'

# COMMAND ----------

# MAGIC %md
# MAGIC **MERGE** = upsert in one atomic statement. The workshop sends a report: two bikes were
# MAGIC serviced (update them) and one new bike arrived (insert it).

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bikes AS target
# MAGIC USING (
# MAGIC   SELECT * FROM VALUES
# MAGIC     ('LIS-B103', 'lisbon', 'electric', 100, 'available', DATE'2026-11-22'),
# MAGIC     ('SEL-B301', 'seoul',  'electric', 100, 'available', DATE'2026-11-22'),
# MAGIC     ('LIS-B104', 'lisbon', 'electric', 100, 'available', DATE'2026-11-22')
# MAGIC   AS report(bike_id, city, bike_type, battery_pct, status, last_service)
# MAGIC ) AS source
# MAGIC ON target.bike_id = source.bike_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   battery_pct  = source.battery_pct,
# MAGIC   status       = source.status,
# MAGIC   last_service = source.last_service
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bikes ORDER BY bike_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. History and time travel
# MAGIC
# MAGIC Every write above created a new **version** in the transaction log. Look at the
# MAGIC `operation` and `operationMetrics` columns: Delta records what each write did.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bikes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The table as it was right after the first insert
# MAGIC SELECT * FROM bikes VERSION AS OF 1 ORDER BY bike_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How the fleet size changed, version by version
# MAGIC SELECT 1 AS version, COUNT(*) AS bikes FROM bikes VERSION AS OF 1
# MAGIC UNION ALL SELECT 2, COUNT(*) FROM bikes VERSION AS OF 2
# MAGIC UNION ALL SELECT 4, COUNT(*) FROM bikes VERSION AS OF 4
# MAGIC UNION ALL SELECT 5, COUNT(*) FROM bikes VERSION AS OF 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The accident, and RESTORE
# MAGIC
# MAGIC An intern runs a DELETE without the right filter: every electric bike disappears.
# MAGIC Note the current version first, then cause the accident.

# COMMAND ----------

version_before_accident = spark.sql("DESCRIBE HISTORY bikes").agg({"version": "max"}).first()[0]
spark.sql("DELETE FROM bikes WHERE bike_type = 'electric'")
print(f"Version before the accident: {version_before_accident}")
print(f"Bikes left after the accident: {spark.table('bikes').count()}")

# COMMAND ----------

spark.sql(f"RESTORE TABLE bikes TO VERSION AS OF {version_before_accident}")
print(f"Bikes after RESTORE: {spark.table('bikes').count()}")
# RESTORE is itself a new version: history is never rewritten, only added to.
display(spark.sql("DESCRIBE HISTORY bikes").select("version", "operation").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Schema enforcement, then schema evolution
# MAGIC
# MAGIC Someone tries to append bikes with an extra column, `color`. Delta **rejects** the
# MAGIC write instead of silently corrupting the table.

# COMMAND ----------

new_bikes = spark.createDataFrame(
    [("LIS-B105", "lisbon", "electric", 77, "available", None, "green")],
    "bike_id STRING, city STRING, bike_type STRING, battery_pct INT, "
    "status STRING, last_service DATE, color STRING",
)

try:
    new_bikes.write.mode("append").saveAsTable("bikes")
except Exception as error:  # AnalysisException: schema mismatch
    print("Write rejected by schema enforcement:")
    print("   ", str(error).splitlines()[0][:250])

# COMMAND ----------

# The change is intentional? Say so explicitly with mergeSchema: the schema evolves.
new_bikes.write.mode("append").option("mergeSchema", "true").saveAsTable("bikes")
display(spark.table("bikes").select("bike_id", "city", "color").orderBy("bike_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. CHECK constraints
# MAGIC
# MAGIC A battery level of 150% is impossible. A constraint makes every future write prove it.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bikes ADD CONSTRAINT battery_range
# MAGIC   CHECK (battery_pct IS NULL OR battery_pct BETWEEN 0 AND 100)

# COMMAND ----------

try:
    spark.sql("INSERT INTO bikes (bike_id, city, bike_type, battery_pct, status) "
              "VALUES ('MTL-B299', 'montreal', 'electric', 150, 'available')")
except Exception as error:
    print("Write rejected by the CHECK constraint:")
    print("   ", str(error).splitlines()[0][:250])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Liquid clustering, OPTIMIZE and the file layout
# MAGIC
# MAGIC Many small writes produce many small files. `OPTIMIZE` compacts them and clusters
# MAGIC the rows by `city`. On Unity Catalog managed tables, **Predictive Optimization** can
# MAGIC run this for you automatically. Compare `numFiles` before and after.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL bikes

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE bikes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Look at numFiles and clusteringColumns
# MAGIC DESCRIBE DETAIL bikes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Change Data Feed: which rows changed?
# MAGIC
# MAGIC Time travel shows the whole table at a version. **Change Data Feed** shows only the
# MAGIC rows that changed, with the type of change. Downstream systems use it to sync
# MAGIC incrementally.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bikes SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

cdf_start = spark.sql("DESCRIBE HISTORY bikes").agg({"version": "max"}).first()[0] + 1
spark.sql("UPDATE bikes SET battery_pct = battery_pct - 5 WHERE city = 'lisbon' AND bike_type = 'electric'")

# Each update appears twice: update_preimage (before) and update_postimage (after).
display(spark.sql(f"""
    SELECT bike_id, battery_pct, _change_type, _commit_version
    FROM table_changes('bikes', {cdf_start})
    ORDER BY bike_id, _change_type
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Governance with Unity Catalog `[databricks-only]`
# MAGIC
# MAGIC Documentation and classification: comments and tags make data discoverable.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- [databricks-only]
# MAGIC COMMENT ON TABLE bikes IS 'Pedalhouse fleet: one row per bike, maintained by the workshop.';
# MAGIC ALTER TABLE bikes SET TAGS ('domain' = 'operations', 'layer' = 'lab');
# MAGIC ALTER TABLE bikes ALTER COLUMN battery_pct SET TAGS ('unit' = 'percent')

# COMMAND ----------

# MAGIC %md
# MAGIC Now some personal data. Riders' emails are PII. A **column mask** hides them from
# MAGIC anyone outside a privileged group, and a **row filter** limits which cities a user
# MAGIC sees. The group `pedalhouse_privacy_team` does not exist, so **you** are treated like
# MAGIC a regular analyst and see the masked, filtered result.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- [databricks-only]
# MAGIC CREATE OR REPLACE TABLE riders (rider_id STRING, full_name STRING, email STRING, city STRING);
# MAGIC INSERT INTO riders VALUES
# MAGIC   ('M00001', 'Ana Silva',    'ana.silva@example.com',    'lisbon'),
# MAGIC   ('M00002', 'Liam Roy',     'liam.roy@example.com',     'montreal'),
# MAGIC   ('M00003', 'Seo-yeon Kim', 'seoyeon.kim@example.com',  'seoul');
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION mask_email(email STRING)
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('pedalhouse_privacy_team') THEN email
# MAGIC   ELSE regexp_replace(email, '^[^@]+', '*****')
# MAGIC END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION only_lisbon_unless_privileged(city STRING)
# MAGIC RETURN is_account_group_member('pedalhouse_privacy_team') OR city = 'lisbon';
# MAGIC
# MAGIC ALTER TABLE riders ALTER COLUMN email SET MASK mask_email;
# MAGIC ALTER TABLE riders SET ROW FILTER only_lisbon_unless_privileged ON (city)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- [databricks-only]
# MAGIC -- You should see ONE row (Lisbon) with a masked email.
# MAGIC SELECT * FROM riders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- [databricks-only]
# MAGIC -- Permissions are SQL too. 'account users' = everyone in your account.
# MAGIC GRANT SELECT ON TABLE bikes TO `account users`;
# MAGIC SHOW GRANTS ON TABLE bikes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- [databricks-only]
# MAGIC -- information_schema: the catalog describes itself, queryable with SQL.
# MAGIC SELECT table_name, table_type, comment
# MAGIC FROM information_schema.tables
# MAGIC WHERE table_schema = current_schema()
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC | You did | Delta / UC feature |
# MAGIC |---|---|
# MAGIC | INSERT, UPDATE, DELETE, MERGE | ACID transactions, one version per commit |
# MAGIC | `DESCRIBE HISTORY`, `VERSION AS OF` | transaction log, time travel |
# MAGIC | `RESTORE` after a bad delete | recovery without backups |
# MAGIC | rejected append, then `mergeSchema` | schema enforcement and evolution |
# MAGIC | `ADD CONSTRAINT ... CHECK` | data quality at write time |
# MAGIC | `CLUSTER BY`, `OPTIMIZE`, `DESCRIBE DETAIL` | file layout, liquid clustering |
# MAGIC | `table_changes()` | Change Data Feed |
# MAGIC | tags, masks, row filters, grants | Unity Catalog governance |
# MAGIC
# MAGIC ### Challenge
# MAGIC 1. Which version first contained the `color` column? Find it with `DESCRIBE HISTORY`.
# MAGIC 2. Show the bikes that were in `maintenance` at version 3 but are `available` now.
# MAGIC
# MAGIC Solutions are in the next cell. Try first.

# COMMAND ----------

# Solution 1: the write whose operationParameters mention mergeSchema / the WRITE after RESTORE.
display(spark.sql("DESCRIBE HISTORY bikes")
        .where("operation IN ('WRITE', 'CREATE OR REPLACE TABLE AS SELECT')")
        .select("version", "operation", "operationParameters"))

# Solution 2: time travel joined with the current table.
display(spark.sql("""
    SELECT old.bike_id, old.status AS status_v3, now.status AS status_now
    FROM bikes VERSION AS OF 3 AS old
    JOIN bikes AS now USING (bike_id)
    WHERE old.status = 'maintenance' AND now.status = 'available'
"""))
