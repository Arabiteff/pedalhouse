# Databricks notebook source
# MAGIC %md
# MAGIC # Job task · City report (runs once per city, inside a **For each** loop)
# MAGIC
# MAGIC The For each task passes one city per iteration through the `city` parameter
# MAGIC (`{{input}}` in the job definition). This notebook writes one row per city and
# MAGIC day into `city_daily_report` with a **MERGE**, so re-running a day updates the
# MAGIC row instead of duplicating it (idempotent).
# MAGIC
# MAGIC **Repair-run drill:** set the job parameter `fail_city` to `seoul`. Only the
# MAGIC Seoul iteration fails. Then set it back to `none` and use **Repair run**: only
# MAGIC the failed iteration and the tasks after it run again.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "pedalhouse_dev")
dbutils.widgets.text("city", "lisbon")
dbutils.widgets.text("fail_city", "none")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
city = dbutils.widgets.get("city")
fail_city = dbutils.widgets.get("fail_city")

if city == fail_city:
    raise RuntimeError(f"Simulated failure for {city}: the reporting API timed out. "
                       "Set fail_city back to 'none' and use Repair run.")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.city_daily_report (
      city            STRING,
      ride_date       DATE,
      trips           BIGINT,
      gross_revenue   DOUBLE,
      busiest_station STRING,
      reported_at     TIMESTAMP
    )
    COMMENT 'One row per city and day, written by the For each task of pedalhouse-daily'
""")

spark.sql(f"""
    MERGE INTO {catalog}.{schema}.city_daily_report AS target
    USING (
      WITH latest AS (
        SELECT MAX(ride_date) AS ride_date
        FROM {catalog}.{schema}.gold_station_daily
        WHERE city = '{city}'
      ),
      day_stats AS (
        SELECT g.city, g.ride_date, g.station_name, g.trips, g.gross_revenue
        FROM {catalog}.{schema}.gold_station_daily AS g
        JOIN latest USING (ride_date)
        WHERE g.city = '{city}'
      )
      SELECT city,
             ride_date,
             SUM(trips)                     AS trips,
             ROUND(SUM(gross_revenue), 2)   AS gross_revenue,
             MAX_BY(station_name, trips)    AS busiest_station,
             current_timestamp()            AS reported_at
      FROM day_stats
      GROUP BY city, ride_date
    ) AS source
    ON target.city = source.city AND target.ride_date = source.ride_date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

display(spark.sql(f"""
    SELECT * FROM {catalog}.{schema}.city_daily_report
    WHERE city = '{city}' ORDER BY ride_date DESC LIMIT 5
"""))
