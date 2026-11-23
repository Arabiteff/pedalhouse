"""Build every pipeline table on local Spark, from files made by the generator.

This mirrors the Declarative Pipeline closely enough to run the labs' SQL and
the grader against it:

* bronze readers reproduce the three flows of src/pipeline/bronze/02_trips_bronze.sql
* silver uses the SAME Python functions as src/pipeline/silver/02_trips_silver.py
* AUTO CDC (Databricks-only) is simulated in plain Python, with the same rules:
  SEQUENCE BY event_ts, deletes, TRACK HISTORY ON * EXCEPT (email, full_name)
* gold runs the SAME SQL files as the pipeline
"""

import glob
import json
import os

from pyspark.sql import functions as F

from notebook_utils import materialized_view_queries
from pedalhouse.transforms import enrich_trips, flag_trips

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(ROOT, "src", "pipeline", "gold")
TRIP_COLUMNS = ["trip_id", "member_id", "bike_id", "bike_type", "start_station_id",
                "end_station_id", "started_at", "ended_at", "city", "source_file"]
TRACKED = ("plan", "home_city")


def read_city_csv(spark, landing, city):
    df = spark.read.option("header", "true").csv(f"{landing}/trips/{city}/*.csv")
    return (df.withColumn("started_at", F.expr("try_to_timestamp(started_at)"))
              .withColumn("ended_at", F.expr("try_to_timestamp(ended_at)"))
              .withColumn("city", F.lit(city))
              .withColumn("source_file", F.col("_metadata.file_name"))
              .select(*TRIP_COLUMNS))


def read_seoul(spark, landing):
    return spark.read.json(f"{landing}/trips/seoul/*.json").select(
        F.col("trip_id"),
        F.col("rider.member_id").alias("member_id"),
        F.col("bike.bike_id").alias("bike_id"),
        F.col("bike.type").alias("bike_type"),
        F.col("route.from_station").alias("start_station_id"),
        F.col("route.to_station").alias("end_station_id"),
        F.expr("try_to_timestamp(times.start)").alias("started_at"),
        F.expr("try_to_timestamp(times.end)").alias("ended_at"),
        F.lit("seoul").alias("city"),
        F.col("_metadata.file_name").alias("source_file"),
    )


def simulate_auto_cdc(events):
    """Return (scd1_rows, scd2_rows) the way AUTO CDC would build them."""
    by_member = {}
    for e in events:
        by_member.setdefault(e["member_id"], []).append(e)

    scd1, scd2 = [], []
    for member_id, member_events in by_member.items():
        current = None
        for e in sorted(member_events, key=lambda x: x["event_ts"]):  # SEQUENCE BY event_ts
            if e["operation"] == "DELETE":
                if current:
                    current["__END_AT"] = e["event_ts"]
                current = None
                continue
            values = {k: e[k] for k in ("member_id", "full_name", "email", "home_city", "plan")}
            if current and all(current[k] == values[k] for k in TRACKED):
                current.update(values)          # untracked change: update in place
                continue
            if current:
                current["__END_AT"] = e["event_ts"]
            current = {**values, "event_ts": e["event_ts"], "__START_AT": e["event_ts"], "__END_AT": None}
            scd2.append(current)
        if current:
            latest = max((x for x in member_events if x["operation"] != "DELETE"),
                         key=lambda x: x["event_ts"])
            scd1.append({**{k: latest[k] for k in ("member_id", "full_name", "email",
                                                    "home_city", "plan")},
                         "event_ts": latest["event_ts"]})
    return scd1, scd2


def build(spark, landing):
    """Return a dict of DataFrames named like the pipeline tables."""
    bronze = (read_city_csv(spark, landing, "lisbon")
              .unionByName(read_city_csv(spark, landing, "montreal"))
              .unionByName(read_seoul(spark, landing))
              .withColumn("_rescued_data", F.lit(None).cast("string")))

    stations_bronze = (spark.read.option("header", "true").option("inferSchema", "true")
                       .csv(f"{landing}/stations/stations.csv")
                       .withColumn("source_file", F.col("_metadata.file_name")))
    stations_silver = stations_bronze.where(
        "lat IS NOT NULL AND lon IS NOT NULL AND lat BETWEEN -90 AND 90 "
        "AND lon BETWEEN -180 AND 180").drop("source_file")

    flagged = flag_trips(bronze)
    silver = flagged.where("NOT is_quarantined").drop("is_quarantined", "failed_rules")
    tables = {
        "trips_bronze": bronze,
        "stations_bronze": stations_bronze,
        "stations_silver": stations_silver,
        "trips_flagged": flagged,
        "trips_quarantine": flagged.where("is_quarantined"),
        "trips_silver": silver,
        "trips_enriched": enrich_trips(silver, stations_silver),
    }

    events = []
    for path in sorted(glob.glob(f"{landing}/members_cdc/*.json")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                events.append({**json.loads(line), "source_file": os.path.basename(path)})
    member_schema = ("member_id STRING, full_name STRING, email STRING, home_city STRING, "
                     "plan STRING, event_ts STRING")
    scd1, scd2 = simulate_auto_cdc(events)
    tables["members_cdc_bronze"] = (spark.createDataFrame(
        events, member_schema + ", operation STRING, source_file STRING")
        .withColumn("event_ts", F.to_timestamp("event_ts")))
    tables["members_current"] = (spark.createDataFrame(scd1, member_schema)
                                 .withColumn("event_ts", F.to_timestamp("event_ts")))
    tables["members_history"] = (spark.createDataFrame(
        scd2, member_schema + ", __START_AT STRING, __END_AT STRING")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("__START_AT", F.to_timestamp("__START_AT"))
        .withColumn("__END_AT", F.to_timestamp("__END_AT")))
    return tables


def register_gold(spark, tables):
    """Register every table as a temp view, then run the gold SQL files on top."""
    for name, df in tables.items():
        df.createOrReplaceTempView(name)
    for path in sorted(glob.glob(os.path.join(GOLD_DIR, "*.sql"))):
        for name, query in materialized_view_queries(path):
            tables[name] = spark.sql(query)
            tables[name].createOrReplaceTempView(name)
    return tables
