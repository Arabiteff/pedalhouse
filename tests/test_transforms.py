"""Unit tests: each function on a tiny, hand-written DataFrame.

Fast (seconds), deterministic, no tables, no files. Runs locally, in CI and
inside a Databricks notebook (Lab 06).
"""

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from pedalhouse.transforms import (
    QUALITY_RULES,
    base_fare,
    duration_bucket,
    duration_minutes,
    enrich_trips,
    failed_rules,
    flag_trips,
    haversine_km,
)

TRIP_SCHEMA = ("trip_id STRING, member_id STRING, bike_id STRING, bike_type STRING, "
               "start_station_id STRING, end_station_id STRING, "
               "started_at TIMESTAMP, ended_at TIMESTAMP")
T0 = datetime(2026, 11, 2, 8, 0, 0)


def _trip(trip_id, bike_type="classic", start="LIS-01", end="LIS-02",
          started=T0, ended=datetime(2026, 11, 2, 8, 12, 30)):
    return (trip_id, None, "LIS-B100", bike_type, start, end, started, ended)


def test_duration_minutes(spark):
    df = spark.createDataFrame(
        [(T0, datetime(2026, 11, 2, 8, 12, 30)), (T0, None)],
        "started_at TIMESTAMP, ended_at TIMESTAMP",
    )
    result = df.select(duration_minutes(F.col("started_at"), F.col("ended_at")).alias("m"))
    assertDataFrameEqual(result, spark.createDataFrame([(12.5,), (None,)], "m DOUBLE"))


def test_duration_bucket_boundaries(spark):
    df = spark.createDataFrame([(9.9,), (10.0,), (29.9,), (30.0,), (90.0,), (None,)], "m DOUBLE")
    result = df.select("m", duration_bucket(F.col("m")).alias("bucket"))
    expected = spark.createDataFrame(
        [(9.9, "short"), (10.0, "medium"), (29.9, "medium"), (30.0, "long"),
         (90.0, "marathon"), (None, None)],
        "m DOUBLE, bucket STRING",
    )
    assertDataFrameEqual(result, expected)


def test_base_fare(spark):
    df = spark.createDataFrame(
        [("classic", 10.0), ("electric", 10.0), ("classic", -5.0)],
        "bike_type STRING, m DOUBLE",
    )
    result = df.select("bike_type", "m", base_fare(F.col("bike_type"), F.col("m")).alias("fare"))
    expected = spark.createDataFrame(
        # unlock 1.00 + 0.15/min classic, 0.30/min electric, negative minutes count as 0
        [("classic", 10.0, 2.5), ("electric", 10.0, 4.0), ("classic", -5.0, 1.0)],
        "bike_type STRING, m DOUBLE, fare DOUBLE",
    )
    assertDataFrameEqual(result, expected)


def test_haversine_km(spark):
    df = spark.createDataFrame(
        [(0.0, 0.0, 0.0, 1.0), (38.7, -9.1, 38.7, -9.1)],
        "lat1 DOUBLE, lon1 DOUBLE, lat2 DOUBLE, lon2 DOUBLE",
    )
    result = df.select(haversine_km(F.col("lat1"), F.col("lon1"),
                                    F.col("lat2"), F.col("lon2")).alias("km"))
    # One degree of longitude at the equator is about 111.195 km.
    assertDataFrameEqual(result, spark.createDataFrame([(111.195,), (0.0,)], "km DOUBLE"))


def test_failed_rules_names_every_broken_rule(spark):
    trips = spark.createDataFrame([
        _trip("ok"),
        _trip("backwards", ended=datetime(2026, 11, 2, 7, 50, 0)),
        _trip("no_end_unknown_bike", bike_type="unicycle", ended=None),
        _trip("no_station", start=None),
    ], TRIP_SCHEMA)
    result = trips.select("trip_id", failed_rules().alias("rules"))
    expected = spark.createDataFrame([
        ("ok", []),
        ("backwards", ["ends_after_start"]),
        ("no_end_unknown_bike", ["has_end_time", "ends_after_start", "known_bike_type"]),
        ("no_station", ["has_stations"]),
    ], "trip_id STRING, rules ARRAY<STRING>")
    assertDataFrameEqual(result, expected)


def test_flag_trips_keeps_every_row_and_flags_bad_ones(spark):
    trips = spark.createDataFrame([
        _trip("good-1"),
        _trip("good-2", bike_type="electric"),
        _trip("bad-1", bike_type="scooter"),
    ], TRIP_SCHEMA)
    flagged = flag_trips(trips)
    assert flagged.count() == 3, "the quarantine pattern must never drop rows"
    result = flagged.select("trip_id", "is_quarantined", "duration_min")
    expected = spark.createDataFrame(
        [("good-1", False, 12.5), ("good-2", False, 12.5), ("bad-1", True, 12.5)],
        "trip_id STRING, is_quarantined BOOLEAN, duration_min DOUBLE",
    )
    assertDataFrameEqual(result, expected)


def test_quality_rules_are_valid_sql(spark):
    trips = spark.createDataFrame([_trip("x")], TRIP_SCHEMA)
    for name, rule in QUALITY_RULES.items():
        assert trips.where(rule).count() == 1, f"rule {name} should pass on a clean trip"


def test_enrich_trips_adds_columns_not_rows(spark):
    trips = flag_trips(spark.createDataFrame([
        _trip("a", start="LIS-01", end="LIS-02"),
        _trip("b", start="LIS-01", end="LIS-01"),
        _trip("c", start="LIS-01", end="LIS-77"),   # unknown station: kept, name is NULL
    ], TRIP_SCHEMA))
    stations = spark.createDataFrame([
        ("LIS-01", "Alfama", 38.71, -9.13),
        ("LIS-02", "Baixa", 38.71, -9.14),
    ], "station_id STRING, station_name STRING, lat DOUBLE, lon DOUBLE")

    enriched = enrich_trips(trips, stations)
    assert enriched.count() == 3
    result = enriched.select("trip_id", "start_station_name", "end_station_name",
                             "is_round_trip", "duration_bucket", "base_fare")
    expected = spark.createDataFrame([
        ("a", "Alfama", "Baixa", False, "medium", 2.88),
        ("b", "Alfama", "Alfama", True, "medium", 2.88),
        ("c", "Alfama", None, False, "medium", 2.88),
    ], "trip_id STRING, start_station_name STRING, end_station_name STRING, "
       "is_round_trip BOOLEAN, duration_bucket STRING, base_fare DOUBLE")
    assertDataFrameEqual(result, expected)
    distances = {r.trip_id: r.distance_km for r in enriched.select("trip_id", "distance_km").collect()}
    assert distances["b"] == 0.0 and distances["a"] > 0 and distances["c"] is None
