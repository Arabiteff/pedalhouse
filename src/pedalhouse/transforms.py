"""Business logic for Pedalhouse, written as small, testable PySpark functions.

The Declarative Pipeline (src/pipeline/silver/trips_silver.py) imports these
functions, and tests/test_transforms.py unit tests them with tiny in-memory
DataFrames. Same code in production and in tests: that is the DevOps lesson.

Functions that return a ``Column`` can be reused inside any ``select`` or
``withColumn``. Functions that take and return a ``DataFrame`` describe a
whole step.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

VALID_BIKE_TYPES = ("classic", "electric")

# A trip that breaks any of these rules goes to quarantine instead of silver.
# Each rule is a SQL expression, so the exact same text can be reused in
# pipeline expectations, notebooks and tests.
QUALITY_RULES: dict[str, str] = {
    "has_start_time": "started_at IS NOT NULL",
    "has_end_time": "ended_at IS NOT NULL",
    "ends_after_start": "ended_at > started_at",
    "has_stations": "start_station_id IS NOT NULL AND end_station_id IS NOT NULL",
    "known_bike_type": "bike_type IN ('classic', 'electric')",
}

UNLOCK_FEE = 1.00
PRICE_PER_MINUTE = {"classic": 0.15, "electric": 0.30}


# --------------------------------------------------------------------------- columns

def duration_minutes(start: Column, end: Column) -> Column:
    """Trip length in minutes, one decimal. NULL if either side is NULL."""
    return F.round((F.unix_timestamp(end) - F.unix_timestamp(start)) / 60.0, 1)


def duration_bucket(minutes: Column) -> Column:
    """Human-friendly length category used in the gold tables."""
    return (F.when(minutes.isNull(), F.lit(None))
            .when(minutes < 10, "short")
            .when(minutes < 30, "medium")
            .when(minutes < 90, "long")
            .otherwise("marathon"))


def base_fare(bike_type: Column, minutes: Column) -> Column:
    """Unlock fee plus a per-minute price that depends on the bike type."""
    per_minute = (F.when(bike_type == "electric", PRICE_PER_MINUTE["electric"])
                  .otherwise(PRICE_PER_MINUTE["classic"]))
    return F.round(F.lit(UNLOCK_FEE) + per_minute * F.greatest(minutes, F.lit(0.0)), 2)


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Straight-line distance between two GPS points, in kilometres."""
    r = 6371.0
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    return F.round(F.lit(2 * r) * F.asin(F.sqrt(a)), 3)


def all_rules_pass(rules: dict[str, str] = QUALITY_RULES) -> Column:
    """TRUE when every rule passes. NULL results (missing data) count as a failure."""
    checks = [F.coalesce(F.expr(rule), F.lit(False)) for rule in rules.values()]
    result = checks[0]
    for check in checks[1:]:
        result = result & check
    return result


def failed_rules(rules: dict[str, str] = QUALITY_RULES) -> Column:
    """Array with the name of every rule the row breaks (empty when the row is clean)."""
    flags = [F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name))
             for name, rule in rules.items()]
    return F.filter(F.array(*flags), lambda x: x.isNotNull())


# --------------------------------------------------------------------------- steps

def flag_trips(trips: DataFrame, rules: dict[str, str] = QUALITY_RULES) -> DataFrame:
    """Quarantine pattern, step 1: keep EVERY row and flag the bad ones.

    Inverse logic: ``is_quarantined = NOT (all rules pass)``. Nothing is
    dropped here, so downstream tables can route valid and invalid rows.
    """
    return (trips
            .withColumn("duration_min", duration_minutes(F.col("started_at"), F.col("ended_at")))
            .withColumn("failed_rules", failed_rules(rules))
            .withColumn("is_quarantined", ~all_rules_pass(rules)))


def enrich_trips(trips: DataFrame, stations: DataFrame) -> DataFrame:
    """Add station names, coordinates, distance, fare and a length bucket.

    In the pipeline, ``trips`` is a stream and ``stations`` is a regular
    table: a stream-static join. Each new trip is joined with the latest
    station data at the moment it is processed.
    """
    start = stations.select(
        F.col("station_id").alias("start_station_id"),
        F.col("station_name").alias("start_station_name"),
        F.col("lat").alias("start_lat"),
        F.col("lon").alias("start_lon"),
    )
    end = stations.select(
        F.col("station_id").alias("end_station_id"),
        F.col("station_name").alias("end_station_name"),
        F.col("lat").alias("end_lat"),
        F.col("lon").alias("end_lon"),
    )
    minutes = F.col("duration_min")
    return (trips
            .join(start, on="start_station_id", how="left")
            .join(end, on="end_station_id", how="left")
            .withColumn("distance_km",
                        haversine_km(F.col("start_lat"), F.col("start_lon"),
                                     F.col("end_lat"), F.col("end_lon")))
            .withColumn("base_fare", base_fare(F.col("bike_type"), minutes))
            .withColumn("duration_bucket", duration_bucket(minutes))
            .withColumn("is_round_trip", F.col("start_station_id") == F.col("end_station_id"))
            .drop("start_lat", "start_lon", "end_lat", "end_lon"))
