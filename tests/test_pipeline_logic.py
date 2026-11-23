"""Integration test: generated files -> bronze -> silver -> gold, on local Spark.

The real pipeline uses Databricks-only features (read_files, streaming
tables, AUTO CDC). Here we reproduce the bronze layer with plain Spark
readers, then run the SAME silver Python functions and the SAME gold SQL
files as the pipeline. If the numbers line up with what the generator
broke on purpose, the logic is right.
"""

import glob
import os
from datetime import datetime

import pytest
from pyspark.sql import functions as F

import local_lakehouse
from notebook_utils import materialized_view_queries
from pedalhouse import datagen

GOLD_DIR = local_lakehouse.GOLD_DIR


@pytest.fixture(scope="module")
def lakehouse(spark, tmp_path_factory):
    landing = str(tmp_path_factory.mktemp("landing"))
    for _ in range(3):
        datagen.generate_batch(landing, trips_per_city=200, bad_rate=0.05)
    t = local_lakehouse.build(spark, landing)
    for name in ("trips_flagged", "trips_enriched"):
        t[name] = t[name].cache()
    for name in ("trips_flagged", "trips_quarantine", "trips_enriched"):
        t[name].createOrReplaceTempView(name)
    return {"landing": landing, "bronze": t["trips_bronze"], "flagged": t["trips_flagged"],
            "silver": t["trips_silver"], "quarantine": t["trips_quarantine"],
            "enriched": t["trips_enriched"], "stations_bronze": t["stations_bronze"],
            "stations_silver": t["stations_silver"]}


def test_quarantine_catches_exactly_the_broken_trips(lakehouse):
    expected = datagen.expected_totals(lakehouse["landing"])
    assert lakehouse["bronze"].count() == expected["rows"] == 3 * 3 * 200
    assert lakehouse["quarantine"].count() == expected["bad"]
    assert lakehouse["silver"].count() + lakehouse["quarantine"].count() == expected["rows"]


def test_outliers_are_kept_in_silver(lakehouse):
    expected = datagen.expected_totals(lakehouse["landing"])
    long_trips = lakehouse["silver"].where("duration_min > 360").count()
    assert long_trips == expected["outliers"] > 0


def test_stations_drop_rule(lakehouse):
    assert lakehouse["stations_bronze"].count() == 25
    assert lakehouse["stations_silver"].count() == 24


def test_enrichment_finds_every_station(lakehouse):
    enriched = lakehouse["enriched"]
    assert enriched.count() == lakehouse["silver"].count()
    assert enriched.where("start_station_name IS NULL OR end_station_name IS NULL").count() == 0


def test_gold_sql_files_run_on_spark(spark, lakehouse):
    """Execute every gold materialized view query from the real pipeline files."""
    # A tiny SCD2 history and SCD1 table so the member joins have something to match.
    members_history = spark.createDataFrame([
        ("M00001", "basic", "lisbon", datetime(2026, 11, 2, 0, 0), datetime(2026, 11, 3, 12, 0)),
        ("M00001", "pro",   "lisbon", datetime(2026, 11, 3, 12, 0), None),
    ], "member_id STRING, plan STRING, home_city STRING, __START_AT TIMESTAMP, __END_AT TIMESTAMP")
    members_history.createOrReplaceTempView("members_history")
    members_history.where("__END_AT IS NULL").drop("__START_AT", "__END_AT") \
        .createOrReplaceTempView("members_current")

    files = sorted(glob.glob(os.path.join(GOLD_DIR, "*.sql")))
    views = [mv for f in files for mv in materialized_view_queries(f)]
    assert {name for name, _ in views} >= {
        "gold_station_daily", "gold_city_quality", "gold_quarantine_reasons",
        "gold_revenue_by_plan", "gold_members_by_plan"}

    results = {}
    for name, query in views:
        df = spark.sql(query)
        df.createOrReplaceTempView(name)
        results[name] = df

    total_clean = lakehouse["enriched"].count()
    assert results["gold_station_daily"].agg(F.sum("trips")).first()[0] == total_clean
    assert results["gold_revenue_by_plan"].agg(F.sum("trips")).first()[0] == total_clean

    quality = results["gold_city_quality"].agg(F.sum("total_trips"), F.sum("quarantined_trips")).first()
    assert quality[0] == lakehouse["bronze"].count()
    assert quality[1] == lakehouse["quarantine"].count()

    reasons = results["gold_quarantine_reasons"].agg(F.sum("trips")).first()[0]
    assert reasons >= lakehouse["quarantine"].count()  # a trip can break several rules

    plans = {r.plan for r in results["gold_revenue_by_plan"].select("plan").distinct().collect()}
    assert "casual" in plans


def test_point_in_time_join_uses_the_plan_valid_at_trip_time(spark):
    trips = spark.createDataFrame([
        ("t1", "M1", datetime(2026, 11, 3, 9, 0), 10.0, "lisbon"),   # before the change: basic
        ("t2", "M1", datetime(2026, 11, 3, 15, 0), 10.0, "lisbon"),  # after the change: pro
        ("t3", None, datetime(2026, 11, 3, 15, 0), 10.0, "lisbon"),  # casual rider
    ], "trip_id STRING, member_id STRING, started_at TIMESTAMP, base_fare DOUBLE, city STRING")
    trips.createOrReplaceTempView("trips_enriched")
    spark.createDataFrame([
        ("M1", "basic", datetime(2026, 11, 2), datetime(2026, 11, 3, 12, 0)),
        ("M1", "pro", datetime(2026, 11, 3, 12, 0), None),
    ], "member_id STRING, plan STRING, __START_AT TIMESTAMP, __END_AT TIMESTAMP") \
        .createOrReplaceTempView("members_history")

    [(_, query)] = materialized_view_queries(os.path.join(GOLD_DIR, "03_gold_revenue_by_plan.sql"))
    result = {r.plan: (r.trips, r.net_revenue) for r in spark.sql(query).collect()}
    assert result == {"basic": (1, 9.0), "pro": (1, 5.0), "casual": (1, 10.0)}
