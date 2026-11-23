"""
SILVER | trips: the quarantine pattern, expectations and a stream-static join.

Written in Python on purpose: the same pipeline mixes SQL and Python files,
and Python lets us import the tested business logic from src/pedalhouse.

    trips_bronze ──► trips_flagged ──► trips_silver ──► trips_enriched
                          │                                   ▲
                          └──────► trips_quarantine           │
                                                     stations_silver (static)

Every table here is a STREAMING TABLE: each trip is processed exactly once.
"""

import sys

from pyspark import pipelines as dp

# The bundle passes the deployed path of src/ as a pipeline configuration
# value, so this file can import our package like any Python module.
sys.path.append(spark.conf.get("src_path"))  # noqa: F821  (spark is provided by the pipeline)

from pedalhouse.transforms import enrich_trips, flag_trips  # noqa: E402


@dp.table(
    name="trips_flagged",
    comment="Every bronze trip with quality flags. Nothing is dropped here (inverse logic).",
)
# WARN expectations: the row is kept, the violation is counted in the event log.
@dp.expect("reasonable_duration", "duration_min IS NULL OR duration_min <= 360")
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def trips_flagged():
    return flag_trips(spark.readStream.table("trips_bronze"))  # noqa: F821


@dp.table(
    name="trips_quarantine",
    comment="Trips that broke at least one quality rule, with the list of rules they broke.",
)
def trips_quarantine():
    return spark.readStream.table("trips_flagged").where("is_quarantined")  # noqa: F821


@dp.table(
    name="trips_silver",
    comment="Clean trips only. Safe to build gold tables on.",
)
def trips_silver():
    return (spark.readStream.table("trips_flagged")  # noqa: F821
            .where("NOT is_quarantined")
            .drop("is_quarantined", "failed_rules"))


@dp.table(
    name="trips_enriched",
    comment="Clean trips joined with stations (stream-static join): names, distance, fare, bucket.",
)
def trips_enriched():
    # spark.readStream -> the streaming side: only new trips are processed.
    # spark.read       -> the static side: the latest stations on each micro-batch.
    trips = spark.readStream.table("trips_silver")  # noqa: F821
    stations = spark.read.table("stations_silver")  # noqa: F821
    return enrich_trips(trips, stations)
