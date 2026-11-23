"""Shared pytest fixtures.

On a laptop or in CI: a local SparkSession with Delta Lake.
Inside a Databricks notebook (Lab 06): the notebook's own Spark session.
"""

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))


def delta_enabled() -> bool:
    """Delta needs its jars from Maven. Set PEDALHOUSE_NO_DELTA=1 where Maven is unreachable."""
    return os.environ.get("PEDALHOUSE_NO_DELTA") != "1"


def _local_spark(warehouse_dir: str):
    from pyspark.sql import SparkSession

    if not delta_enabled():
        return (SparkSession.builder.master("local[2]").appName("pedalhouse-tests")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.warehouse.dir", warehouse_dir)
                .config("spark.ui.enabled", "false")
                .getOrCreate())

    from delta import configure_spark_with_delta_pip

    builder = (SparkSession.builder
               .master("local[2]")
               .appName("pedalhouse-tests")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.sources.default", "delta")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.warehouse.dir", warehouse_dir)
               .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")
               .config("spark.ui.enabled", "false"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        from databricks.sdk.runtime import spark as notebook_spark
        yield notebook_spark
        return
    session = _local_spark(str(tmp_path_factory.mktemp("warehouse")))
    yield session
    session.stop()
