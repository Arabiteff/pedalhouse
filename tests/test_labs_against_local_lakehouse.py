"""Run Lab 04 and the grader notebook against a local copy of every pipeline table.

The tables are saved in a local database, then the notebooks' own cells are
executed. This proves the lab queries run, and that the grader marks a
correct lakehouse as PASS with exact counts.
"""

import os
from dataclasses import dataclass

import pytest

import local_lakehouse
from notebook_utils import notebook_cells
from pedalhouse import datagen
from pedalhouse.config import CITIES, LakehousePaths

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = "pedalhouse_local"


@dataclass(frozen=True)
class LocalPaths(LakehousePaths):
    landing_dir: str = ""

    @property
    def landing(self) -> str:
        return self.landing_dir


@pytest.fixture(scope="module")
def saved_lakehouse(spark, tmp_path_factory):
    landing = str(tmp_path_factory.mktemp("landing"))
    for _ in range(4):
        datagen.generate_batch(landing, trips_per_city=150, bad_rate=0.05)
    datagen.write_poison_file(landing)
    datagen.fix_poison_files(landing)

    tables = local_lakehouse.register_gold(spark, local_lakehouse.build(spark, landing))
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"USE {DB}")
    for name, df in tables.items():
        df.write.mode("overwrite").format("parquet").saveAsTable(f"{DB}.{name}")
    for name in tables:
        spark.catalog.dropTempView(name)
    return LocalPaths(catalog="spark_catalog", schema=DB, landing_dir=landing)


def _run_notebook(path, namespace, spark):
    for kind, source in notebook_cells(path):
        if kind == "sql":
            spark.sql(source).collect()
        elif kind == "python":
            exec(compile(source, os.path.basename(path), "exec"), namespace)
    return namespace


def _namespace(spark, paths):
    shown = []
    return {"spark": spark, "paths": paths, "datagen": datagen, "CITIES": CITIES, "os": os,
            "display": lambda df: shown.append(df.collect()), "_shown": shown}


def test_lab04_queries_run_and_resolve_out_of_order_events(spark, saved_lakehouse):
    ns = _run_notebook(os.path.join(ROOT, "labs", "04_cdc_and_scd.py"),
                       _namespace(spark, saved_lakehouse), spark)
    assert ns["actual_plan"] == ns["expected_plan"]
    assert len(ns["_shown"]) >= 9  # every display() produced a result
    history = spark.table(f"{DB}.members_history")
    assert history.where("__END_AT IS NOT NULL").count() > 0


def test_grader_passes_on_a_correct_lakehouse(spark, saved_lakehouse):
    ns = _run_notebook(os.path.join(ROOT, "labs", "99_check_my_work.py"),
                       _namespace(spark, saved_lakehouse), spark)
    status = {(lab, name): (st, detail) for lab, name, st, detail in ns["results"]}

    must_pass = [k for k in status if k[0] in ("03", "04") and "Challenge" not in k[1]]
    assert must_pass, "no lab 03/04 checks found"
    failures = {k: v for k, v in status.items() if k in must_pass and v[0] != "PASS"}
    assert not failures, failures
    # Labs not built in this test are reported as TODO, not as crashes.
    assert status[("05", "pedalhouse-daily job deployed")][0] == "TODO"


def test_grader_catches_a_wrong_quarantine(spark, saved_lakehouse):
    quarantine = spark.table(f"{DB}.trips_quarantine")
    spark.createDataFrame(quarantine.limit(quarantine.count() - 1).collect(), quarantine.schema) \
        .write.mode("overwrite").format("parquet").saveAsTable(f"{DB}.trips_quarantine_bad")
    spark.sql(f"ALTER TABLE {DB}.trips_quarantine RENAME TO {DB}.trips_quarantine_ok")
    spark.sql(f"ALTER TABLE {DB}.trips_quarantine_bad RENAME TO {DB}.trips_quarantine")
    try:
        ns = _run_notebook(os.path.join(ROOT, "labs", "99_check_my_work.py"),
                           _namespace(spark, saved_lakehouse), spark)
        status = {name: (st, d) for lab, name, st, d in ns["results"]}
        st, detail = status["Trips: bronze = silver + quarantine, exact"]
        assert st == "FAIL" and "quarantine" in detail
    finally:
        spark.sql(f"DROP TABLE {DB}.trips_quarantine")
        spark.sql(f"ALTER TABLE {DB}.trips_quarantine_ok RENAME TO {DB}.trips_quarantine")
