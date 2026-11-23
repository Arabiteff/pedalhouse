"""Checks on the lab and job notebooks themselves.

* every Python cell in every notebook is valid Python
* Lab 01 actually RUNS end to end on local Spark + Delta (cells marked
  [databricks-only] are skipped, because they use Unity Catalog features)
"""

import ast
import glob
import os

import pytest

from conftest import delta_enabled
from notebook_utils import DATABRICKS_ONLY, notebook_cells, sql_statements

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS = sorted(glob.glob(os.path.join(ROOT, "labs", "*.py"))
                   + glob.glob(os.path.join(ROOT, "src", "jobs", "*.py")))


@pytest.mark.parametrize("path", NOTEBOOKS, ids=[os.path.relpath(p, ROOT) for p in NOTEBOOKS])
def test_python_cells_parse(path):
    cells = notebook_cells(path)
    assert cells, "notebook has no cells"
    for kind, source in cells:
        if kind == "python":
            ast.parse(source, filename=path)


@pytest.mark.parametrize("path", NOTEBOOKS, ids=[os.path.relpath(p, ROOT) for p in NOTEBOOKS])
def test_notebook_header(path):
    with open(path, encoding="utf-8") as f:
        assert f.readline().strip() == "# Databricks notebook source"


@pytest.mark.skipif(not delta_enabled(), reason="Delta Lake jars not available (PEDALHOUSE_NO_DELTA=1)")
def test_lab01_runs_end_to_end_on_local_delta(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS lab01_test")
    spark.sql("USE lab01_test")
    namespace = {"spark": spark, "display": lambda df: df.show(truncate=False)}
    executed = 0

    for kind, source in notebook_cells(os.path.join(ROOT, "labs", "01_delta_lake_deep_dive.py")):
        if DATABRICKS_ONLY in source or kind in ("md", "magic"):
            continue
        if kind == "sql":
            for statement in sql_statements(source):
                spark.sql(statement).collect()
                executed += 1
        else:
            exec(compile(source, "lab01", "exec"), namespace)
            executed += 1

    assert executed >= 20
    operations = [r.operation for r in spark.sql("DESCRIBE HISTORY bikes").collect()]
    for op in ("MERGE", "DELETE", "UPDATE", "RESTORE", "OPTIMIZE", "ADD CONSTRAINT"):
        assert op in operations, f"{op} missing from history: {operations}"

    bikes = spark.table("bikes")
    assert "color" in bikes.columns, "schema evolution step did not run"
    assert bikes.where("bike_type = 'electric'").count() > 0, "RESTORE did not bring the bikes back"
    assert bikes.where("battery_pct > 100").count() == 0, "CHECK constraint let a bad row in"

    detail = spark.sql("DESCRIBE DETAIL bikes").first()
    assert detail["clusteringColumns"] == ["city"]
    props = spark.sql("SHOW TBLPROPERTIES bikes").collect()
    assert any(r.key == "delta.enableChangeDataFeed" and r.value == "true" for r in props)
