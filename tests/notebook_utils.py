"""Helpers to read Databricks source-format notebooks and SQL pipeline files in tests."""

import re

CELL_SEPARATOR = "# COMMAND ----------"
DATABRICKS_ONLY = "[databricks-only]"


def notebook_cells(path: str) -> list[tuple[str, str]]:
    """Return (kind, source) for each cell. kind: python, sql, md, magic."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    text = text.replace("# Databricks notebook source\n", "", 1)
    cells = []
    for raw in text.split(CELL_SEPARATOR):
        lines = raw.strip("\n").splitlines()
        if not lines:
            continue
        if all(line.startswith("# MAGIC") for line in lines if line.strip()):
            body = "\n".join(re.sub(r"^# MAGIC ?", "", line) for line in lines)
            first = body.lstrip().split("\n", 1)[0].strip()
            if first.startswith("%sql"):
                cells.append(("sql", body.lstrip()[len("%sql"):].strip()))
            elif first.startswith("%md"):
                cells.append(("md", body))
            else:
                cells.append(("magic", body))
        else:
            cells.append(("python", "\n".join(lines)))
    return cells


def sql_statements(sql: str) -> list[str]:
    """Split a SQL cell into statements, dropping comment-only lines."""
    kept = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(kept).split(";") if s.strip()]


def materialized_view_queries(path: str) -> list[tuple[str, str]]:
    """(name, SELECT ...) for every CREATE OR REFRESH MATERIALIZED VIEW in a pipeline SQL file."""
    with open(path, encoding="utf-8") as f:
        text = "\n".join(line for line in f.read().splitlines()
                         if not line.strip().startswith("--"))
    pattern = re.compile(
        r"CREATE OR REFRESH MATERIALIZED VIEW\s+(\w+).*?\bAS\s+(SELECT\b.*?);",
        re.DOTALL | re.IGNORECASE,
    )
    return [(m.group(1), m.group(2)) for m in pattern.finditer(text)]
