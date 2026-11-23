# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 99 · Check my work
# MAGIC
# MAGIC An automatic grader. It inspects your tables, volumes and jobs and compares them
# MAGIC with what the data generator produced, so the checks are **exact**, not "roughly
# MAGIC right". Run it whenever you finish a lab.
# MAGIC
# MAGIC * **PASS**: done and correct
# MAGIC * **FAIL**: done but something is off (read the hint)
# MAGIC * **TODO**: not started yet

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import json

results = []

def check(lab, name, fn):
    """Run one check. fn returns (status, detail); exceptions count as TODO."""
    try:
        status, detail = fn()
    except Exception as error:  # table missing, job missing...
        status, detail = "TODO", str(error).splitlines()[0][:160]
    results.append((lab, name, status, detail))

def exists(table):
    return spark.catalog.tableExists(paths.table(table))

def count(table):
    return spark.table(paths.table(table)).count()

state = datagen.load_state(paths.landing)
expected = datagen.expected_totals(paths.landing)

def rows_for_files(files):
    """Exact number of generated rows for a set of source file names."""
    total = 0
    for batch in state["batches"].values():
        for city, c in batch["cities"].items():
            if c["file"] in files:
                total += c["rows"]
    total += 3 * sum(1 for p in state["poison_files"] if os.path.basename(p) in files)
    return total

def bad_for_files(files):
    return sum(c["bad"] for b in state["batches"].values() for c in b["cities"].values()
               if c["file"] in files)

# COMMAND ----------

# ---------------------------------------------------------------- Lab 00
check("00", "Landing volume has data", lambda: (
    ("PASS", f"{expected['batches']} batch(es) generated") if expected["batches"] >= 1
    else ("TODO", "run lab 00")))
check("00", "First Delta table lab00_stations", lambda: (
    ("PASS", "25 stations") if count("lab00_stations") == 25 else ("FAIL", "expected 25 rows")))

# ---------------------------------------------------------------- Lab 01
def lab01_history():
    ops = {r.operation for r in spark.sql(f"DESCRIBE HISTORY {paths.table('bikes')}").collect()}
    needed = {"MERGE", "DELETE", "UPDATE", "RESTORE", "OPTIMIZE", "ADD CONSTRAINT"}
    missing = needed - ops
    return ("PASS", "all operations found") if not missing else ("FAIL", f"missing: {sorted(missing)}")

def lab01_features():
    detail = spark.sql(f"DESCRIBE DETAIL {paths.table('bikes')}").first()
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {paths.table('bikes')}").collect()}
    problems = []
    if detail["clusteringColumns"] != ["city"]:
        problems.append("not clustered by city")
    if props.get("delta.enableChangeDataFeed") != "true":
        problems.append("Change Data Feed off")
    if not any(k.startswith("delta.constraints.") for k in props):
        problems.append("no CHECK constraint")
    if "color" not in spark.table(paths.table("bikes")).columns:
        problems.append("schema evolution step not done")
    return ("PASS", "clustering, CDF, constraint, evolved schema") if not problems else ("FAIL", "; ".join(problems))

def lab01_governance():
    visible = spark.table(paths.table("riders")).collect()
    if len(visible) == 1 and visible[0]["email"].startswith("*****"):
        return "PASS", "row filter and column mask active"
    return "FAIL", f"expected 1 masked row, got {len(visible)} rows"

check("01", "Delta history: MERGE/DELETE/UPDATE/RESTORE/OPTIMIZE", lab01_history)
check("01", "Clustering, CDF, constraint, schema evolution", lab01_features)
check("01", "Unity Catalog row filter and column mask", lab01_governance)

# ---------------------------------------------------------------- Lab 02
check("02", "CTAS stations_ctas", lambda: (
    ("PASS", "25 rows") if count("stations_ctas") == 25 else ("FAIL", "expected 25 rows")))

def lab02_copy():
    df = spark.table(paths.table("trips_montreal_copy"))
    files = {r.source_file for r in df.select("source_file").distinct().collect()}
    n, want = df.count(), rows_for_files(files)
    if len(files) < 2:
        return "FAIL", "load at least two batches (run COPY INTO after generating batch 2)"
    return ("PASS", f"{n} rows from {len(files)} files, no duplicates") if n == want \
        else ("FAIL", f"{n} rows but the files contain {want}: duplicates?")

def lab02_autoloader():
    df = spark.table(paths.table("trips_lisbon_autoloader"))
    files = {r.source_file for r in df.select("source_file").distinct().collect()}
    problems = []
    if "helmet_used" not in df.columns:
        problems.append("schema drift not handled (no helmet_used)")
    if df.where("_rescued_data IS NOT NULL").count() == 0:
        problems.append("no rescued data found")
    if df.count() != rows_for_files(files):
        problems.append("row count does not match the files")
    return ("PASS", f"{len(files)} files, drift + rescued data OK") if not problems else ("FAIL", "; ".join(problems))

check("02", "COPY INTO is idempotent", lab02_copy)
check("02", "Auto Loader with schema drift and rescued data", lab02_autoloader)

# ---------------------------------------------------------------- Lab 03
def lab03_stations():
    b, s = count("stations_bronze"), count("stations_silver")
    return ("PASS", "SEL-99 dropped by the expectation") if (b, s) == (25, 24) else ("FAIL", f"bronze {b}, silver {s}")

def lab03_trips():
    bronze = spark.table(paths.table("trips_bronze"))
    files = {r.source_file for r in bronze.select("source_file").distinct().collect()}
    n_bronze, want = bronze.count(), rows_for_files(files)
    n_quarantine, want_bad = count("trips_quarantine"), bad_for_files(files)
    n_silver, n_enriched = count("trips_silver"), count("trips_enriched")
    problems = []
    if n_bronze != want:
        problems.append(f"bronze {n_bronze} != {want} rows in its files")
    if n_quarantine != want_bad:
        problems.append(f"quarantine {n_quarantine} != {want_bad} broken trips")
    if n_silver + n_quarantine != n_bronze:
        problems.append("silver + quarantine != bronze")
    if n_enriched != n_silver:
        problems.append("enriched != silver (run the pipeline again)")
    detail = f"{n_bronze} bronze = {n_silver} silver + {n_quarantine} quarantined, from {len(files)} files"
    return ("PASS", detail) if not problems else ("FAIL", "; ".join(problems))

def lab03_gold():
    gold = ["gold_station_daily", "gold_city_quality", "gold_quarantine_reasons",
            "gold_revenue_by_plan", "gold_members_by_plan"]
    empty = [t for t in gold if count(t) == 0]
    return ("PASS", "5 gold tables populated") if not empty else ("FAIL", f"empty: {empty}")

def lab03_fail_drill():
    if not state["poison_files"]:
        return "TODO", "run the FAIL UPDATE drill (section 5)"
    fixed = spark.sql(f"SELECT COUNT(*) AS n FROM {paths.table('trips_bronze')} "
                      "WHERE source_file LIKE 'poison%'").first()["n"]
    return ("PASS", "bad file fixed and ingested") if fixed > 0 else ("FAIL", "fix the file and re-run the pipeline")

def lab03_challenge():
    return ("PASS", f"{count('gold_hourly_demand')} rows") if exists("gold_hourly_demand") \
        else ("TODO", "optional challenge: gold_hourly_demand")

check("03", "Stations: DROP expectation", lab03_stations)
check("03", "Trips: bronze = silver + quarantine, exact", lab03_trips)
check("03", "Gold materialized views", lab03_gold)
check("03", "FAIL UPDATE drill", lab03_fail_drill)
check("03", "Challenge: gold_hourly_demand", lab03_challenge)

# ---------------------------------------------------------------- Lab 04
def lab04_out_of_order():
    ingested = {r.source_file for r in spark.table(paths.table("members_cdc_bronze"))
                .select("source_file").distinct().collect()}
    to_check = {m: i for m, i in state["out_of_order_members"].items()
                if f"batch_{i['batch']:04d}.json" in ingested}
    if not to_check:
        return "TODO", "generate 2+ batches and run the pipeline"
    current = {r.member_id: r.plan for r in spark.table(paths.table("members_current")).collect()}
    wrong = [m for m, i in to_check.items() if current.get(m) != i["expected_plan"]]
    return ("PASS", f"{len(to_check)} out-of-order member(s) resolved by SEQUENCE BY") if not wrong \
        else ("FAIL", f"wrong plan for {wrong}")

def lab04_scd2():
    closed = spark.sql(f"SELECT COUNT(*) AS n FROM {paths.table('members_history')} "
                       "WHERE __END_AT IS NOT NULL").first()["n"]
    return ("PASS", f"{closed} closed versions in history") if closed > 0 else ("FAIL", "no history yet")

check("04", "SCD1 respects event order", lab04_out_of_order)
check("04", "SCD2 keeps history", lab04_scd2)

# ---------------------------------------------------------------- Lab 05
def lab05_job():
    from databricks.sdk import WorkspaceClient
    names = [j.settings.name for j in WorkspaceClient().jobs.list() if j.settings]
    found = [n for n in names if "pedalhouse-daily" in (n or "")]
    return ("PASS", found[0]) if found else ("TODO", "deploy the bundle (lab 05 part B)")

def lab05_runs():
    reports = spark.table(paths.table("city_daily_report"))
    cities = {r.city for r in reports.select("city").distinct().collect()}
    runs = count("job_run_log")
    if cities != set(CITIES):
        return "FAIL", f"city reports for {sorted(cities)} only"
    return "PASS", f"{runs} logged run(s), reports for all 3 cities"

def lab05_false_branch():
    n = count("quality_alerts")
    return ("PASS", f"{n} alert(s) raised") if n else ("TODO", "run the job with bad_rate=0.25")

check("05", "pedalhouse-daily job deployed", lab05_job)
check("05", "Job ran: For each + Run if", lab05_runs)
check("05", "If/else false branch exercised", lab05_false_branch)

# COMMAND ----------

report = spark.createDataFrame(results, "lab STRING, check STRING, status STRING, detail STRING")
display(report)

score = sum(1 for r in results if r[2] == "PASS")
print(f"\nScore: {score} / {len(results)} checks passed")
