# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 04 · Change data capture with AUTO CDC: SCD Type 1 and Type 2
# MAGIC
# MAGIC Course: **Build Data Pipelines with Lakeflow Spark Declarative Pipelines**
# MAGIC
# MAGIC The membership system sends **change events**, not tables:
# MAGIC
# MAGIC | operation | meaning |
# MAGIC |---|---|
# MAGIC | INSERT | a new member signed up |
# MAGIC | UPDATE | plan, city, email or name changed |
# MAGIC | DELETE | the member left |
# MAGIC
# MAGIC The file `src/pipeline/silver/03_members_cdc.sql` applies this one feed to two targets:
# MAGIC `members_current` (SCD Type 1) and `members_history` (SCD Type 2). Run the pipeline at
# MAGIC least once after Lab 03 before starting.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

state = datagen.load_state(paths.landing)
print("Batches generated:", state["next_batch"] - 1)
print("Members ever created:", len(state["members"]),
      "| still active:", sum(1 for m in state["members"].values() if m["active"]))
print("Members with out-of-order events:", state["out_of_order_members"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The raw feed
# MAGIC
# MAGIC Pick the first member whose events arrived **out of order**: the generator wrote the
# MAGIC 18:00 update *before* the 09:00 update in the same file.

# COMMAND ----------

disordered = sorted(state["out_of_order_members"])
if not disordered:
    raise ValueError("Generate at least 2 batches and run the pipeline first (Lab 03).")
member = disordered[0]
expected_plan = state["out_of_order_members"][member]["expected_plan"]
print(f"Member {member}: the last real change (18:00) set the plan to '{expected_plan}'")

display(spark.sql(f"""
    SELECT member_id, plan, operation, event_ts, source_file
    FROM members_cdc_bronze
    WHERE member_id = '{member}'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SCD Type 1: only the latest state
# MAGIC
# MAGIC `SEQUENCE BY event_ts` makes AUTO CDC apply the events in **event time** order, not
# MAGIC arrival order. So the plan below must be the 18:00 one, even though the 09:00 event
# MAGIC was processed last.

# COMMAND ----------

current = spark.sql(f"SELECT * FROM members_current WHERE member_id = '{member}'")
display(current)
actual_plan = current.first()["plan"] if current.count() else None
print("PASS" if actual_plan == expected_plan else "CHECK",
      f"- expected '{expected_plan}', found '{actual_plan}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD Type 2: the full history
# MAGIC
# MAGIC One row per **version** of the member. `__START_AT` and `__END_AT` come from the
# MAGIC `SEQUENCE BY` column. `__END_AT IS NULL` marks the current version.

# COMMAND ----------

display(spark.sql(f"""
    SELECT member_id, plan, home_city, email, __START_AT, __END_AT
    FROM members_history
    WHERE member_id = '{member}'
    ORDER BY __START_AT
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Type 1 vs Type 2, side by side

# COMMAND ----------

display(spark.sql("""
    SELECT 'members_current (SCD1)'                       AS table_name,
           COUNT(*)                                       AS rows,
           COUNT(DISTINCT member_id)                      AS members
    FROM members_current
    UNION ALL
    SELECT 'members_history (SCD2)', COUNT(*), COUNT(DISTINCT member_id)
    FROM members_history
    UNION ALL
    SELECT 'members_history, current versions only', COUNT(*), COUNT(DISTINCT member_id)
    FROM members_history WHERE __END_AT IS NULL
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Deleted members:** gone from SCD1, but in SCD2 their last version is simply closed.
# MAGIC That is why finance can still explain last month's revenue after a member leaves.

# COMMAND ----------

display(spark.sql("""
    SELECT h.member_id, h.plan, h.__START_AT, h.__END_AT,
           c.member_id IS NOT NULL AS still_in_current
    FROM members_history AS h
    LEFT JOIN members_current AS c USING (member_id)
    WHERE c.member_id IS NULL
    ORDER BY h.__END_AT DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **TRACK HISTORY ON \* EXCEPT (email, full_name):** an email change updates the current
# MAGIC version in place instead of creating a new one. Find members whose email changed
# MAGIC (the generator adds `.b<batch>` to it) and count their versions.

# COMMAND ----------

display(spark.sql("""
    SELECT member_id, email, COUNT(*) OVER (PARTITION BY member_id) AS versions, __START_AT, __END_AT
    FROM members_history
    WHERE member_id IN (SELECT member_id FROM members_current WHERE email LIKE '%.b%@example.com')
    ORDER BY member_id, __START_AT
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Why SCD2 matters: the point-in-time join
# MAGIC
# MAGIC Plans change during the day, and the discount depends on the plan. `gold_revenue_by_plan`
# MAGIC joins each trip to the member version that was valid **when the trip started**.
# MAGIC Find a member who changed plan and rode both before and after the change:

# COMMAND ----------

display(spark.sql("""
    WITH changed AS (
      SELECT member_id FROM members_history GROUP BY member_id HAVING COUNT(*) > 1
    )
    SELECT t.member_id, t.trip_id, t.started_at, h.plan AS plan_at_trip_time,
           h.__START_AT AS plan_valid_from, h.__END_AT AS plan_valid_to, t.base_fare
    FROM trips_enriched AS t
    JOIN changed USING (member_id)
    JOIN members_history AS h
      ON  t.member_id = h.member_id
      AND t.started_at >= h.__START_AT
      AND (t.started_at < h.__END_AT OR h.__END_AT IS NULL)
    ORDER BY t.member_id, t.started_at
    LIMIT 30
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT plan, SUM(trips) AS trips, SUM(gross_revenue) AS gross, SUM(net_revenue) AS net
    FROM gold_revenue_by_plan
    GROUP BY plan
    ORDER BY net DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge
# MAGIC
# MAGIC 1. "Which plan did member X have on day 2 at 10:00?" Write the as-of query on `members_history`.
# MAGIC 2. How many members upgraded (basic to plus/pro, or plus to pro) across all batches?
# MAGIC
# MAGIC Solutions below.

# COMMAND ----------

as_of = "2026-11-03 10:00:00"
display(spark.sql(f"""
    SELECT member_id, plan, home_city
    FROM members_history
    WHERE member_id = '{member}'
      AND TIMESTAMP'{as_of}' >= __START_AT
      AND (TIMESTAMP'{as_of}' < __END_AT OR __END_AT IS NULL)
"""))

display(spark.sql("""
    WITH ranked AS (
      SELECT member_id, plan,
             LAG(plan) OVER (PARTITION BY member_id ORDER BY __START_AT) AS previous_plan
      FROM members_history
    ),
    levels AS (
      SELECT *, CASE plan WHEN 'basic' THEN 1 WHEN 'plus' THEN 2 WHEN 'pro' THEN 3 END AS lvl,
                CASE previous_plan WHEN 'basic' THEN 1 WHEN 'plus' THEN 2 WHEN 'pro' THEN 3 END AS prev_lvl
      FROM ranked
    )
    SELECT COUNT(*) AS upgrades FROM levels WHERE lvl > prev_lvl
"""))
