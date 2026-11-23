-- =====================================================================
-- GOLD | data quality tables  (MATERIALIZED VIEWS)
-- Data quality is a product too: the operations team wants to see how
-- much of each city's data is unusable, and why.
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_city_quality
COMMENT "Share of quarantined trips per city and source file."
AS SELECT
  city,
  source_file,
  COUNT(*)                                                    AS total_trips,
  SUM(CASE WHEN is_quarantined THEN 1 ELSE 0 END)             AS quarantined_trips,
  ROUND(100 * AVG(CASE WHEN is_quarantined THEN 1 ELSE 0 END), 2) AS quarantined_pct,
  SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END)  AS rows_with_rescued_data
FROM trips_flagged
GROUP BY city, source_file;


CREATE OR REFRESH MATERIALIZED VIEW gold_quarantine_reasons
COMMENT "How many quarantined trips broke each quality rule, per city."
AS SELECT
  city,
  rule                AS broken_rule,
  COUNT(*)            AS trips
FROM (
  SELECT city, explode(failed_rules) AS rule
  FROM trips_quarantine
)
GROUP BY city, rule;
