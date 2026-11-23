-- =====================================================================
-- GOLD | gold_members_by_plan  (MATERIALIZED VIEW over the SCD1 table)
-- How many active members does each city have, on each plan, right now?
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_members_by_plan
COMMENT "Active members per home city and plan."
AS SELECT
  home_city,
  plan,
  COUNT(*) AS members
FROM members_current
GROUP BY home_city, plan;
