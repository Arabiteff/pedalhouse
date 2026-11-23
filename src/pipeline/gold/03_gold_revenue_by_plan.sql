-- =====================================================================
-- GOLD | gold_revenue_by_plan  (MATERIALIZED VIEW, point-in-time join)
-- ---------------------------------------------------------------------
-- Members get a discount that depends on their plan. But plans CHANGE
-- during the day. Which plan applies to a trip? The one that was valid
-- WHEN THE TRIP STARTED. That is exactly what SCD Type 2 makes possible:
-- join on the key AND on the validity window.
--
-- Casual riders (no member_id) pay full price.
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_by_plan
COMMENT "Gross and net revenue per city, day and plan, using the plan valid at trip time."
AS SELECT
  t.city,
  DATE(t.started_at)                                              AS ride_date,
  CASE
    WHEN t.member_id IS NULL THEN 'casual'
    ELSE COALESCE(m.plan, 'unknown')
  END                                                             AS plan,
  COUNT(*)                                                        AS trips,
  ROUND(SUM(t.base_fare), 2)                                      AS gross_revenue,
  ROUND(SUM(t.base_fare * (1 - CASE m.plan
                                 WHEN 'pro'   THEN 0.50
                                 WHEN 'plus'  THEN 0.25
                                 WHEN 'basic' THEN 0.10
                                 ELSE 0.0
                               END)), 2)                          AS net_revenue
FROM trips_enriched AS t
LEFT JOIN members_history AS m
  ON  t.member_id = m.member_id
  AND t.started_at >= m.__START_AT
  AND (t.started_at < m.__END_AT OR m.__END_AT IS NULL)
GROUP BY
  t.city,
  DATE(t.started_at),
  CASE WHEN t.member_id IS NULL THEN 'casual' ELSE COALESCE(m.plan, 'unknown') END;
