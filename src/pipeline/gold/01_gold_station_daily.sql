-- =====================================================================
-- GOLD | gold_station_daily  (MATERIALIZED VIEW)
-- Business question: which stations are busiest, and what do they earn?
-- An aggregation must UPDATE existing rows when new trips arrive, which
-- an append-only streaming table cannot do. That is why gold = MVs.
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_station_daily
COMMENT "Trips, revenue and averages per departure station per day."
AS SELECT
  city,
  start_station_id                                        AS station_id,
  start_station_name                                      AS station_name,
  DATE(started_at)                                        AS ride_date,
  COUNT(*)                                                AS trips,
  SUM(CASE WHEN bike_type = 'electric' THEN 1 ELSE 0 END) AS electric_trips,
  SUM(CASE WHEN is_round_trip THEN 1 ELSE 0 END)          AS round_trips,
  ROUND(SUM(base_fare), 2)                                AS gross_revenue,
  ROUND(AVG(duration_min), 1)                             AS avg_duration_min,
  ROUND(AVG(distance_km), 2)                              AS avg_distance_km
FROM trips_enriched
GROUP BY city, start_station_id, start_station_name, DATE(started_at);
