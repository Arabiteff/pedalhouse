-- =====================================================================
-- SILVER | stations_silver  (MATERIALIZED VIEW with a DROP expectation)
-- ---------------------------------------------------------------------
-- A station without GPS coordinates is useless for distance calculations.
-- ON VIOLATION DROP ROW removes it from this table and counts it in the
-- pipeline event log. The source data in bronze is untouched.
--
-- Expected result: 25 stations in bronze, 24 here (SEL-99 is dropped).
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW stations_silver (
  CONSTRAINT valid_coordinates
    EXPECT (lat IS NOT NULL AND lon IS NOT NULL
            AND lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180)
    ON VIOLATION DROP ROW
)
COMMENT "Clean station reference data: only stations with valid coordinates."
AS SELECT
  station_id,
  station_name,
  city,
  lat,
  lon,
  capacity
FROM stations_bronze;
