-- =====================================================================
-- BRONZE | stations_bronze  (MATERIALIZED VIEW over raw files)
-- ---------------------------------------------------------------------
-- Station reference data is tiny and changes rarely, so we simply re-read
-- the whole CSV on every update. A materialized view is perfect for that:
-- it stores the result of a query and keeps it up to date.
--
-- ${landing_path} is a pipeline configuration value. It is set in
-- resources/pedalhouse.pipeline.yml, so the same code works in dev and prod.
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW stations_bronze
COMMENT "Raw station reference data, re-read in full on every update."
AS SELECT
  station_id,
  station_name,
  city,
  lat,
  lon,
  capacity,
  _rescued_data,
  _metadata.file_name AS source_file
FROM read_files(
  '${landing_path}/stations/',
  format => 'csv',
  header => true,
  schemaHints => 'lat DOUBLE, lon DOUBLE, capacity INT',
  rescuedDataColumn => '_rescued_data'
);
