-- =====================================================================
-- BRONZE | trips_bronze  (one STREAMING TABLE fed by THREE flows)
-- ---------------------------------------------------------------------
-- Each city sends trips its own way:
--   Lisbon   -> CSV            (adds a new column from batch 3: schema drift)
--   Montreal -> CSV
--   Seoul    -> nested JSON lines with different field names
--
-- Multi-flow pattern: declare the target once, then one CREATE FLOW per
-- source. Each flow has its own checkpoint, so you can add a fourth city
-- later without reprocessing the other three.
--
-- Data quality rules live on the TARGET table, never on a flow. Bronze
-- keeps everything raw, except the one thing we can never accept: a trip
-- without a key. That rule FAILS the update (see lab 03).
-- =====================================================================

CREATE OR REFRESH STREAMING TABLE trips_bronze (
  CONSTRAINT trip_id_present EXPECT (trip_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "Raw trips from all cities, appended incrementally. One row per trip event received."
TBLPROPERTIES (
  -- Protect raw history: a 'full refresh' will NOT truncate this table,
  -- because old source files may no longer exist to be re-read.
  'pipelines.reset.allowed' = 'false'
);

-- Lisbon: CSV. schemaHints types the timestamp columns. A value that cannot
-- be parsed (e.g. 'not-a-time') becomes NULL and the original text is kept
-- in _rescued_data instead of being lost.
CREATE FLOW trips_lisbon
AS INSERT INTO trips_bronze BY NAME
SELECT
  CAST(trip_id AS STRING)           AS trip_id,
  CAST(member_id AS STRING)         AS member_id,
  CAST(bike_id AS STRING)           AS bike_id,
  CAST(bike_type AS STRING)         AS bike_type,
  CAST(start_station_id AS STRING)  AS start_station_id,
  CAST(end_station_id AS STRING)    AS end_station_id,
  started_at,
  ended_at,
  'lisbon'                          AS city,
  _rescued_data,
  _metadata.file_name               AS source_file,
  current_timestamp()               AS ingested_at
FROM STREAM read_files(
  '${landing_path}/trips/lisbon/',
  format => 'csv',
  header => true,
  schemaHints => 'started_at TIMESTAMP, ended_at TIMESTAMP, trip_id STRING, member_id STRING',
  rescuedDataColumn => '_rescued_data'
);

-- Montreal: same CSV layout as Lisbon.
CREATE FLOW trips_montreal
AS INSERT INTO trips_bronze BY NAME
SELECT
  CAST(trip_id AS STRING)           AS trip_id,
  CAST(member_id AS STRING)         AS member_id,
  CAST(bike_id AS STRING)           AS bike_id,
  CAST(bike_type AS STRING)         AS bike_type,
  CAST(start_station_id AS STRING)  AS start_station_id,
  CAST(end_station_id AS STRING)    AS end_station_id,
  started_at,
  ended_at,
  'montreal'                        AS city,
  _rescued_data,
  _metadata.file_name               AS source_file,
  current_timestamp()               AS ingested_at
FROM STREAM read_files(
  '${landing_path}/trips/montreal/',
  format => 'csv',
  header => true,
  schemaHints => 'started_at TIMESTAMP, ended_at TIMESTAMP, trip_id STRING, member_id STRING',
  rescuedDataColumn => '_rescued_data'
);

-- Seoul: nested JSON. Flatten it with dot notation and rename to the common
-- layout. Timestamps arrive as text, so TRY_TO_TIMESTAMP turns a bad value
-- into NULL instead of failing the whole update.
CREATE FLOW trips_seoul
AS INSERT INTO trips_bronze BY NAME
SELECT
  CAST(trip_id AS STRING)            AS trip_id,
  CAST(rider.member_id AS STRING)    AS member_id,
  CAST(bike.bike_id AS STRING)       AS bike_id,
  CAST(bike.type AS STRING)          AS bike_type,
  CAST(route.from_station AS STRING) AS start_station_id,
  CAST(route.to_station AS STRING)   AS end_station_id,
  try_to_timestamp(times.start)      AS started_at,
  try_to_timestamp(times.end)        AS ended_at,
  'seoul'                            AS city,
  _rescued_data,
  _metadata.file_name                AS source_file,
  current_timestamp()                AS ingested_at
FROM STREAM read_files(
  '${landing_path}/trips/seoul/',
  format => 'json',
  rescuedDataColumn => '_rescued_data'
);
