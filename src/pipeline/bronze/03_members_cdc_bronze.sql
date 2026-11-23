-- =====================================================================
-- BRONZE | members_cdc_bronze  (STREAMING TABLE of change events)
-- ---------------------------------------------------------------------
-- The membership system does not send "the current list of members".
-- It sends CHANGES: INSERT, UPDATE and DELETE events, sometimes out of
-- order. We land them as-is; silver turns them into SCD tables with
-- AUTO CDC (see ../silver/03_members_cdc.sql).
-- =====================================================================

CREATE OR REFRESH STREAMING TABLE members_cdc_bronze
COMMENT "Raw member change events (INSERT / UPDATE / DELETE), in arrival order."
AS SELECT
  member_id,
  full_name,
  email,
  home_city,
  plan,
  operation,
  event_ts,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '${landing_path}/members_cdc/',
  format => 'json',
  schemaHints => 'event_ts TIMESTAMP'
);
