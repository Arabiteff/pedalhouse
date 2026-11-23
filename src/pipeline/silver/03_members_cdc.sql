-- =====================================================================
-- SILVER | members: AUTO CDC into SCD Type 1 AND SCD Type 2
-- ---------------------------------------------------------------------
-- Same change feed, two different targets:
--
--   members_current  (SCD TYPE 1) one row per member, latest values only,
--                     deleted members disappear.
--   members_history  (SCD TYPE 2) one row per VERSION of a member, with
--                     __START_AT / __END_AT. __END_AT IS NULL = current.
--
-- KEYS         identifies the record
-- SEQUENCE BY  orders the events, so an out-of-order event cannot
--              overwrite a newer one (the generator sends some on purpose)
-- APPLY AS DELETE WHEN  turns 'DELETE' events into deletes
-- TRACK HISTORY ON      only plan and home_city create a new version; an
--                       email or name change updates the current version
--                       in place instead of adding history
-- =====================================================================

CREATE OR REFRESH STREAMING TABLE members_current
COMMENT "SCD Type 1: the current state of every active member.";

CREATE FLOW members_current_cdc
AS AUTO CDC INTO members_current
FROM STREAM(members_cdc_bronze)
KEYS (member_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY event_ts
COLUMNS * EXCEPT (operation, source_file)
STORED AS SCD TYPE 1;


CREATE OR REFRESH STREAMING TABLE members_history
COMMENT "SCD Type 2: every plan or city each member ever had, with validity dates.";

CREATE FLOW members_history_cdc
AS AUTO CDC INTO members_history
FROM STREAM(members_cdc_bronze)
KEYS (member_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY event_ts
COLUMNS * EXCEPT (operation, source_file)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (email, full_name);
