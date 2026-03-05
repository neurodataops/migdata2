-- Object     : tpch_sf10.region
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 02a65d149e55efa4ea743bb6dfca59dec1aac930451e8b6a89318beba0bd811b
-- Generated  : 2026-03-05T17:04:00.694354
-- ---

CREATE TABLE tpch_sf10.region (
 r_regionkey NUMBER NOT NULL,
 r_name TEXT NOT NULL,
 r_comment TEXT
)

USING DELTA;