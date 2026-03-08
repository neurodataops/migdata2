-- Object     : analytics.sessions
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 4/10
-- Rules      : remove_cluster_by, remove_data_retention, number_to_bigint, timestamp_ntz, varchar_to_string, append_using_delta
-- Warnings   : 0
-- SHA-256    : da989653d94c08ca7bf6962c29d47cea300f15fd9b460134d393fdbbde177e5e
-- Generated  : 2026-03-08T12:18:43.188709
-- ---

CREATE TABLE analytics.sessions (
 session_id STRING NOT NULL,
 user_id BIGINT NOT NULL,
 start_time TIMESTAMP NOT NULL,
 end_time TIMESTAMP,
 duration_seconds BIGINT,
 page_count BIGINT,
 device_type STRING,
 browser STRING,
 os STRING,
 country_code STRING
)


USING DELTA;