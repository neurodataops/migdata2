-- Object     : staging.stg_clickstream
-- Type       : TABLE
-- Class      : CONVERT_WITH_WARNINGS
-- Difficulty : 6/10
-- Rules      : remove_cluster_by, remove_transient, remove_data_retention, number_to_bigint, variant_to_string, timestamp_ntz, varchar_to_string, append_using_delta
-- Warnings   : 1
-- SHA-256    : cb48fb542b1dd427dc8a7de862651aee8cec9a90654593275b4c2abf5a43d840
-- Generated  : 2026-03-04T16:31:48.102495
-- ---

CREATE TABLE staging.stg_clickstream (
 row_id BIGINT NOT NULL,
 raw_payload STRING,
 source_system STRING,
 ingested_at TIMESTAMP NOT NULL,
 file_name STRING,
 batch_id STRING,
 is_processed BOOLEAN NOT NULL
)


USING DELTA;