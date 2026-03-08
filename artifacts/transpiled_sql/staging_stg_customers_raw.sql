-- Object     : staging.stg_customers_raw
-- Type       : TABLE
-- Class      : CONVERT_WITH_WARNINGS
-- Difficulty : 6/10
-- Rules      : remove_transient, remove_data_retention, number_to_bigint, variant_to_string, timestamp_ntz, varchar_to_string, append_using_delta
-- Warnings   : 1
-- SHA-256    : e060c996a34a0db00a589869b10fa6ce88906ca1dd926e31ad7415ca700749e3
-- Generated  : 2026-03-08T12:18:43.203713
-- ---

CREATE TABLE staging.stg_customers_raw (
 row_id BIGINT NOT NULL,
 raw_payload STRING,
 source_system STRING,
 ingested_at TIMESTAMP NOT NULL,
 file_name STRING,
 batch_id STRING,
 is_processed BOOLEAN NOT NULL
)

USING DELTA;