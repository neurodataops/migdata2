-- Object     : staging.stg_products_raw
-- Type       : TABLE
-- Class      : CONVERT_WITH_WARNINGS
-- Difficulty : 6/10
-- Rules      : remove_transient, remove_data_retention, number_to_bigint, variant_to_string, timestamp_ntz, varchar_to_string, append_using_delta
-- Warnings   : 1
-- SHA-256    : 42f7bd6302b3915c74365a468916fec44fbde5cdd99381fe59721bfc8a49e1c2
-- Generated  : 2026-03-04T16:31:48.095437
-- ---

CREATE TABLE staging.stg_products_raw (
 row_id BIGINT NOT NULL,
 raw_payload STRING,
 source_system STRING,
 ingested_at TIMESTAMP NOT NULL,
 file_name STRING,
 batch_id STRING,
 is_processed BOOLEAN NOT NULL
)

USING DELTA;