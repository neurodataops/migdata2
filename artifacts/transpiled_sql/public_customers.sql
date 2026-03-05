-- Object     : public.customers
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 4/10
-- Rules      : remove_data_retention, number_to_bigint, number_to_decimal, timestamp_ntz, varchar_to_string, append_using_delta
-- Warnings   : 0
-- SHA-256    : 86201813cbb87247d23c4407362954168f5881cc5d396b9cfb747a0178d2c4a5
-- Generated  : 2026-03-04T16:31:48.049982
-- ---

CREATE TABLE public.customers (
 customer_id BIGINT NOT NULL,
 first_name STRING NOT NULL,
 last_name STRING NOT NULL,
 email STRING NOT NULL,
 phone STRING,
 created_at TIMESTAMP NOT NULL,
 updated_at TIMESTAMP,
 is_active BOOLEAN NOT NULL,
 lifetime_value DECIMAL(12,2),
 segment_code STRING
)

USING DELTA;