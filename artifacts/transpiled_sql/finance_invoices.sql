-- Object     : finance.invoices
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 4/10
-- Rules      : remove_cluster_by, remove_data_retention, number_to_bigint, number_to_decimal, timestamp_ntz, varchar_to_string, append_using_delta
-- Warnings   : 0
-- SHA-256    : 982f2ee1475bde2598f51ef6dc22378b0dd3d39bce96dc367c37787ca36c2382
-- Generated  : 2026-03-08T12:18:43.217711
-- ---

CREATE TABLE finance.invoices (
 id BIGINT NOT NULL,
 reference_number STRING NOT NULL,
 amount DECIMAL(14,2) NOT NULL,
 currency_code STRING NOT NULL,
 transaction_date DATE NOT NULL,
 status STRING NOT NULL,
 customer_id BIGINT,
 notes STRING,
 created_at TIMESTAMP NOT NULL,
 updated_at TIMESTAMP
)


USING DELTA;