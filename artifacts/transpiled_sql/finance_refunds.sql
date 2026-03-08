-- Object     : finance.refunds
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 4/10
-- Rules      : remove_cluster_by, remove_data_retention, number_to_bigint, number_to_decimal, timestamp_ntz, varchar_to_string, append_using_delta
-- Warnings   : 0
-- SHA-256    : 73caad46ed2d9c5358502dcb2e9459cd0f65ece7e674ddf29da34f16d42b2f34
-- Generated  : 2026-03-08T12:18:43.222720
-- ---

CREATE TABLE finance.refunds (
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