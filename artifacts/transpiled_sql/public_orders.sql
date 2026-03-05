-- Object     : public.orders
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 4/10
-- Rules      : remove_cluster_by, remove_data_retention, number_to_bigint, number_to_decimal, timestamp_ntz, varchar_to_string, append_using_delta
-- Warnings   : 0
-- SHA-256    : 1cdf298e7dbebb9ca19f17dde1b58530d5330292a810e4b17a40182132440d8d
-- Generated  : 2026-03-04T16:31:48.055072
-- ---

CREATE TABLE public.orders (
 order_id BIGINT NOT NULL,
 customer_id BIGINT NOT NULL,
 order_date DATE NOT NULL,
 status STRING NOT NULL,
 total_amount DECIMAL(14,2) NOT NULL,
 discount_amount DECIMAL(10,2),
 shipping_cost DECIMAL(8,2),
 currency_code STRING NOT NULL,
 created_at TIMESTAMP NOT NULL,
 warehouse_id BIGINT
)


USING DELTA;