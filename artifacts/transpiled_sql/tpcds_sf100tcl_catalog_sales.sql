-- Object     : tpcds_sf100tcl.catalog_sales
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : ed0cf3fb1b6cd01528fc9b09a4db385466b2cf5867ee76eaec9b02694518f415
-- Generated  : 2026-03-08T16:35:46.934499
-- ---

CREATE TABLE tpcds_sf100tcl.catalog_sales (
 cs_sold_date_sk NUMBER,
 cs_sold_time_sk NUMBER,
 cs_ship_date_sk NUMBER,
 cs_bill_customer_sk NUMBER,
 cs_bill_cdemo_sk NUMBER,
 cs_bill_hdemo_sk NUMBER,
 cs_bill_addr_sk NUMBER,
 cs_ship_customer_sk NUMBER,
 cs_ship_cdemo_sk NUMBER,
 cs_ship_hdemo_sk NUMBER,
 cs_ship_addr_sk NUMBER,
 cs_call_center_sk NUMBER,
 cs_catalog_page_sk NUMBER,
 cs_ship_mode_sk NUMBER,
 cs_warehouse_sk NUMBER,
 cs_item_sk NUMBER NOT NULL,
 cs_promo_sk NUMBER,
 cs_order_number NUMBER NOT NULL,
 cs_quantity NUMBER,
 cs_wholesale_cost NUMBER,
 cs_list_price NUMBER,
 cs_sales_price NUMBER,
 cs_ext_discount_amt NUMBER,
 cs_ext_sales_price NUMBER,
 cs_ext_wholesale_cost NUMBER,
 cs_ext_list_price NUMBER,
 cs_ext_tax NUMBER,
 cs_coupon_amt NUMBER,
 cs_ext_ship_cost NUMBER,
 cs_net_paid NUMBER,
 cs_net_paid_inc_tax NUMBER,
 cs_net_paid_inc_ship NUMBER,
 cs_net_paid_inc_ship_tax NUMBER,
 cs_net_profit NUMBER
)

USING DELTA;