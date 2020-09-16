%default input_path '/tpcds/input'

DEFINE customer() RETURNS A {
  $A = LOAD '$input_path/customer' USING PigStorage('|') AS (
    c_customer_sk			:biginteger,
    c_customer_id			:chararray,
    c_current_cdemo_sk		:biginteger
    c_current_hdemo_sk		:biginteger,
    c_current_addr_sk		:biginteger,
    c_first_shipto_date_sk	:biginteger,
    c_first_sales_date_sk	:biginteger,
    c_salutation			:chararray,
    c_first_name			:chararray,
    c_last_name				:chararray,
    c_preferred_cust_flag	:chararray,
    c_birth_day				:biginteger,
    c_birth_month			:biginteger,
    c_birth_year			:biginteger,
    c_birth_country			:chararray,
    c_login					:chararray,
    c_email_address			:chararray,
    c_last_review_date_sk	:biginteger);
};

DEFINE customer_address() RETURNS A {
  $A = LOAD '$input_path/customer_address' USING PigStorage('|') AS (
    ca_address_sk		:biginteger,
    ca_address_id		:chararray,
    ca_street_number	:chararray,
    ca_street_name		:chararray,
    ca_street_type		:chararray,
    ca_suite_number		:chararray,
    ca_city				:chararray,
    ca_county			:chararray,
    ca_state			:chararray,
    ca_zip				:chararray,
    ca_country			:chararray,
    ca_gmt_offset		:double,
    ca_location_type	:chararray);
};

DEFINE customer_demographics() RETURNS A {
  $A = LOAD '$input_path/customer_demographics' USING PigStorage('|') AS (
    cd_demo_sk				:biginteger,
    cd_gender				:chararray,
    cd_marital_status		:chararray,
    cd_education_status		:chararray,
    cd_purchase_estimate	:biginteger,
    cd_credit_rating		:chararray,
    cd_dep_count			:biginteger,
    cd_dep_employed_count	:biginteger,
    cd_dep_college_count	:biginteger);
};

DEFINE date_dim() RETURNS A {
  $A = LOAD '$input_path/date_dim' USING PigStorage('|') AS (
    d_date_sk			:biginteger,
    d_date_id			:chararray,
    d_date				:chararray,
    d_month_seq			:biginteger,
    d_week_seq			:biginteger,
    d_quarter_seq		:biginteger,
    d_year				:biginteger,
    d_dow				:biginteger,
    d_moy				:biginteger,
    d_dom				:biginteger,
    d_qoy				:biginteger,
    d_fy_year			:biginteger,
    d_fy_quarter_seq	:biginteger,
    d_fy_week_seq		:biginteger,
    d_day_name			:chararray,
    d_quarter_name		:chararray,
    d_holiday			:chararray,
    d_weekend			:chararray,
    d_following_holiday	:chararray,
    d_first_dom			:biginteger,
    d_last_dom			:biginteger,
    d_same_day_ly		:biginteger,
    d_same_day_lq		:biginteger,
    d_current_day		:chararray,
    d_current_week		:chararray,
    d_current_month		:chararray,
    d_current_quarter	:chararray,
    d_current_year		:chararray);
};

DEFINE household_demographics() RETURNS A {
  $A = LOAD '$input_path/household_demographics' USING PigStorage('|') AS (
    hd_demo_sk			:biginteger,
    hd_income_band_sk	:biginteger,
    hd_buy_potential	:chararray,
    hd_dep_count		:biginteger,
    hd_vehicle_count	:biginteger);
};

DEFINE item() RETURNS A {
  $A = LOAD '$input_path/item' USING PigStorage('|') AS (
    i_item_sk			:biginteger,
    i_item_id			:chararray,
    i_rec_start_date	:chararray,
    i_rec_end_date		:chararray,
    i_item_desc			:chararray,
    i_current_price		:double,
    i_wholesale_cost	:double,
    i_brand_id			:biginteger,
    i_brand				:chararray,
    i_class_id			:biginteger,
    i_class				:chararray,
    i_category_id		:biginteger,
    i_category			:chararray,
    i_manufact_id		:biginteger,
    i_manufact			:chararray,
    i_size				:chararray,
    i_formulation		:chararray,
    i_color				:chararray,
    i_units				:chararray,
    i_container			:chararray,
    i_manager_id		:biginteger,
    i_product_name		:chararray);
};

DEFINE promotion() RETURNS A {
  $A = LOAD '$input_path/promotion' USING PigStorage('|') AS (
    p_promo_sk			:biginteger,
    p_promo_id			:chararray,
    p_start_date_sk		:biginteger,
    p_end_date_sk		:biginteger,
    p_item_sk			:biginteger,
    p_cost				:double,
    p_response_target	:biginteger,
    p_promo_name		:chararray,
    p_channel_dmail		:chararray,
    p_channel_email		:chararray,
    p_channel_catalog	:chararray,
    p_channel_tv		:chararray,
    p_channel_radio		:chararray,
    p_channel_press		:chararray,
    p_channel_event		:chararray,
    p_channel_demo		:chararray,
    p_channel_details	:chararray,
    p_purpose			:chararray,
    p_discount_active	:chararray);
};

DEFINE stores() RETURNS A {
  $A = LOAD '$input_path/store' USING PigStorage('|') AS (
    s_store_sk			:biginteger,
    s_store_id			:chararray,
    s_rec_start_date	:chararray,
    s_rec_end_date		:chararray,
    s_closed_date_sk	:biginteger,
    s_store_name		:chararray,
    s_number_employees	:biginteger,
    s_floor_space		:biginteger,
    s_hours				:chararray,
    s_manager			:chararray,
    s_market_id			:biginteger,
    s_geography_class	:chararray,
    s_market_desc		:chararray,
    s_market_manager	:chararray,
    s_division_id		:biginteger,
    s_division_name		:chararray,
    s_company_id		:biginteger,
    s_company_name		:chararray,
    s_street_number		:chararray,
    s_street_name		:chararray,
    s_street_type		:chararray,
    s_suite_number		:chararray,
    s_city				:chararray,
    s_county			:chararray,
    s_state				:chararray,
    s_zip				:chararray,
    s_country			:chararray,
    s_gmt_offset		:double,
    s_tax_precentage	:double);
};

DEFINE store_sales() RETURNS A {
  $A = LOAD '$input_path/store_sales' USING PigStorage('|') AS (
    ss_sold_date_sk			:biginteger,
    ss_sold_time_sk			:biginteger,
    ss_item_sk				:biginteger,
    ss_customer_sk			:biginteger,
    ss_cdemo_sk				:biginteger,
    ss_hdemo_sk				:biginteger,
    ss_addr_sk				:biginteger,
    ss_store_sk				:biginteger,
    ss_promo_sk				:biginteger,
    ss_ticket_number		:biginteger,
    ss_quantity				:biginteger,
    ss_wholesale_cost		:double,
    ss_list_price			:double,
    ss_sales_price			:double,
    ss_ext_discount_amt		:double,
    ss_ext_sales_price		:double,
    ss_ext_wholesale_cost	:double,
    ss_ext_list_price		:double,
    ss_ext_tax				:double,
    ss_coupon_amt			:double,
    ss_net_paid				:double,
    ss_net_paid_inc_tax		:double,
    ss_net_profit			:double);
};
