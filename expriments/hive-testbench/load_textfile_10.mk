all: date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory
date_dim:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/date_dim.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table date_dim (1/24).'
time_dim:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/time_dim.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table time_dim (2/24).'
item:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/item.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table item (3/24).'
customer:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/customer.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table customer (4/24).'
customer_demographics:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/customer_demographics.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table customer_demographics (5/24).'
household_demographics:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/household_demographics.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table household_demographics (6/24).'
customer_address:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/customer_address.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table customer_address (7/24).'
store:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/store.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table store (8/24).'
promotion:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/promotion.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table promotion (9/24).'
warehouse:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/warehouse.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table warehouse (10/24).'
ship_mode:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/ship_mode.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table ship_mode (11/24).'
reason:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/reason.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table reason (12/24).'
income_band:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/income_band.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table income_band (13/24).'
call_center:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/call_center.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table call_center (14/24).'
web_page:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/web_page.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table web_page (15/24).'
catalog_page:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/catalog_page.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table catalog_page (16/24).'
web_site:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/web_site.sql 	    -d DB=tpcds_bin_partitioned_textfile_10 -d SOURCE=tpcds_text_10             -d SCALE=10 	    -d REDUCERS=10 	    -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table web_site (17/24).'
store_sales:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/store_sales.sql 	    -d DB=tpcds_bin_partitioned_textfile_10             -d SCALE=10 	    -d SOURCE=tpcds_text_10 -d BUCKETS=1 	    -d RETURN_BUCKETS=1 -d REDUCERS=10 -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table store_sales (18/24).'
store_returns:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/store_returns.sql 	    -d DB=tpcds_bin_partitioned_textfile_10             -d SCALE=10 	    -d SOURCE=tpcds_text_10 -d BUCKETS=1 	    -d RETURN_BUCKETS=1 -d REDUCERS=10 -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table store_returns (19/24).'
web_sales:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/web_sales.sql 	    -d DB=tpcds_bin_partitioned_textfile_10             -d SCALE=10 	    -d SOURCE=tpcds_text_10 -d BUCKETS=1 	    -d RETURN_BUCKETS=1 -d REDUCERS=10 -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table web_sales (20/24).'
web_returns:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/web_returns.sql 	    -d DB=tpcds_bin_partitioned_textfile_10             -d SCALE=10 	    -d SOURCE=tpcds_text_10 -d BUCKETS=1 	    -d RETURN_BUCKETS=1 -d REDUCERS=10 -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table web_returns (21/24).'
catalog_sales:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/catalog_sales.sql 	    -d DB=tpcds_bin_partitioned_textfile_10             -d SCALE=10 	    -d SOURCE=tpcds_text_10 -d BUCKETS=1 	    -d RETURN_BUCKETS=1 -d REDUCERS=10 -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table catalog_sales (22/24).'
catalog_returns:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/catalog_returns.sql 	    -d DB=tpcds_bin_partitioned_textfile_10             -d SCALE=10 	    -d SOURCE=tpcds_text_10 -d BUCKETS=1 	    -d RETURN_BUCKETS=1 -d REDUCERS=10 -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table catalog_returns (23/24).'
inventory:
	@hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/inventory.sql 	    -d DB=tpcds_bin_partitioned_textfile_10             -d SCALE=10 	    -d SOURCE=tpcds_text_10 -d BUCKETS=1 	    -d RETURN_BUCKETS=1 -d REDUCERS=10 -d FILE=textfile 2> /dev/null 1> /dev/null && echo 'Optimizing table inventory (24/24).'
