IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q27

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

FSS = FILTER SS BY ss_sold_date_sk >= 2451545 AND ss_sold_date_sk <= 2451910;
FCD = FILTER CD BY cd_gender == 'F' AND cd_marital_status == 'D' AND cd_education_status == 'Primary';
FD = FILTER D BY d_year == 2000;
FS = FILTER S BY s_state matches 'TN|AL|SD';

J1 = JOIN FSS BY ss_sold_date_sk, FD BY d_date_sk;
J2 = JOIN J1 BY ss_item_sk, I BY i_item_sk;
J3 = JOIN J2 BY ss_store_sk, FS BY s_store_sk;
J4 = JOIN J3 BY ss_cdemo_sk, FCD BY cd_demo_sk;

RESULTS = FOREACH J4 GENERATE
	i_item_id,
	s_state,
	ss_quantity AS agg1,
	ss_list_price AS agg2,
	ss_coupon_amt AS agg3,
	ss_sales_price AS agg4;

GR1 = GROUP RESULTS BY (i_item_id, s_state);
GR2 = GROUP RESULTS BY i_item_id;
GR3 = GROUP RESULTS ALL;

RES1 = FOREACH GR1 GENERATE
	group.i_item_id,
	group.s_state,
	0 AS g_state,
	AVG(RESULTS.agg1) AS agg1,
	AVG(RESULTS.agg2) AS agg2,
	AVG(RESULTS.agg3) AS agg3,
	AVG(RESULTS.agg4) AS agg4;

RES2 = FOREACH GR2 GENERATE
	group,
	NULL AS s_state,
	1 AS g_state,
	AVG(RESULTS.agg1) AS agg1,
	AVG(RESULTS.agg2) AS agg2,
	AVG(RESULTS.agg3) AS agg3,
	AVG(RESULTS.agg4) AS agg4;

RES3 = FOREACH GR3 GENERATE
	NULL AS i_item_id,
	NULL AS s_state,
	1 AS g_state,
	AVG(RESULTS.agg1) AS agg1,
	AVG(RESULTS.agg2) AS agg2,
	AVG(RESULTS.agg3) AS agg3,
	AVG(RESULTS.agg4) AS agg4;

URES = UNION RES1, RES2, RES3;

O1 = ORDER URES BY i_item_id, s_state;

L1 = LIMIT O1 100;

STORE L1 INTO '$output_path/Q27' USING PigStorage('|');

