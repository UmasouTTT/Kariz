IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q07

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

FCD = FILTER CD BY cd_gender == 'F' AND cd_marital_status == 'W' AND cd_education_status == 'Primary';
FP = FILTER P BY p_channel_email == 'N' OR p_channel_event == 'N';
FD = FILTER D BY d_year == 1998;
FSS = FILTER SS BY ss_sold_date_sk >= 2450815 AND ss_sold_date_sk <= 2451179;

J1 = JOIN FSS BY ss_sold_date_sk, FD BY d_date_sk;
J2 = JOIN J1 BY ss_item_sk, I BY i_item_sk;
J3 = JOIN J2 BY ss_cdemo_sk, FCD BY cd_demo_sk;
J4 = JOIN J3 BY ss_promo_sk, FP BY p_promo_sk;

G1 = GROUP J4 BY i_item_id;

F1 = FOREACH G1 GENERATE
	group AS i_item_id,
	AVG(J4.ss_quantity),
	AVG(J4.ss_list_price),
	AVG(J4.ss_coupon_amt),
	AVG(J4.ss_sales_price);

O1 = ORDER F1 BY i_item_id;

L1 = LIMIT O1 100;

STORE L1 INTO '$output_path/Q07' USING PigStorage('|');
