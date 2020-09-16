IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q43

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

FS = FILTER S BY s_gmt_offset == -5.0;
FD = FILTER D BY d_year == 1998;
FSS = FILTER SS BY ss_sold_date_sk >= 2450816 AND ss_sold_date_sk <= 2451179;

J1 = JOIN FSS BY ss_sold_date_sk, FD BY d_date_sk;
J2 = JOIN J1 BY ss_store_sk, FS BY s_store_sk;

F1 = FOREACH J2 GENERATE
	s_store_name,
	s_store_id,
	(d_day_name == 'Sunday' ? ss_sales_price : 0) AS sun_sales,
	(d_day_name == 'Monday' ? ss_sales_price : 0) AS mon_sales,
	(d_day_name == 'Tuesday' ? ss_sales_price : 0) AS tue_sales,
	(d_day_name == 'Wednesday' ? ss_sales_price : 0) AS wed_sales,
	(d_day_name == 'Thursday' ? ss_sales_price : 0) AS thu_sales,
	(d_day_name == 'Friday' ? ss_sales_price : 0) AS fri_sales,
	(d_day_name == 'Saturday' ? ss_sales_price : 0) AS sat_sales;

G1 = GROUP F1 BY (s_store_name, s_store_id);

F2 = FOREACH G1 GENERATE
	group.s_store_name,
	group.s_store_id,
	SUM(F1.sun_sales) AS sun_sales,
	SUM(F1.mon_sales) AS mon_sales,
	SUM(F1.tue_sales) AS tue_sales,
	SUM(F1.wed_sales) AS wed_sales,
	SUM(F1.thu_sales) AS thu_sales,
	SUM(F1.fri_sales) AS fri_sales,
	SUM(F1.sat_sales) AS sat_sales;

O1 = ORDER F2 BY
	s_store_name,
	s_store_id,
	sun_sales,
	mon_sales,
	tue_sales,
	wed_sales,
	thu_sales,
	fri_sales,
	sat_sales;

L1 = LIMIT O1 100;

STORE L1 INTO '$output_path/Q43' USING PigStorage('|');

