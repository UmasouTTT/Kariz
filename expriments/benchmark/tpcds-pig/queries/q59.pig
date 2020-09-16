IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q59

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();


-- WSS
J1 = JOIN SS BY ss_sold_date_sk, D BY d_date_sk;
F1 = FOREACH J1 GENERATE
	d_week_seq,
	ss_store_sk,
	(d_day_name == 'Sunday' ? ss_sales_price : 0) AS sun_sales,
	(d_day_name == 'Monday' ? ss_sales_price : 0) AS mon_sales,
	(d_day_name == 'Tuesday' ? ss_sales_price : 0) AS tue_sales,
	(d_day_name == 'Wednesday' ? ss_sales_price : 0) AS wed_sales,
	(d_day_name == 'Thursday' ? ss_sales_price : 0) AS thu_sales,
	(d_day_name == 'Friday' ? ss_sales_price : 0) AS fri_sales,
	(d_day_name == 'Saturday' ? ss_sales_price : 0) AS sat_sales;
GF1 = GROUP F1 BY (d_week_seq, ss_store_sk);
WSS = FOREACH GF1 GENERATE
	group.d_week_seq,
    group.ss_store_sk,
    SUM(F1.sun_sales) AS sun_sales,
	SUM(F1.mon_sales) AS mon_sales,
	SUM(F1.tue_sales) AS tue_sales,
	SUM(F1.wed_sales) AS wed_sales,
	SUM(F1.thu_sales) AS thu_sales,
	SUM(F1.fri_sales) AS fri_sales,
	SUM(F1.sat_sales) AS sat_sales;

JWSS1 = JOIN WSS BY ss_store_sk, S BY s_store_sk;
JWSS = JOIN JWSS1 BY d_week_seq, D BY d_week_seq;

-- Y
FJWSS1 = FILTER JWSS BY d_month_seq >= 1185 AND d_month_seq <= 1197;
Y = FOREACH FJWSS1 GENERATE
	s_store_name AS s_store_name1,
    D::d_week_seq AS d_week_seq1,
    s_store_id AS s_store_id1,
    sun_sales AS sun_sales1,
    mon_sales AS mon_sales1,
    tue_sales AS tue_sales1,
    wed_sales AS wed_sales1,
    thu_sales AS thu_sales1,
    fri_sales AS fri_sales1,
    sat_sales AS sat_sales1;

-- X
FJWSS2 = FILTER JWSS BY d_month_seq >= 1197 AND d_month_seq <= 1208;
X = FOREACH FJWSS2 GENERATE
	s_store_name AS s_store_name2,
    D::d_week_seq AS d_week_seq2,
    s_store_id AS s_store_id2,
    sun_sales AS sun_sales2,
    mon_sales AS mon_sales2,
    tue_sales AS tue_sales2,
    wed_sales AS wed_sales2,
    thu_sales AS thu_sales2,
    fri_sales AS fri_sales2,
    sat_sales AS sat_sales2;

XY = JOIN Y BY s_store_id1, X BY s_store_id2;

FXY = FILTER XY BY d_week_seq1 == d_week_seq2 - 52;

FFXY = FOREACH FXY GENERATE
	s_store_name1,
    s_store_id1,
    d_week_seq1,
    sun_sales1 / sun_sales2,
    mon_sales1 / mon_sales2,
    tue_sales1 / tue_sales2,
    wed_sales1 / wed_sales2,
    thu_sales1 / thu_sales2,
    fri_sales1 / fri_sales2,
    sat_sales1 / sat_sales2;

O1 = ORDER FFXY BY
	s_store_name1,
    s_store_id1,
    d_week_seq1;

L1 = LIMIT O1 100;

STORE L1 INTO '$output_path/Q59' USING PigStorage('|');

