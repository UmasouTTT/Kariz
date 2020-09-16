IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q89

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

FD = FILTER D BY d_year == 2000;
FI = FILTER I BY
	(
	i_category MATCHES 'Home|Books|Electronics' AND
	i_class MATCHES 'wallpaper|parenting|musical'
	) OR
	(
	i_category MATCHES 'Shoes|Jewelry|Men' AND
	i_class MATCHES 'womens|birdal|pants'
	);
FSS = FILTER SS BY ss_sold_date_sk >= 2451545 AND ss_sold_date_sk <= 2451910;

J1 = JOIN FSS BY ss_sold_date_sk, FD BY d_date_sk;
J2 = JOIN J1 BY ss_item_sk, FI BY i_item_sk;
J3 = JOIN J2 BY ss_store_sk, S BY s_store_sk;

-- windowing
GW = GROUP J3 BY (i_category, i_brand, s_store_name, s_company_name);
FW = FOREACH GW GENERATE
	group.i_category AS category,
	group.i_brand AS brand,
	group.s_store_name AS store_name,
	group.s_company_name AS company_name,
	SUM(J3.ss_sales_price) AS monthly_sales;

JFW = JOIN FW BY (category, brand, store_name, company_name), J3 BY (i_category, i_brand, s_store_name, s_company_name);

G1 = GROUP JFW BY (i_category, i_class, i_brand, s_store_name, s_company_name, d_moy);

F1 = FOREACH G1 GENERATE
    	group.i_category,
	    group.i_class,
	    group.i_brand,
	    group.s_store_name,
	    group.s_company_name,
	    group.d_moy,
	    SUM(JFW.ss_sales_price) AS sum_sales,
	    AVG(JFW.monthly_sales) AS avg_monthly_sales,
	    SUM(JFW.ss_sales_price) - AVG(JFW.monthly_sales) AS diff;


FF1 = FILTER F1 BY
	avg_monthly_sales != 0 AND
	(ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales) * 10 > 1;

O1 = ORDER FF1 BY
	diff,
  	s_store_name;

FO1 = FOREACH O1 GENERATE
	i_category,
    i_class,
    i_brand,
    s_store_name,
    s_company_name,
    d_moy,
    sum_sales AS sum_sales,
    avg_monthly_sales AS avg_monthly_sales;

L1 = LIMIT FO1 100;

STORE L1 INTO '$output_path/Q89' USING PigStorage('|');
