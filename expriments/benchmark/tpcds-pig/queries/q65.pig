IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q65

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

FD = FILTER D BY d_month_seq >= 1212 AND d_month_seq <= 1223;
FSS = FILTER SS BY ss_sold_date_sk >= 2451911 AND ss_sold_date_sk <= 2452275;

J1 = JOIN FSS BY ss_sold_date_sk, FD BY d_date_sk;

G1 = GROUP J1 BY (ss_store_sk, ss_item_sk);

F1 = FOREACH G1 GENERATE
	group.ss_store_sk,
	group.ss_item_sk,
	SUM(J1.ss_sales_price) as revenue;

GF1 = GROUP F1 BY ss_store_sk;

FGF1 = FOREACH GF1 GENERATE
	group AS store_sk,
    AVG(F1.revenue) as ave;

J1F = JOIN FGF1 BY store_sk, F1 BY ss_store_sk;


FJ1F = FILTER J1F BY revenue <= 0.1 * ave;

JFJ1F = JOIN FJ1F BY ss_item_sk, I BY i_item_sk;
JJFJ1F = JOIN JFJ1F BY ss_store_sk, S BY s_store_sk;


FFJ2F = FOREACH JJFJ1F GENERATE
	s_store_name,
    i_item_desc,
    revenue,
    i_current_price,
    i_wholesale_cost,
    i_brand;

O1 = ORDER FFJ2F BY
	s_store_name,
    i_item_desc;

L1 = LIMIT O1 100;

STORE L1 INTO '$output_path/Q65' USING PigStorage('|');

