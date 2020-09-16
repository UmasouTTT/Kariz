IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q79

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

FHD = FILTER HD BY hd_dep_count == 8 OR hd_vehicle_count > 0;
FD = FILTER D BY d_dow == 1 AND (d_year == 1998 OR d_year == 1999 OR d_year == 2000);
FS = FILTER S BY s_number_employees >= 200 AND s_number_employees <= 295;
FSS = FILTER SS BY ss_sold_date_sk >= 2450819 AND ss_sold_date_sk <= 2451904;

J1 = JOIN FSS BY ss_sold_date_sk, FD BY d_date_sk;
J2 = JOIN J1 BY ss_store_sk, FS BY s_store_sk;
J3 = JOIN J2 BY ss_hdemo_sk, FHD BY hd_demo_sk;

G1 = GROUP J3 BY (ss_ticket_number, ss_customer_sk, ss_addr_sk, s_city);

F1 = FOREACH G1 GENERATE
	group.ss_ticket_number,
    group.ss_customer_sk,
    group.s_city,
    SUM(J3.ss_coupon_amt) AS amt,
    SUM(J3.ss_net_profit) AS profit;

J1F = JOIN F1 BY ss_customer_sk, C BY c_customer_sk;

FFJ1F = FOREACH J1F GENERATE
	c_last_name,
    c_first_name,
    s_city,
    ss_ticket_number,
    amt,
    profit;

O1 = ORDER FFJ1F BY
	c_last_name,
    c_first_name,
    s_city,
    profit;

L1 = LIMIT O1 100;

STORE L1 INTO '$output_path/Q79' USING PigStorage('|');

