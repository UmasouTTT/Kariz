IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q34

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

FD = FILTER D BY
	((d_dom >= 1 AND d_dom <= 3) OR (d_dom >= 25 AND d_dom <= 28)) AND
	(d_year == 1998 OR d_year == 1999 OR d_year == 2000);
FHD = FILTER HD BY
	(hd_buy_potential == '>10000' OR hd_buy_potential == 'Unknown') AND
	hd_vehicle_count > 0 AND
	(hd_dep_count / hd_vehicle_count) * 10 > 12;
FS = FILTER S BY s_county MATCHES 'Saginaw County|Sumner County|Appanoose County|Daviess County|Fairfield County|Raleigh County|Ziebach County|Williamson County';
FSS = FILTER SS BY ss_sold_date_sk >= 2450816 AND ss_sold_date_sk <= 2451910;

J1 = JOIN FSS BY ss_sold_date_sk, FD BY d_date_sk;
J2 = JOIN J1 BY ss_store_sk, FS BY s_store_sk;
J3 = JOIN J2 BY ss_hdemo_sk, FHD BY hd_demo_sk;

G1 = GROUP J3 BY (ss_ticket_number, ss_customer_sk);

F1 = FOREACH G1 GENERATE
	group.ss_ticket_number,
	group.ss_customer_sk,
	COUNT_STAR(J3) AS cnt;

FF1 = FILTER F1 BY cnt >= 15 AND cnt <= 20;

JFF1 = JOIN FF1 BY ss_customer_sk, C BY c_customer_sk;

FJFF1 = FOREACH JFF1 GENERATE
	c_last_name,
	c_first_name,
	c_salutation,
	c_preferred_cust_flag,
	ss_ticket_number,
	cnt;

O1 = ORDER FJFF1 BY
	c_last_name,
	c_first_name,
	c_salutation,
	c_preferred_cust_flag desc;

STORE O1 INTO '$output_path/Q34' USING PigStorage('|');

