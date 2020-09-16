IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q98

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

-- d_date >= '2001-01-01' AND d_date <= '2001-01-31';
FD = FILTER D BY d_date >= '2001-01-01' AND d_date <= '2001-01-31';
FI = FILTER I BY i_category MATCHES 'Jewelry|Sports|Books';
FSS = FILTER SS BY ss_sold_date_sk >= 2451911 AND ss_sold_date_sk <= 2451941;

J1 = JOIN FSS BY ss_sold_date_sk, FD BY d_date_sk;
J2 = JOIN J1 BY ss_item_sk, FI BY i_item_sk;

G1 = GROUP J2 BY (i_item_id, i_item_desc, i_category, i_class, i_current_price);

F1 = FOREACH G1 GENERATE
	group.i_item_id,
	group.i_item_desc,
	group.i_category,
	group.i_class,
	group.i_current_price,
	SUM(J2.ss_ext_sales_price) AS itemrevenue;

-- windowing
GW = GROUP F1 BY i_class;
FW = FOREACH GW GENERATE
	group AS class,
	SUM(F1.itemrevenue) AS class_revenu;

JFW = JOIN FW BY class, F1 BY i_class;

FJFW = FOREACH JFW GENERATE
	i_item_id,
	i_item_desc,
	i_category,
	i_class,
	i_current_price,
	itemrevenue,
	itemrevenue * 100 / class_revenu AS revenueratio;

O1 = ORDER FJFW BY
	i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio;

FO1 = FOREACH O1 GENERATE
	i_item_desc,
	i_category,
	i_class,
	i_current_price,
	itemrevenue,
	revenueratio;

STORE FO1 INTO '$output_path/Q98' USING PigStorage('|');

