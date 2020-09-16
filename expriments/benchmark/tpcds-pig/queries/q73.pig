IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q73

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

FHD = FILTER HD BY
	(hd_buy_potential == '>10000' OR hd_buy_potential == 'Unknown') AND
	hd_vehicle_count > 0 AND hd_dep_count > hd_vehicle_count;
FD = FILTER D BY (d_dow == 1 OR d_dow == 2) AND
	(d_year == 1998 OR d_year == 1999 OR d_year == 2000);
FS = FILTER S BY s_county MATCHES 'Fairfield County|Ziebach County|Bronx County|Barrow County';
FSS = FILTER SS BY ((chararray) ss_sold_date_sk) MATCHES '2450815|2450816|2450846|2450847|2450874|2450875|2450905|2450906|2450935|2450936|2450966|2450967|2450996|2450997|2451027|2451028|2451058|2451059|2451088|2451089|2451119|2451120|2451149|2451150|2451180|2451181|2451211|2451212|2451239|2451240|2451270|2451271|2451300|2451301|2451331|2451332|2451361|2451362|2451392|2451393|2451423|2451424|2451453|2451454|2451484|2451485|2451514|2451515|2451545|2451546|2451576|2451577|2451605|2451606|2451636|2451637|2451666|2451667|2451697|2451698|2451727|2451728|2451758|2451759|2451789|2451790|2451819|2451820|2451850|2451851|2451880|2451881';

J1 = JOIN FSS BY ss_sold_date_sk, FD BY d_date_sk;
J2 = JOIN J1 BY ss_store_sk, FS BY s_store_sk;
J3 = JOIN J2 BY ss_hdemo_sk, FHD BY hd_demo_sk;

G1 = GROUP J3 BY (ss_ticket_number, ss_customer_sk);

F1 = FOREACH G1 GENERATE
	group.ss_ticket_number,
    group.ss_customer_sk,
    COUNT_STAR(J3) AS cnt;

J1F = JOIN F1 BY ss_customer_sk, C BY c_customer_sk;

FJ1F = FILTER J1F BY cnt >= 1 AND cnt <= 5;

FFJ1F = FOREACH FJ1F GENERATE
	c_last_name,
    c_first_name,
    c_salutation,
    c_preferred_cust_flag,
    ss_ticket_number,
    cnt;

O1 = ORDER FFJ1F BY cnt DESC;

STORE O1 INTO '$output_path/Q73' USING PigStorage('|');

