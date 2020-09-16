IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q68

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

FHD = FILTER HD BY hd_dep_count == 5 AND hd_vehicle_count == 3;
FD = FILTER D BY (d_dow == 1 OR d_dow == 2) AND
	(d_year == 1999 OR d_year == 2000 OR d_year == 2001);
FS = FILTER S BY s_city MATCHES 'Midway|Fairview';
FSS = FILTER SS BY ((chararray) ss_sold_date_sk) MATCHES '2451180|2451181|2451211|2451212|2451239|2451240|2451270|2451271|2451300|2451301|2451331|2451332|2451361|2451362|2451392|2451393|2451423|2451424|2451453|2451454|2451484|2451485|2451514|2451515|2451545|2451546|2451576|2451577|2451605|2451606|2451636|2451637|2451666|2451667|2451697|2451698|2451727|2451728|2451758|2451759|2451789|2451790|2451819|2451820|2451850|2451851|2451880|2451881|2451911|2451912|2451942|2451943|2451970|2451971|2452001|2452002|2452031|2452032|2452062|2452063|2452092|2452093|2452123|2452124|2452154|2452155|2452184|2452185|2452215|2452216|2452245|2452246';

J1 = JOIN FSS BY ss_sold_date_sk, FD BY d_date_sk;
J2 = JOIN J1 BY ss_store_sk, FS BY s_store_sk;
J3 = JOIN J2 BY ss_hdemo_sk, FHD BY hd_demo_sk;
J4 = JOIN J3 BY ss_store_sk, CA BY ca_address_sk;

G1 = GROUP J4 BY (ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city);

F1 = FOREACH G1 GENERATE
	group.ss_ticket_number,
    group.ss_customer_sk,
    group.ca_city AS bought_city,
    SUM(J4.ss_ext_sales_price) AS extended_price,
    SUM(J4.ss_ext_list_price) AS list_price,
    SUM(J4.ss_ext_tax) AS extended_tax;

J1F = JOIN F1 BY ss_customer_sk, C BY c_customer_sk;
J2F = JOIN J1F BY c_current_addr_sk, CA BY ca_address_sk;

FJ2F = FILTER J2F BY ca_city != bought_city;

FFJ2F = FOREACH FJ2F GENERATE
	c_last_name,
    c_first_name,
    ca_city,
    bought_city,
    ss_ticket_number,
    extended_price,
    extended_tax,
    list_price;

O1 = ORDER FFJ2F BY
	c_last_name,
    ss_ticket_number;

L1 = LIMIT O1 100;

STORE L1 INTO '$output_path/Q68' USING PigStorage('|');

