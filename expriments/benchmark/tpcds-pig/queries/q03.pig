IMPORT 'tablesPlaintext.pig';
%default output_path '/tpcds/output'
RMF $output_path/Q03

C = customer();
CA = customer_address();
CD = customer_demographics();
D = date_dim();
HD = household_demographics();
I = item();
P = promotion();
S = stores();
SS = store_sales();

FI = FILTER I BY i_manufact_id == 436;
FD = FILTER D BY d_moy == 12;
FSS = FILTER SS BY (
	(ss_sold_date_sk >= 2415355 AND ss_sold_date_sk <= 2415385) OR
	(ss_sold_date_sk >= 2415720 AND ss_sold_date_sk <= 2415750) OR
	(ss_sold_date_sk >= 2416085 AND ss_sold_date_sk <= 2416115) OR
	(ss_sold_date_sk >= 2416450 AND ss_sold_date_sk <= 2416480) OR
	(ss_sold_date_sk >= 2416816 AND ss_sold_date_sk <= 2416846) OR
	(ss_sold_date_sk >= 2417181 AND ss_sold_date_sk <= 2417211) OR
	(ss_sold_date_sk >= 2417546 AND ss_sold_date_sk <= 2417576) OR
	(ss_sold_date_sk >= 2417911 AND ss_sold_date_sk <= 2417941) OR
	(ss_sold_date_sk >= 2418277 AND ss_sold_date_sk <= 2418307) OR
	(ss_sold_date_sk >= 2418642 AND ss_sold_date_sk <= 2418672) OR
	(ss_sold_date_sk >= 2419007 AND ss_sold_date_sk <= 2419037) OR
	(ss_sold_date_sk >= 2419372 AND ss_sold_date_sk <= 2419402) OR
	(ss_sold_date_sk >= 2419738 AND ss_sold_date_sk <= 2419768) OR
	(ss_sold_date_sk >= 2420103 AND ss_sold_date_sk <= 2420133) OR
	(ss_sold_date_sk >= 2420468 AND ss_sold_date_sk <= 2420498) OR
	(ss_sold_date_sk >= 2420833 AND ss_sold_date_sk <= 2420863) OR
	(ss_sold_date_sk >= 2421199 AND ss_sold_date_sk <= 2421229) OR
	(ss_sold_date_sk >= 2421564 AND ss_sold_date_sk <= 2421594) OR
	(ss_sold_date_sk >= 2421929 AND ss_sold_date_sk <= 2421959) OR
	(ss_sold_date_sk >= 2422294 AND ss_sold_date_sk <= 2422324) OR
	(ss_sold_date_sk >= 2422660 AND ss_sold_date_sk <= 2422690) OR
	(ss_sold_date_sk >= 2423025 AND ss_sold_date_sk <= 2423055) OR
	(ss_sold_date_sk >= 2423390 AND ss_sold_date_sk <= 2423420) OR
	(ss_sold_date_sk >= 2423755 AND ss_sold_date_sk <= 2423785) OR
	(ss_sold_date_sk >= 2424121 AND ss_sold_date_sk <= 2424151) OR
	(ss_sold_date_sk >= 2424486 AND ss_sold_date_sk <= 2424516) OR
	(ss_sold_date_sk >= 2424851 AND ss_sold_date_sk <= 2424881) OR
	(ss_sold_date_sk >= 2425216 AND ss_sold_date_sk <= 2425246) OR
	(ss_sold_date_sk >= 2425582 AND ss_sold_date_sk <= 2425612) OR
	(ss_sold_date_sk >= 2425947 AND ss_sold_date_sk <= 2425977) OR
	(ss_sold_date_sk >= 2426312 AND ss_sold_date_sk <= 2426342) OR
	(ss_sold_date_sk >= 2426677 AND ss_sold_date_sk <= 2426707) OR
	(ss_sold_date_sk >= 2427043 AND ss_sold_date_sk <= 2427073) OR
	(ss_sold_date_sk >= 2427408 AND ss_sold_date_sk <= 2427438) OR
	(ss_sold_date_sk >= 2427773 AND ss_sold_date_sk <= 2427803) OR
	(ss_sold_date_sk >= 2428138 AND ss_sold_date_sk <= 2428168) OR
	(ss_sold_date_sk >= 2428504 AND ss_sold_date_sk <= 2428534) OR
	(ss_sold_date_sk >= 2428869 AND ss_sold_date_sk <= 2428899) OR
	(ss_sold_date_sk >= 2429234 AND ss_sold_date_sk <= 2429264) OR
	(ss_sold_date_sk >= 2429599 AND ss_sold_date_sk <= 2429629) OR
	(ss_sold_date_sk >= 2429965 AND ss_sold_date_sk <= 2429995) OR
	(ss_sold_date_sk >= 2430330 AND ss_sold_date_sk <= 2430360) OR
	(ss_sold_date_sk >= 2430695 AND ss_sold_date_sk <= 2430725) OR
	(ss_sold_date_sk >= 2431060 AND ss_sold_date_sk <= 2431090) OR
	(ss_sold_date_sk >= 2431426 AND ss_sold_date_sk <= 2431456) OR
	(ss_sold_date_sk >= 2431791 AND ss_sold_date_sk <= 2431821) OR
	(ss_sold_date_sk >= 2432156 AND ss_sold_date_sk <= 2432186) OR
	(ss_sold_date_sk >= 2432521 AND ss_sold_date_sk <= 2432551) OR
	(ss_sold_date_sk >= 2432887 AND ss_sold_date_sk <= 2432917) OR
	(ss_sold_date_sk >= 2433252 AND ss_sold_date_sk <= 2433282) OR
	(ss_sold_date_sk >= 2433617 AND ss_sold_date_sk <= 2433647) OR
	(ss_sold_date_sk >= 2433982 AND ss_sold_date_sk <= 2434012) OR
	(ss_sold_date_sk >= 2434348 AND ss_sold_date_sk <= 2434378) OR
	(ss_sold_date_sk >= 2434713 AND ss_sold_date_sk <= 2434743) OR
	(ss_sold_date_sk >= 2435078 AND ss_sold_date_sk <= 2435108) OR
	(ss_sold_date_sk >= 2435443 AND ss_sold_date_sk <= 2435473) OR
	(ss_sold_date_sk >= 2435809 AND ss_sold_date_sk <= 2435839) OR
	(ss_sold_date_sk >= 2436174 AND ss_sold_date_sk <= 2436204) OR
	(ss_sold_date_sk >= 2436539 AND ss_sold_date_sk <= 2436569) OR
	(ss_sold_date_sk >= 2436904 AND ss_sold_date_sk <= 2436934) OR
	(ss_sold_date_sk >= 2437270 AND ss_sold_date_sk <= 2437300) OR
	(ss_sold_date_sk >= 2437635 AND ss_sold_date_sk <= 2437665) OR
	(ss_sold_date_sk >= 2438000 AND ss_sold_date_sk <= 2438030) OR
	(ss_sold_date_sk >= 2438365 AND ss_sold_date_sk <= 2438395) OR
	(ss_sold_date_sk >= 2438731 AND ss_sold_date_sk <= 2438761) OR
	(ss_sold_date_sk >= 2439096 AND ss_sold_date_sk <= 2439126) OR
	(ss_sold_date_sk >= 2439461 AND ss_sold_date_sk <= 2439491) OR
	(ss_sold_date_sk >= 2439826 AND ss_sold_date_sk <= 2439856) OR
	(ss_sold_date_sk >= 2440192 AND ss_sold_date_sk <= 2440222) OR
	(ss_sold_date_sk >= 2440557 AND ss_sold_date_sk <= 2440587) OR
	(ss_sold_date_sk >= 2440922 AND ss_sold_date_sk <= 2440952) OR
	(ss_sold_date_sk >= 2441287 AND ss_sold_date_sk <= 2441317) OR
	(ss_sold_date_sk >= 2441653 AND ss_sold_date_sk <= 2441683) OR
	(ss_sold_date_sk >= 2442018 AND ss_sold_date_sk <= 2442048) OR
	(ss_sold_date_sk >= 2442383 AND ss_sold_date_sk <= 2442413) OR
	(ss_sold_date_sk >= 2442748 AND ss_sold_date_sk <= 2442778) OR
	(ss_sold_date_sk >= 2443114 AND ss_sold_date_sk <= 2443144) OR
	(ss_sold_date_sk >= 2443479 AND ss_sold_date_sk <= 2443509) OR
	(ss_sold_date_sk >= 2443844 AND ss_sold_date_sk <= 2443874) OR
	(ss_sold_date_sk >= 2444209 AND ss_sold_date_sk <= 2444239) OR
	(ss_sold_date_sk >= 2444575 AND ss_sold_date_sk <= 2444605) OR
	(ss_sold_date_sk >= 2444940 AND ss_sold_date_sk <= 2444970) OR
	(ss_sold_date_sk >= 2445305 AND ss_sold_date_sk <= 2445335) OR
	(ss_sold_date_sk >= 2445670 AND ss_sold_date_sk <= 2445700) OR
	(ss_sold_date_sk >= 2446036 AND ss_sold_date_sk <= 2446066) OR
	(ss_sold_date_sk >= 2446401 AND ss_sold_date_sk <= 2446431) OR
	(ss_sold_date_sk >= 2446766 AND ss_sold_date_sk <= 2446796) OR
	(ss_sold_date_sk >= 2447131 AND ss_sold_date_sk <= 2447161) OR
	(ss_sold_date_sk >= 2447497 AND ss_sold_date_sk <= 2447527) OR
	(ss_sold_date_sk >= 2447862 AND ss_sold_date_sk <= 2447892) OR
	(ss_sold_date_sk >= 2448227 AND ss_sold_date_sk <= 2448257) OR
	(ss_sold_date_sk >= 2448592 AND ss_sold_date_sk <= 2448622) OR
	(ss_sold_date_sk >= 2448958 AND ss_sold_date_sk <= 2448988) OR
	(ss_sold_date_sk >= 2449323 AND ss_sold_date_sk <= 2449353) OR
	(ss_sold_date_sk >= 2449688 AND ss_sold_date_sk <= 2449718) OR
	(ss_sold_date_sk >= 2450053 AND ss_sold_date_sk <= 2450083) OR
	(ss_sold_date_sk >= 2450419 AND ss_sold_date_sk <= 2450449) OR
	(ss_sold_date_sk >= 2450784 AND ss_sold_date_sk <= 2450814) OR
	(ss_sold_date_sk >= 2451149 AND ss_sold_date_sk <= 2451179) OR
	(ss_sold_date_sk >= 2451514 AND ss_sold_date_sk <= 2451544) OR
	(ss_sold_date_sk >= 2451880 AND ss_sold_date_sk <= 2451910) OR
	(ss_sold_date_sk >= 2452245 AND ss_sold_date_sk <= 2452275) OR
	(ss_sold_date_sk >= 2452610 AND ss_sold_date_sk <= 2452640) OR
	(ss_sold_date_sk >= 2452975 AND ss_sold_date_sk <= 2453005) OR
	(ss_sold_date_sk >= 2453341 AND ss_sold_date_sk <= 2453371) OR
	(ss_sold_date_sk >= 2453706 AND ss_sold_date_sk <= 2453736) OR
	(ss_sold_date_sk >= 2454071 AND ss_sold_date_sk <= 2454101) OR
	(ss_sold_date_sk >= 2454436 AND ss_sold_date_sk <= 2454466) OR
	(ss_sold_date_sk >= 2454802 AND ss_sold_date_sk <= 2454832) OR
	(ss_sold_date_sk >= 2455167 AND ss_sold_date_sk <= 2455197) OR
	(ss_sold_date_sk >= 2455532 AND ss_sold_date_sk <= 2455562) OR
	(ss_sold_date_sk >= 2455897 AND ss_sold_date_sk <= 2455927) OR
	(ss_sold_date_sk >= 2456263 AND ss_sold_date_sk <= 2456293) OR
	(ss_sold_date_sk >= 2456628 AND ss_sold_date_sk <= 2456658) OR
	(ss_sold_date_sk >= 2456993 AND ss_sold_date_sk <= 2457023) OR
	(ss_sold_date_sk >= 2457358 AND ss_sold_date_sk <= 2457388) OR
	(ss_sold_date_sk >= 2457724 AND ss_sold_date_sk <= 2457754) OR
	(ss_sold_date_sk >= 2458089 AND ss_sold_date_sk <= 2458119) OR
	(ss_sold_date_sk >= 2458454 AND ss_sold_date_sk <= 2458484) OR
	(ss_sold_date_sk >= 2458819 AND ss_sold_date_sk <= 2458849) OR
	(ss_sold_date_sk >= 2459185 AND ss_sold_date_sk <= 2459215) OR
	(ss_sold_date_sk >= 2459550 AND ss_sold_date_sk <= 2459580) OR
	(ss_sold_date_sk >= 2459915 AND ss_sold_date_sk <= 2459945) OR
	(ss_sold_date_sk >= 2460280 AND ss_sold_date_sk <= 2460310) OR
	(ss_sold_date_sk >= 2460646 AND ss_sold_date_sk <= 2460676) OR
	(ss_sold_date_sk >= 2461011 AND ss_sold_date_sk <= 2461041) OR
	(ss_sold_date_sk >= 2461376 AND ss_sold_date_sk <= 2461406) OR
	(ss_sold_date_sk >= 2461741 AND ss_sold_date_sk <= 2461771) OR
	(ss_sold_date_sk >= 2462107 AND ss_sold_date_sk <= 2462137) OR
	(ss_sold_date_sk >= 2462472 AND ss_sold_date_sk <= 2462502) OR
	(ss_sold_date_sk >= 2462837 AND ss_sold_date_sk <= 2462867) OR
	(ss_sold_date_sk >= 2463202 AND ss_sold_date_sk <= 2463232) OR
	(ss_sold_date_sk >= 2463568 AND ss_sold_date_sk <= 2463598) OR
	(ss_sold_date_sk >= 2463933 AND ss_sold_date_sk <= 2463963) OR
	(ss_sold_date_sk >= 2464298 AND ss_sold_date_sk <= 2464328) OR
	(ss_sold_date_sk >= 2464663 AND ss_sold_date_sk <= 2464693) OR
	(ss_sold_date_sk >= 2465029 AND ss_sold_date_sk <= 2465059) OR
	(ss_sold_date_sk >= 2465394 AND ss_sold_date_sk <= 2465424) OR
	(ss_sold_date_sk >= 2465759 AND ss_sold_date_sk <= 2465789) OR
	(ss_sold_date_sk >= 2466124 AND ss_sold_date_sk <= 2466154) OR
	(ss_sold_date_sk >= 2466490 AND ss_sold_date_sk <= 2466520) OR
	(ss_sold_date_sk >= 2466855 AND ss_sold_date_sk <= 2466885) OR
	(ss_sold_date_sk >= 2467220 AND ss_sold_date_sk <= 2467250) OR
	(ss_sold_date_sk >= 2467585 AND ss_sold_date_sk <= 2467615) OR
	(ss_sold_date_sk >= 2467951 AND ss_sold_date_sk <= 2467981) OR
	(ss_sold_date_sk >= 2468316 AND ss_sold_date_sk <= 2468346) OR
	(ss_sold_date_sk >= 2468681 AND ss_sold_date_sk <= 2468711) OR
	(ss_sold_date_sk >= 2469046 AND ss_sold_date_sk <= 2469076) OR
	(ss_sold_date_sk >= 2469412 AND ss_sold_date_sk <= 2469442) OR
	(ss_sold_date_sk >= 2469777 AND ss_sold_date_sk <= 2469807) OR
	(ss_sold_date_sk >= 2470142 AND ss_sold_date_sk <= 2470172) OR
	(ss_sold_date_sk >= 2470507 AND ss_sold_date_sk <= 2470537) OR
	(ss_sold_date_sk >= 2470873 AND ss_sold_date_sk <= 2470903) OR
	(ss_sold_date_sk >= 2471238 AND ss_sold_date_sk <= 2471268) OR
	(ss_sold_date_sk >= 2471603 AND ss_sold_date_sk <= 2471633) OR
	(ss_sold_date_sk >= 2471968 AND ss_sold_date_sk <= 2471998) OR
	(ss_sold_date_sk >= 2472334 AND ss_sold_date_sk <= 2472364) OR
	(ss_sold_date_sk >= 2472699 AND ss_sold_date_sk <= 2472729) OR
	(ss_sold_date_sk >= 2473064 AND ss_sold_date_sk <= 2473094) OR
	(ss_sold_date_sk >= 2473429 AND ss_sold_date_sk <= 2473459) OR
	(ss_sold_date_sk >= 2473795 AND ss_sold_date_sk <= 2473825) OR
	(ss_sold_date_sk >= 2474160 AND ss_sold_date_sk <= 2474190) OR
	(ss_sold_date_sk >= 2474525 AND ss_sold_date_sk <= 2474555) OR
	(ss_sold_date_sk >= 2474890 AND ss_sold_date_sk <= 2474920) OR
	(ss_sold_date_sk >= 2475256 AND ss_sold_date_sk <= 2475286) OR
	(ss_sold_date_sk >= 2475621 AND ss_sold_date_sk <= 2475651) OR
	(ss_sold_date_sk >= 2475986 AND ss_sold_date_sk <= 2476016) OR
	(ss_sold_date_sk >= 2476351 AND ss_sold_date_sk <= 2476381) OR
	(ss_sold_date_sk >= 2476717 AND ss_sold_date_sk <= 2476747) OR
	(ss_sold_date_sk >= 2477082 AND ss_sold_date_sk <= 2477112) OR
	(ss_sold_date_sk >= 2477447 AND ss_sold_date_sk <= 2477477) OR
	(ss_sold_date_sk >= 2477812 AND ss_sold_date_sk <= 2477842) OR
	(ss_sold_date_sk >= 2478178 AND ss_sold_date_sk <= 2478208) OR
	(ss_sold_date_sk >= 2478543 AND ss_sold_date_sk <= 2478573) OR
	(ss_sold_date_sk >= 2478908 AND ss_sold_date_sk <= 2478938) OR
	(ss_sold_date_sk >= 2479273 AND ss_sold_date_sk <= 2479303) OR
	(ss_sold_date_sk >= 2479639 AND ss_sold_date_sk <= 2479669) OR
	(ss_sold_date_sk >= 2480004 AND ss_sold_date_sk <= 2480034) OR
	(ss_sold_date_sk >= 2480369 AND ss_sold_date_sk <= 2480399) OR
	(ss_sold_date_sk >= 2480734 AND ss_sold_date_sk <= 2480764) OR
	(ss_sold_date_sk >= 2481100 AND ss_sold_date_sk <= 2481130) OR
	(ss_sold_date_sk >= 2481465 AND ss_sold_date_sk <= 2481495) OR
	(ss_sold_date_sk >= 2481830 AND ss_sold_date_sk <= 2481860) OR
	(ss_sold_date_sk >= 2482195 AND ss_sold_date_sk <= 2482225) OR
	(ss_sold_date_sk >= 2482561 AND ss_sold_date_sk <= 2482591) OR
	(ss_sold_date_sk >= 2482926 AND ss_sold_date_sk <= 2482956) OR
	(ss_sold_date_sk >= 2483291 AND ss_sold_date_sk <= 2483321) OR
	(ss_sold_date_sk >= 2483656 AND ss_sold_date_sk <= 2483686) OR
	(ss_sold_date_sk >= 2484022 AND ss_sold_date_sk <= 2484052) OR
	(ss_sold_date_sk >= 2484387 AND ss_sold_date_sk <= 2484417) OR
	(ss_sold_date_sk >= 2484752 AND ss_sold_date_sk <= 2484782) OR
	(ss_sold_date_sk >= 2485117 AND ss_sold_date_sk <= 2485147) OR
	(ss_sold_date_sk >= 2485483 AND ss_sold_date_sk <= 2485513) OR
	(ss_sold_date_sk >= 2485848 AND ss_sold_date_sk <= 2485878) OR
	(ss_sold_date_sk >= 2486213 AND ss_sold_date_sk <= 2486243) OR
	(ss_sold_date_sk >= 2486578 AND ss_sold_date_sk <= 2486608) OR
	(ss_sold_date_sk >= 2486944 AND ss_sold_date_sk <= 2486974) OR
	(ss_sold_date_sk >= 2487309 AND ss_sold_date_sk <= 2487339) OR
	(ss_sold_date_sk >= 2487674 AND ss_sold_date_sk <= 2487704) OR
	(ss_sold_date_sk >= 2488039 AND ss_sold_date_sk <= 2488069)
);

J1 = JOIN FD BY d_date_sk, FSS BY ss_sold_date_sk;
J2 = JOIN J1 BY ss_item_sk, FI BY i_item_sk;

G1 = GROUP J2 BY (d_year, i_brand, i_brand_id);

F1 = FOREACH G1 GENERATE
	group.d_year,
	group.i_brand_id AS brand_id,
	group.i_brand,
	SUM(J2.ss_net_profit) AS sum_agg;

O1 = ORDER F1 BY d_year, sum_agg DESC, brand_id;

L1 = LIMIT O1 100;

STORE L1 INTO '$output_path/Q03' USING PigStorage('|');
