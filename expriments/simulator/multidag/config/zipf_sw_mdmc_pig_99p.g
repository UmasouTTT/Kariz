#t,100,24
v,0,COGROUP,ea0_customer:ea0_orders,21,0.55
v,1,GROUP_BY:COMBINER,,7,1.00
v,2,SAMPLER,,27,1.00
v,3,ORDER_BY,,65,1.00
e,0,1
e,1,2
e,2,3
#t,101,24
v,0,HASH_JOIN,ea0_nation:ea0_supplier,5,0.83
v,1,HASH_JOIN,ea0_partsupp,24,0.74
v,2,GROUP_BY:COMBINER,,68,1.00
v,3,GROUP_BY:COMBINER,,85,1.00
v,4,SAMPLER,,91,1.00
v,5,ORDER_BY,,7,1.00
e,0,1
e,1,2
e,1,3
e,2,3
e,3,4
e,4,5
#t,102,24
v,0,COGROUP,ea0_lineitem:ea0_orders,62,0.35
v,1,GROUP_BY:COMBINER,,87,1.00
v,2,SAMPLER,,96,1.00
v,3,ORDER_BY,,90,1.00
e,0,1
e,1,2
e,2,3
#t,103,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,45,0.65
v,1,HASH_JOIN,,92,1.00
v,2,DISTINCT,ea0_part,15,0.90
v,3,HASH_JOIN,ea0_partsupp,74,0.58
v,4,DISTINCT,,50,1.00
v,5,HASH_JOIN,,56,1.00
v,6,HASH_JOIN,ea0_nation:ea0_supplier,69,0.92
v,7,SAMPLER,,36,1.00
v,8,ORDER_BY,,9,1.00
e,0,1
e,1,4
e,2,3
e,3,1
e,4,5
e,5,7
e,6,5
e,7,8
#t,104,24
v,0,COGROUP,ea0_lineitem:ea0_part,19,0.35
v,1,GROUP_BY:COMBINER,,72,1.00
e,0,1
#t,105,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,70,0.97
v,1,GROUP_BY:COMBINER,,91,1.00
v,2,HASH_JOIN,ea0_supplier,82,0.56
v,3,SAMPLER,,39,1.00
v,4,ORDER_BY,,57,1.00
e,0,1
e,0,2
e,1,2
e,2,3
e,3,4
#t,106,24
v,0,HASH_JOIN,ea0_customer:ea0_orders,70,0.65
v,1,HASH_JOIN,ea0_nation,22,0.39
v,2,HASH_JOIN,ea0_lineitem,50,0.89
v,3,GROUP_BY:COMBINER,,46,1.00
v,4,SAMPLER,,6,1.00
v,5,ORDER_BY:COMBINER,,28,1.00
v,6,SAVE,,79,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
#t,107,24
v,0,MAP_ONLY,ea0_nation,57,0.45
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,63,0.48
v,2,HASH_JOIN,ea0_lineitem:ea0_part,58,0.31
v,3,HASH_JOIN,ea0_partsupp,22,0.90
v,4,HASH_JOIN,ea0_orders,16,0.99
v,5,GROUP_BY:COMBINER,,53,1.00
v,6,SAMPLER,,47,1.00
v,7,ORDER_BY,,49,1.00
e,0,1
e,1,3
e,2,1
e,3,4
e,4,5
e,5,6
e,6,7
#t,108,24
v,0,HASH_JOIN,ea0_partsupp:ea0_supplier,54,0.94
v,1,HASH_JOIN,ea0_part,68,0.34
v,2,GROUP_BY:COMBINER,,49,1.00
v,3,SAMPLER,,23,1.00
v,4,ORDER_BY,,55,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,109,24
v,0,COGROUP,ea0_lineitem:ea0_orders,27,0.90
v,1,GROUP_BY:COMBINER,,100,1.00
v,2,SAMPLER,,92,1.00
v,3,ORDER_BY,,7,1.00
e,0,1
e,1,2
e,2,3
#t,110,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,100,0.67
v,1,HASH_JOIN,,65,1.00
v,2,DISTINCT,ea0_part,24,0.51
v,3,HASH_JOIN,ea0_partsupp,79,0.63
v,4,DISTINCT,,13,1.00
v,5,HASH_JOIN,,90,1.00
v,6,HASH_JOIN,ea0_nation:ea0_supplier,44,0.96
v,7,SAMPLER,,79,1.00
v,8,ORDER_BY,,72,1.00
e,0,1
e,1,4
e,2,3
e,3,1
e,4,5
e,5,7
e,6,5
e,7,8
#t,111,24
v,0,HASH_JOIN,ea0_lineitem:ea0_part,82,0.98
v,1,GROUP_BY:COMBINER,,13,1.00
e,0,1
#t,112,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,68,0.51
v,1,SAMPLER,,94,1.00
v,2,ORDER_BY,,88,1.00
e,0,1
e,1,2
#t,113,24
v,0,HASH_JOIN,ea0_orders:ea0_customer,5,0.65
v,1,HASH_JOIN,ea0_lineitem,75,0.90
v,2,GROUP_BY:COMBINER,,16,1.00
v,3,SAMPLER,,62,1.00
v,4,ORDER_BY:COMBINER,,10,1.00
v,5,SAVE,,50,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
#t,114,24
v,0,HASH_JOIN,ea0_customer:ea0_orders,85,0.77
v,1,HASH_JOIN,ea0_nation,77,0.35
v,2,HASH_JOIN,ea0_lineitem,38,0.51
v,3,GROUP_BY:COMBINER,,18,1.00
v,4,SAMPLER,,48,1.00
v,5,ORDER_BY:COMBINER,,86,1.00
v,6,SAVE,,54,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
#t,115,24
v,0,MULTI_QUERY:COMBINER,ea0_customer,92,0.88
v,1,HASH_JOIN,ea0_orders,38,0.84
v,2,GROUP_BY:COMBINER,,40,1.00
v,3,SAMPLER,,8,1.00
v,4,ORDER_BY,,16,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,116,24
v,0,HASH_JOIN,ea0_nation:ea0_supplier,20,0.97
v,1,HASH_JOIN,ea0_partsupp,46,0.84
v,2,GROUP_BY:COMBINER,,76,1.00
v,3,GROUP_BY:COMBINER,,87,1.00
v,4,SAMPLER,,68,1.00
v,5,ORDER_BY,,7,1.00
e,0,1
e,1,2
e,1,3
e,2,3
e,3,4
e,4,5
#t,117,24
v,0,HASH_JOIN,ea0_lineitem,50,0.49
#t,118,24
v,0,HASH_JOIN,ea0_region:ea0_nation,37,0.31
v,1,HASH_JOIN,ea0_supplier,35,0.48
v,2,HASH_JOIN,ea0_partsupp,76,0.44
v,3,HASH_JOIN,ea0_part,67,0.50
v,4,GROUP_BY,,25,1.00
v,5,SAMPLER,,41,1.00
v,6,ORDER_BY:COMBINER,,86,1.00
v,7,SAVE,,97,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
e,6,7
#t,119,24
v,0,MULTI_QUERY:COMBINER,ea0_customer,6,0.43
v,1,HASH_JOIN,ea0_orders,61,0.35
v,2,GROUP_BY:COMBINER,,18,1.00
v,3,SAMPLER,,18,1.00
v,4,ORDER_BY,,23,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,120,24
v,0,MULTI_QUERY:COMBINER,ea0_customer,52,0.97
v,1,HASH_JOIN,ea0_orders,52,0.49
v,2,GROUP_BY:COMBINER,,18,1.00
v,3,SAMPLER,,58,1.00
v,4,ORDER_BY,,96,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,121,24
v,0,HASH_JOIN,ea0_nation:ea0_supplier,92,0.62
v,1,HASH_JOIN,ea0_partsupp,53,0.80
v,2,GROUP_BY:COMBINER,,18,1.00
v,3,GROUP_BY:COMBINER,,46,1.00
v,4,SAMPLER,,60,1.00
v,5,ORDER_BY,,75,1.00
e,0,1
e,1,2
e,1,3
e,2,3
e,3,4
e,4,5
#t,122,24
v,0,MAP_ONLY,ea0_nation,68,0.35
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier:ea0_lineitem,45,0.64
v,2,HASH_JOIN,,74,1.00
v,3,MAP_ONLY,ea0_nation,11,0.47
v,4,REPLICATED_JOIN:HASH_JOIN,ea0_orders:ea0_customer,91,0.37
v,5,GROUP_BY:COMBINER,,41,1.00
v,6,SAMPLER,,70,1.00
v,7,ORDER_BY,,59,1.00
e,0,1
e,1,2
e,2,5
e,3,4
e,4,2
e,5,6
e,6,7
#t,123,24
v,0,GROUP_BY:MULTI_QUERY,ea0_lineitem,68,0.31
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,71,0.54
v,2,MAP_ONLY,ea0_nation,90,0.56
v,3,HASH_JOIN,ea0_orders,52,0.31
v,4,GROUP_BY:COMBINER,,44,1.00
v,5,SAMPLER,,29,1.00
v,6,ORDER_BY:COMBINER,,23,1.00
v,7,SAVE,,62,1.00
e,0,1
e,1,3
e,2,1
e,3,4
e,4,5
e,5,6
e,6,7
#t,124,24
v,0,HASH_JOIN,ea0_nation:ea0_region,50,0.86
v,1,HASH_JOIN,ea0_supplier,83,0.76
v,2,HASH_JOIN,ea0_lineitem,7,0.94
v,3,HASH_JOIN,ea0_orders,44,0.73
v,4,HASH_JOIN,ea0_customer,77,0.53
v,5,GROUP_BY:COMBINER,,29,1.00
v,6,SAMPLER,,20,1.00
v,7,ORDER_BY,,93,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
e,6,7
#t,125,24
v,0,COGROUP,ea0_lineitem:ea0_part,44,0.62
v,1,GROUP_BY:COMBINER,,16,1.00
e,0,1
#t,126,24
v,0,HASH_JOIN,ea0_part:ea0_lineitem,51,0.99
v,1,MULTI_QUERY:COMBINER,,92,1.00
v,2,MAP_ONLY,,15,1.00
e,0,1
e,1,2
#t,127,24
v,0,HASH_JOIN,ea0_orders:ea0_lineitem,40,0.95
v,1,GROUP_BY,,52,1.00
v,2,SAMPLER,,31,1.00
v,3,ORDER_BY,,75,1.00
e,0,1
e,1,2
e,2,3
#t,128,24
v,0,COGROUP,ea0_lineitem:ea0_orders,13,0.66
v,1,GROUP_BY:COMBINER,,13,1.00
v,2,SAMPLER,,43,1.00
v,3,ORDER_BY,,37,1.00
e,0,1
e,1,2
e,2,3
#t,129,24
v,0,MAP_ONLY,ea0_nation,92,0.75
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,68,0.31
v,2,HASH_JOIN,ea0_lineitem:ea0_part,20,0.97
v,3,HASH_JOIN,ea0_partsupp,15,0.48
v,4,HASH_JOIN,ea0_orders,80,1.00
v,5,GROUP_BY:COMBINER,,61,1.00
v,6,SAMPLER,,77,1.00
v,7,ORDER_BY,,83,1.00
e,0,1
e,1,3
e,2,1
e,3,4
e,4,5
e,5,6
e,6,7
#t,130,24
v,0,HASH_JOIN,ea0_customer:ea0_orders,25,0.79
v,1,HASH_JOIN,ea0_nation,75,0.58
v,2,HASH_JOIN,ea0_lineitem,82,0.42
v,3,GROUP_BY:COMBINER,,14,1.00
v,4,SAMPLER,,60,1.00
v,5,ORDER_BY:COMBINER,,5,1.00
v,6,SAVE,,92,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
#t,131,24
v,0,COGROUP,ea0_lineitem:ea0_orders,57,0.66
v,1,HASH_JOIN,ea0_customer,14,0.70
v,2,GROUP_BY:COMBINER,,73,1.00
v,3,SAMPLER,,87,1.00
v,4,ORDER_BY,,83,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,132,24
v,0,HASH_JOIN,ea0_orders:ea0_customer,84,0.66
v,1,HASH_JOIN,ea0_lineitem,42,0.66
v,2,GROUP_BY:COMBINER,,12,1.00
v,3,SAMPLER,,87,1.00
v,4,ORDER_BY:COMBINER,,30,1.00
v,5,SAVE,,36,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
#t,133,24
v,0,HASH_JOIN,ea0_nation:ea0_supplier,54,0.49
v,1,HASH_JOIN,ea0_partsupp,50,0.82
v,2,GROUP_BY:COMBINER,,87,1.00
v,3,GROUP_BY:COMBINER,,20,1.00
v,4,SAMPLER,,67,1.00
v,5,ORDER_BY,,57,1.00
e,0,1
e,1,2
e,1,3
e,2,3
e,3,4
e,4,5
#t,134,24
v,0,GROUP_BY:MULTI_QUERY,ea0_lineitem,6,0.92
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,78,1.00
v,2,MAP_ONLY,ea0_nation,56,0.81
v,3,HASH_JOIN,ea0_orders,89,0.51
v,4,GROUP_BY:COMBINER,,71,1.00
v,5,SAMPLER,,70,1.00
v,6,ORDER_BY:COMBINER,,36,1.00
v,7,SAVE,,94,1.00
e,0,1
e,1,3
e,2,1
e,3,4
e,4,5
e,5,6
e,6,7
#t,135,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,52,0.83
v,1,SAMPLER,,96,1.00
v,2,ORDER_BY,,73,1.00
e,0,1
e,1,2
#t,136,24
v,0,COGROUP,ea0_lineitem:ea0_part,53,0.44
v,1,GROUP_BY:COMBINER,,6,1.00
e,0,1
#t,137,24
v,0,COGROUP,ea0_lineitem:ea0_orders,57,0.92
v,1,GROUP_BY:COMBINER,,89,1.00
v,2,SAMPLER,,100,1.00
v,3,ORDER_BY,,88,1.00
e,0,1
e,1,2
e,2,3
#t,138,24
v,0,HASH_JOIN,ea0_lineitem,23,0.56
#t,139,24
v,0,HASH_JOIN,ea0_customer:ea0_orders,40,0.92
v,1,HASH_JOIN,ea0_nation,95,0.93
v,2,HASH_JOIN,ea0_lineitem,8,0.69
v,3,GROUP_BY:COMBINER,,83,1.00
v,4,SAMPLER,,48,1.00
v,5,ORDER_BY:COMBINER,,46,1.00
v,6,SAVE,,38,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
#t,140,24
v,0,COGROUP,ea0_lineitem:ea0_orders,37,0.36
v,1,HASH_JOIN,ea0_customer,34,0.35
v,2,GROUP_BY:COMBINER,,85,1.00
v,3,SAMPLER,,57,1.00
v,4,ORDER_BY,,18,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,141,24
v,0,HASH_JOIN,ea0_orders:ea0_customer,18,0.63
v,1,HASH_JOIN,ea0_lineitem,87,0.43
v,2,GROUP_BY:COMBINER,,81,1.00
v,3,SAMPLER,,14,1.00
v,4,ORDER_BY:COMBINER,,86,1.00
v,5,SAVE,,28,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
#t,142,24
v,0,HASH_JOIN,ea0_partsupp:ea0_supplier,98,0.41
v,1,HASH_JOIN,ea0_part,21,0.41
v,2,GROUP_BY:COMBINER,,93,1.00
v,3,SAMPLER,,98,1.00
v,4,ORDER_BY,,35,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,143,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,95,0.88
v,1,HASH_JOIN,,41,1.00
v,2,DISTINCT,ea0_part,37,0.32
v,3,HASH_JOIN,ea0_partsupp,61,0.36
v,4,DISTINCT,,5,1.00
v,5,HASH_JOIN,,83,1.00
v,6,HASH_JOIN,ea0_nation:ea0_supplier,77,0.91
v,7,SAMPLER,,18,1.00
v,8,ORDER_BY,,96,1.00
e,0,1
e,1,4
e,2,3
e,3,1
e,4,5
e,5,7
e,6,5
e,7,8
#t,144,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,77,0.80
v,1,GROUP_BY:COMBINER,,56,1.00
v,2,HASH_JOIN,ea0_supplier,41,0.80
v,3,SAMPLER,,63,1.00
v,4,ORDER_BY,,82,1.00
e,0,1
e,0,2
e,1,2
e,2,3
e,3,4
#t,145,24
v,0,HASH_JOIN,ea0_region:ea0_nation,19,0.92
v,1,HASH_JOIN,ea0_supplier,10,0.66
v,2,HASH_JOIN,ea0_partsupp,33,0.81
v,3,HASH_JOIN,ea0_part,27,0.98
v,4,GROUP_BY,,80,1.00
v,5,SAMPLER,,35,1.00
v,6,ORDER_BY:COMBINER,,39,1.00
v,7,SAVE,,66,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
e,6,7
#t,146,24
v,0,GROUP_BY:MULTI_QUERY,ea0_lineitem,21,0.90
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,71,0.97
v,2,MAP_ONLY,ea0_nation,27,0.73
v,3,HASH_JOIN,ea0_orders,86,0.50
v,4,GROUP_BY:COMBINER,,8,1.00
v,5,SAMPLER,,24,1.00
v,6,ORDER_BY:COMBINER,,90,1.00
v,7,SAVE,,77,1.00
e,0,1
e,1,3
e,2,1
e,3,4
e,4,5
e,5,6
e,6,7
#t,147,24
v,0,HASH_JOIN,ea1_nation:ea1_region,80,0.76
v,1,HASH_JOIN,ea1_supplier,66,0.34
v,2,HASH_JOIN,ea1_lineitem,33,0.52
v,3,HASH_JOIN,ea1_orders,81,0.56
v,4,HASH_JOIN,ea1_customer,61,0.77
v,5,GROUP_BY:COMBINER,,42,1.00
v,6,SAMPLER,,62,1.00
v,7,ORDER_BY,,20,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
e,6,7
#t,148,24
v,0,HASH_JOIN,ea0_orders:ea0_lineitem,20,0.46
v,1,GROUP_BY,,48,1.00
v,2,SAMPLER,,24,1.00
v,3,ORDER_BY,,35,1.00
e,0,1
e,1,2
e,2,3
#t,149,24
v,0,COGROUP,ea0_lineitem:ea0_orders,15,1.00
v,1,GROUP_BY:COMBINER,,73,1.00
v,2,SAMPLER,,82,1.00
v,3,ORDER_BY,,79,1.00
e,0,1
e,1,2
e,2,3
#t,150,24
v,0,MAP_ONLY,ea0_nation,8,0.41
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,36,0.48
v,2,HASH_JOIN,ea0_lineitem:ea0_part,13,0.75
v,3,HASH_JOIN,ea0_partsupp,32,0.34
v,4,HASH_JOIN,ea0_orders,57,1.00
v,5,GROUP_BY:COMBINER,,71,1.00
v,6,SAMPLER,,35,1.00
v,7,ORDER_BY,,69,1.00
e,0,1
e,1,3
e,2,1
e,3,4
e,4,5
e,5,6
e,6,7
#t,151,24
v,0,MULTI_QUERY:COMBINER,ea0_customer,45,0.80
v,1,HASH_JOIN,ea0_orders,17,0.58
v,2,GROUP_BY:COMBINER,,86,1.00
v,3,SAMPLER,,87,1.00
v,4,ORDER_BY,,51,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,152,24
v,0,HASH_JOIN,ea0_part:ea0_lineitem,60,0.73
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,45,0.66
v,2,MULTI_QUERY:MAP_ONLY,ea0_nation,77,0.56
v,3,REPLICATED_JOIN:MAP_ONLY,,7,1.00
v,4,HASH_JOIN,,19,1.00
v,5,MAP_ONLY,ea0_region,87,0.69
v,6,MAP_ONLY,,75,1.00
v,7,REPLICATED_JOIN:HASH_JOIN,ea0_customer:ea0_orders,72,0.98
v,8,GROUP_BY:COMBINER,,25,1.00
v,9,SAMPLER,,45,1.00
v,10,ORDER_BY,,44,1.00
e,0,1
e,1,4
e,2,1
e,2,3
e,3,6
e,4,8
e,5,3
e,6,7
e,7,4
e,8,9
e,9,10
#t,153,24
v,0,HASH_JOIN,ea0_lineitem,27,0.54
#t,154,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,77,0.85
v,1,HASH_JOIN,,39,1.00
v,2,DISTINCT,ea0_part,97,0.34
v,3,HASH_JOIN,ea0_partsupp,52,0.92
v,4,DISTINCT,,95,1.00
v,5,HASH_JOIN,,90,1.00
v,6,HASH_JOIN,ea0_nation:ea0_supplier,66,0.73
v,7,SAMPLER,,11,1.00
v,8,ORDER_BY,,18,1.00
e,0,1
e,1,4
e,2,3
e,3,1
e,4,5
e,5,7
e,6,5
e,7,8
#t,155,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,84,0.49
v,1,SAMPLER,,84,1.00
v,2,ORDER_BY,,37,1.00
e,0,1
e,1,2
#t,156,24
v,0,HASH_JOIN,ea0_nation:ea0_region,8,0.46
v,1,HASH_JOIN,ea0_supplier,78,0.83
v,2,HASH_JOIN,ea0_lineitem,68,0.36
v,3,HASH_JOIN,ea0_orders,45,0.87
v,4,HASH_JOIN,ea0_customer,80,0.30
v,5,GROUP_BY:COMBINER,,16,1.00
v,6,SAMPLER,,41,1.00
v,7,ORDER_BY,,51,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
e,6,7
#t,157,24
v,0,HASH_JOIN,ea0_customer:ea0_orders,86,0.53
v,1,HASH_JOIN,ea0_nation,50,0.67
v,2,HASH_JOIN,ea0_lineitem,66,0.77
v,3,GROUP_BY:COMBINER,,76,1.00
v,4,SAMPLER,,99,1.00
v,5,ORDER_BY:COMBINER,,49,1.00
v,6,SAVE,,55,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
#t,158,24
v,0,HASH_JOIN,ea0_nation:ea0_supplier,86,0.53
v,1,HASH_JOIN,ea0_partsupp,67,1.00
v,2,GROUP_BY:COMBINER,,47,1.00
v,3,GROUP_BY:COMBINER,,34,1.00
v,4,SAMPLER,,19,1.00
v,5,ORDER_BY,,33,1.00
e,0,1
e,1,2
e,1,3
e,2,3
e,3,4
e,4,5
#t,159,24
v,0,COGROUP,ea0_lineitem:ea0_orders,55,0.68
v,1,GROUP_BY:COMBINER,,91,1.00
v,2,SAMPLER,,56,1.00
v,3,ORDER_BY,,37,1.00
e,0,1
e,1,2
e,2,3
#t,160,24
v,0,COGROUP,ea0_lineitem:ea0_orders,82,0.34
v,1,HASH_JOIN,ea0_customer,9,0.47
v,2,GROUP_BY:COMBINER,,99,1.00
v,3,SAMPLER,,22,1.00
v,4,ORDER_BY,,68,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,161,24
v,0,MULTI_QUERY:COMBINER,ea0_customer,61,0.83
v,1,HASH_JOIN,ea0_orders,91,0.31
v,2,GROUP_BY:COMBINER,,54,1.00
v,3,SAMPLER,,71,1.00
v,4,ORDER_BY,,30,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,162,24
v,0,HASH_JOIN,ea0_part:ea0_lineitem,42,0.73
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,64,0.32
v,2,MULTI_QUERY:MAP_ONLY,ea0_nation,6,0.36
v,3,REPLICATED_JOIN:MAP_ONLY,,32,1.00
v,4,HASH_JOIN,,89,1.00
v,5,MAP_ONLY,ea0_region,60,0.82
v,6,MAP_ONLY,,66,1.00
v,7,REPLICATED_JOIN:HASH_JOIN,ea0_customer:ea0_orders,57,0.52
v,8,GROUP_BY:COMBINER,,55,1.00
v,9,SAMPLER,,51,1.00
v,10,ORDER_BY,,82,1.00
e,0,1
e,1,4
e,2,1
e,2,3
e,3,6
e,4,8
e,5,3
e,6,7
e,7,4
e,8,9
e,9,10
#t,163,24
v,0,HASH_JOIN,ea0_part:ea0_lineitem,9,0.75
v,1,MULTI_QUERY:COMBINER,,22,1.00
v,2,MAP_ONLY,,37,1.00
e,0,1
e,1,2
#t,164,24
v,0,HASH_JOIN,ea0_nation:ea0_supplier,60,0.57
v,1,HASH_JOIN,ea0_partsupp,94,0.48
v,2,GROUP_BY:COMBINER,,52,1.00
v,3,GROUP_BY:COMBINER,,60,1.00
v,4,SAMPLER,,83,1.00
v,5,ORDER_BY,,93,1.00
e,0,1
e,1,2
e,1,3
e,2,3
e,3,4
e,4,5
#t,165,24
v,0,HASH_JOIN,ea0_orders:ea0_lineitem,61,0.61
v,1,GROUP_BY,,65,1.00
v,2,SAMPLER,,61,1.00
v,3,ORDER_BY,,99,1.00
e,0,1
e,1,2
e,2,3
#t,166,24
v,0,MAP_ONLY,ea0_nation,62,0.58
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,60,0.48
v,2,HASH_JOIN,ea0_lineitem:ea0_part,46,0.72
v,3,HASH_JOIN,ea0_partsupp,88,0.94
v,4,HASH_JOIN,ea0_orders,5,0.61
v,5,GROUP_BY:COMBINER,,76,1.00
v,6,SAMPLER,,73,1.00
v,7,ORDER_BY,,43,1.00
e,0,1
e,1,3
e,2,1
e,3,4
e,4,5
e,5,6
e,6,7
#t,167,24
v,0,COGROUP,ea0_lineitem:ea0_orders,91,0.93
v,1,HASH_JOIN,ea0_customer,45,0.92
v,2,GROUP_BY:COMBINER,,6,1.00
v,3,SAMPLER,,88,1.00
v,4,ORDER_BY,,65,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,168,24
v,0,MAP_ONLY,ea0_nation,91,0.98
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier:ea0_lineitem,64,0.96
v,2,HASH_JOIN,,49,1.00
v,3,MAP_ONLY,ea0_nation,90,0.45
v,4,REPLICATED_JOIN:HASH_JOIN,ea0_orders:ea0_customer,38,0.37
v,5,GROUP_BY:COMBINER,,95,1.00
v,6,SAMPLER,,24,1.00
v,7,ORDER_BY,,45,1.00
e,0,1
e,1,2
e,2,5
e,3,4
e,4,2
e,5,6
e,6,7
#t,169,24
v,0,HASH_JOIN,ea0_part:ea0_lineitem,93,0.53
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,94,0.62
v,2,MULTI_QUERY:MAP_ONLY,ea0_nation,81,0.51
v,3,REPLICATED_JOIN:MAP_ONLY,,54,1.00
v,4,HASH_JOIN,,15,1.00
v,5,MAP_ONLY,ea0_region,38,0.34
v,6,MAP_ONLY,,5,1.00
v,7,REPLICATED_JOIN:HASH_JOIN,ea0_customer:ea0_orders,91,0.70
v,8,GROUP_BY:COMBINER,,26,1.00
v,9,SAMPLER,,7,1.00
v,10,ORDER_BY,,29,1.00
e,0,1
e,1,4
e,2,1
e,2,3
e,3,6
e,4,8
e,5,3
e,6,7
e,7,4
e,8,9
e,9,10
#t,170,24
v,0,HASH_JOIN,ea0_part:ea0_lineitem,39,0.74
v,1,MULTI_QUERY:COMBINER,,12,1.00
v,2,MAP_ONLY,,50,1.00
e,0,1
e,1,2
#t,171,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,27,0.66
v,1,GROUP_BY:COMBINER,,25,1.00
v,2,HASH_JOIN,ea0_supplier,96,0.97
v,3,SAMPLER,,72,1.00
v,4,ORDER_BY,,34,1.00
e,0,1
e,0,2
e,1,2
e,2,3
e,3,4
#t,172,24
v,0,HASH_JOIN,ea0_partsupp:ea0_supplier,23,0.60
v,1,HASH_JOIN,ea0_part,46,0.37
v,2,GROUP_BY:COMBINER,,47,1.00
v,3,SAMPLER,,33,1.00
v,4,ORDER_BY,,10,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,173,24
v,0,HASH_JOIN,ea0_nation:ea0_region,26,0.58
v,1,HASH_JOIN,ea0_supplier,48,0.55
v,2,HASH_JOIN,ea0_lineitem,54,0.43
v,3,HASH_JOIN,ea0_orders,85,0.49
v,4,HASH_JOIN,ea0_customer,91,0.92
v,5,GROUP_BY:COMBINER,,65,1.00
v,6,SAMPLER,,73,1.00
v,7,ORDER_BY,,52,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
e,6,7
#t,174,24
v,0,COGROUP,ea0_lineitem:ea0_orders,81,0.58
v,1,GROUP_BY:COMBINER,,20,1.00
v,2,SAMPLER,,18,1.00
v,3,ORDER_BY,,9,1.00
e,0,1
e,1,2
e,2,3
#t,175,24
v,0,HASH_JOIN,ea0_part:ea0_lineitem,60,0.69
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,83,0.53
v,2,MULTI_QUERY:MAP_ONLY,ea0_nation,31,0.75
v,3,REPLICATED_JOIN:MAP_ONLY,,27,1.00
v,4,HASH_JOIN,,44,1.00
v,5,MAP_ONLY,ea0_region,31,0.89
v,6,MAP_ONLY,,10,1.00
v,7,REPLICATED_JOIN:HASH_JOIN,ea0_customer:ea0_orders,100,0.47
v,8,GROUP_BY:COMBINER,,28,1.00
v,9,SAMPLER,,37,1.00
v,10,ORDER_BY,,25,1.00
e,0,1
e,1,4
e,2,1
e,2,3
e,3,6
e,4,8
e,5,3
e,6,7
e,7,4
e,8,9
e,9,10
#t,176,24
v,0,COGROUP,ea0_customer:ea0_orders,48,0.33
v,1,GROUP_BY:COMBINER,,34,1.00
v,2,SAMPLER,,52,1.00
v,3,ORDER_BY,,49,1.00
e,0,1
e,1,2
e,2,3
#t,177,24
v,0,HASH_JOIN,ea0_lineitem,35,0.54
#t,178,24
v,0,HASH_JOIN,ea0_part:ea0_lineitem,16,0.76
v,1,MULTI_QUERY:COMBINER,,76,1.00
v,2,MAP_ONLY,,5,1.00
e,0,1
e,1,2
#t,179,24
v,0,MULTI_QUERY:COMBINER,ea0_customer,19,0.43
v,1,HASH_JOIN,ea0_orders,20,0.58
v,2,GROUP_BY:COMBINER,,28,1.00
v,3,SAMPLER,,76,1.00
v,4,ORDER_BY,,100,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,180,24
v,0,HASH_JOIN,ea0_region:ea0_nation,55,0.89
v,1,HASH_JOIN,ea0_supplier,94,1.00
v,2,HASH_JOIN,ea0_partsupp,96,0.53
v,3,HASH_JOIN,ea0_part,81,0.99
v,4,GROUP_BY,,68,1.00
v,5,SAMPLER,,10,1.00
v,6,ORDER_BY:COMBINER,,27,1.00
v,7,SAVE,,75,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
e,6,7
#t,181,24
v,0,HASH_JOIN,ea0_orders:ea0_lineitem,14,0.37
v,1,GROUP_BY,,11,1.00
v,2,SAMPLER,,14,1.00
v,3,ORDER_BY,,16,1.00
e,0,1
e,1,2
e,2,3
#t,182,24
v,0,HASH_JOIN,ea0_orders:ea0_customer,44,0.96
v,1,HASH_JOIN,ea0_lineitem,97,0.80
v,2,GROUP_BY:COMBINER,,85,1.00
v,3,SAMPLER,,96,1.00
v,4,ORDER_BY:COMBINER,,28,1.00
v,5,SAVE,,89,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
#t,183,24
v,0,COGROUP,ea0_lineitem:ea0_orders,80,0.99
v,1,HASH_JOIN,ea0_customer,49,0.49
v,2,GROUP_BY:COMBINER,,78,1.00
v,3,SAMPLER,,99,1.00
v,4,ORDER_BY,,67,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,184,24
v,0,HASH_JOIN,ea0_part:ea0_lineitem,81,0.84
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,11,0.54
v,2,MULTI_QUERY:MAP_ONLY,ea0_nation,22,0.55
v,3,REPLICATED_JOIN:MAP_ONLY,,8,1.00
v,4,HASH_JOIN,,83,1.00
v,5,MAP_ONLY,ea0_region,97,0.76
v,6,MAP_ONLY,,35,1.00
v,7,REPLICATED_JOIN:HASH_JOIN,ea0_customer:ea0_orders,6,0.85
v,8,GROUP_BY:COMBINER,,92,1.00
v,9,SAMPLER,,61,1.00
v,10,ORDER_BY,,47,1.00
e,0,1
e,1,4
e,2,1
e,2,3
e,3,6
e,4,8
e,5,3
e,6,7
e,7,4
e,8,9
e,9,10
#t,185,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,61,0.84
v,1,SAMPLER,,83,1.00
v,2,ORDER_BY,,84,1.00
e,0,1
e,1,2
#t,186,24
v,0,COGROUP,ea0_customer:ea0_orders,61,0.85
v,1,GROUP_BY:COMBINER,,58,1.00
v,2,SAMPLER,,52,1.00
v,3,ORDER_BY,,82,1.00
e,0,1
e,1,2
e,2,3
#t,187,24
v,0,HASH_JOIN,ea0_customer:ea0_orders,84,0.99
v,1,HASH_JOIN,ea0_nation,33,0.59
v,2,HASH_JOIN,ea0_lineitem,12,0.82
v,3,GROUP_BY:COMBINER,,26,1.00
v,4,SAMPLER,,77,1.00
v,5,ORDER_BY:COMBINER,,91,1.00
v,6,SAVE,,75,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
#t,188,24
v,0,COGROUP,ea0_lineitem:ea0_orders,28,0.92
v,1,HASH_JOIN,ea0_customer,99,0.63
v,2,GROUP_BY:COMBINER,,76,1.00
v,3,SAMPLER,,75,1.00
v,4,ORDER_BY,,42,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,189,24
v,0,HASH_JOIN,ea0_nation:ea0_region,47,0.40
v,1,HASH_JOIN,ea0_supplier,30,0.68
v,2,HASH_JOIN,ea0_lineitem,43,0.79
v,3,HASH_JOIN,ea0_orders,88,0.98
v,4,HASH_JOIN,ea0_customer,13,0.94
v,5,GROUP_BY:COMBINER,,38,1.00
v,6,SAMPLER,,57,1.00
v,7,ORDER_BY,,69,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
e,6,7
#t,190,24
v,0,HASH_JOIN,ea0_part:ea0_lineitem,41,0.53
v,1,REPLICATED_JOIN:HASH_JOIN,ea0_supplier,81,0.73
v,2,MULTI_QUERY:MAP_ONLY,ea0_nation,43,0.81
v,3,REPLICATED_JOIN:MAP_ONLY,,31,1.00
v,4,HASH_JOIN,,65,1.00
v,5,MAP_ONLY,ea0_region,17,0.64
v,6,MAP_ONLY,,29,1.00
v,7,REPLICATED_JOIN:HASH_JOIN,ea0_customer:ea0_orders,15,0.32
v,8,GROUP_BY:COMBINER,,95,1.00
v,9,SAMPLER,,59,1.00
v,10,ORDER_BY,,12,1.00
e,0,1
e,1,4
e,2,1
e,2,3
e,3,6
e,4,8
e,5,3
e,6,7
e,7,4
e,8,9
e,9,10
#t,191,24
v,0,HASH_JOIN,ea0_nation:ea0_supplier,81,0.61
v,1,HASH_JOIN,ea0_partsupp,82,0.90
v,2,GROUP_BY:COMBINER,,90,1.00
v,3,GROUP_BY:COMBINER,,97,1.00
v,4,SAMPLER,,6,1.00
v,5,ORDER_BY,,78,1.00
e,0,1
e,1,2
e,1,3
e,2,3
e,3,4
e,4,5
#t,192,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,82,0.47
v,1,GROUP_BY:COMBINER,,18,1.00
v,2,HASH_JOIN,ea0_supplier,92,0.89
v,3,SAMPLER,,71,1.00
v,4,ORDER_BY,,67,1.00
e,0,1
e,0,2
e,1,2
e,2,3
e,3,4
#t,193,24
v,0,GROUP_BY:COMBINER,ea0_lineitem,94,0.51
v,1,SAMPLER,,67,1.00
v,2,ORDER_BY,,67,1.00
e,0,1
e,1,2
#t,194,24
v,0,COGROUP,ea0_lineitem:ea0_part,38,0.59
v,1,GROUP_BY:COMBINER,,50,1.00
e,0,1
#t,195,24
v,0,HASH_JOIN,ea0_lineitem:ea0_part,75,0.43
v,1,GROUP_BY:COMBINER,,11,1.00
e,0,1
#t,196,24
v,0,HASH_JOIN,ea0_partsupp:ea0_supplier,75,0.57
v,1,HASH_JOIN,ea0_part,76,0.81
v,2,GROUP_BY:COMBINER,,55,1.00
v,3,SAMPLER,,32,1.00
v,4,ORDER_BY,,17,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,197,24
v,0,HASH_JOIN,ea0_region:ea0_nation,41,0.83
v,1,HASH_JOIN,ea0_supplier,24,0.47
v,2,HASH_JOIN,ea0_partsupp,58,0.59
v,3,HASH_JOIN,ea0_part,9,0.36
v,4,GROUP_BY,,27,1.00
v,5,SAMPLER,,5,1.00
v,6,ORDER_BY:COMBINER,,43,1.00
v,7,SAVE,,30,1.00
e,0,1
e,1,2
e,2,3
e,3,4
e,4,5
e,5,6
e,6,7
#t,198,24
v,0,COGROUP,ea0_lineitem:ea0_orders,44,0.65
v,1,HASH_JOIN,ea0_customer,18,0.52
v,2,GROUP_BY:COMBINER,,69,1.00
v,3,SAMPLER,,29,1.00
v,4,ORDER_BY,,5,1.00
e,0,1
e,1,2
e,2,3
e,3,4
#t,199,24
v,0,GROUP_BY:COMBINER,ea2_lineitem,81,0.44
v,1,HASH_JOIN,,87,1.00
v,2,DISTINCT,ea2_part,82,0.65
v,3,HASH_JOIN,ea2_partsupp,84,0.40
v,4,DISTINCT,,12,1.00
v,5,HASH_JOIN,,25,1.00
v,6,HASH_JOIN,ea2_nation:ea2_supplier,50,0.61
v,7,SAMPLER,,20,1.00
v,8,ORDER_BY,,79,1.00
e,0,1
e,1,4
e,2,3
e,3,1
e,4,5
e,5,7
e,6,5
e,7,8
