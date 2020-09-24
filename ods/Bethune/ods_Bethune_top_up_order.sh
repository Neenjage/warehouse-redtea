#!/bin/bash

clickhouse-client -u$user --multiquery -q"
create table ods.ods_Bethune_top_up_order_tmp
Engine=MergeTree
order by id as
select
*
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'top_up_order', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
"


clickhouse-client -u$user --multiquery -q"
drop table ods.ods_Bethune_top_up_order
"


clickhouse-client -u$user --multiquery -q"
rename table ods.ods_Bethune_top_up_order_tmp to ods.ods_Bethune_top_up_order
"