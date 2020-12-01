#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Bethune_payment_order_info_tmp;

create table ods.ods_Bethune_payment_order_info_tmp
Engine=MergeTree
order by id  as
select
    id,
    payment_order_id,
    order_id,
    status
from
mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'payment_order_info', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm');

drop table if exists ods.ods_Bethune_payment_order_info;

rename table ods.ods_Bethune_payment_order_info_tmp to ods.ods_Bethune_payment_order_info;
"