#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Bethune_top_up_order_tmp;

create table ods.ods_Bethune_top_up_order_tmp
Engine=MergeTree
order by id as
select
    id,
    order_no,
    user_id,
    top_up_mobile,
    pay_status,
    amount,
    payment_mode,
    expend_balance,
    top_up_package_id,
    product_id,
    product_name,
    par_value,
    top_up_status,
    provider_order_no,
    top_up_type,
    create_time,
    update_time
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'top_up_order', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm');

drop table if exists ods.ods_Bethune_top_up_order;

rename table ods.ods_Bethune_top_up_order_tmp to ods.ods_Bethune_top_up_order;
"

