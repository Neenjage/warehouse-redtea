#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Nobel_topup_orders_detail_tmp;

create table dwd.dwd_Nobel_topup_orders_detail_tmp
Engine=MergeTree
order by dpo_order_no as
select
 dpo_order_no,
 data_volume,
 create_time,
 update_time,
 source_type,
 order_price,
 top_up_status
from
ods.ods_Nobel_data_plan_topup_order
where top_up_status = 'SUCCESS'
and invalid_time = '2105-12-31 23:59:59';

drop table if exists dwd.dwd_Nobel_topup_orders_detail;

rename table dwd.dwd_Nobel_topup_orders_detail_tmp to dwd.dwd_Nobel_topup_orders_detail;
"
