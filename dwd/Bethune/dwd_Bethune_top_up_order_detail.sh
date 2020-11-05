#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists dwd.dwd_Bethune_top_up_order_detail_tmp;

create table dwd.dwd_Bethune_top_up_order_detail_tmp
Engine=MergeTree
order by user_id as
select
  id,
  user_id,
  top_up_mobile,
  payment_mode,
  pay_status,
  top_up_type,
  amount,
  product_name,
  create_time
from
ods.ods_Bethune_top_up_order;

drop table if exists dwd.dwd_Bethune_top_up_order_detail;

rename table dwd.dwd_Bethune_top_up_order_detail_tmp to dwd.dwd_Bethune_top_up_order_detail;
"
