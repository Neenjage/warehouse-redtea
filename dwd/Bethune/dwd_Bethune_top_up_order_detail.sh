#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Bethune_top_up_order_detail_tmp;

create table dwd.dwd_Bethune_top_up_order_detail_tmp
Engine=MergeTree
order by user_id as
select
t1.*,
payment_order.status as payment_status,
payment_order.update_time as payment_time
from
(select
  top_up_order.id,
  top_up_order.user_id,
  top_up_order.order_no,
  top_up_order.top_up_mobile,
  top_up_order.payment_mode,
  top_up_order.pay_status,
  top_up_order.top_up_type,
  top_up_order.amount,
  top_up_order.product_id,
  top_up_order.product_name,
  top_up_order.create_time,
  top_up_order.update_time,
  payment_order_info.payment_order_id
from
ods.ods_Bethune_top_up_order top_up_order
left join ods.ods_Bethune_payment_order_info payment_order_info
on top_up_order.order_no = payment_order_info.order_id) t1
left join ods.ods_Mammon_payment_order payment_order
on t1.payment_order_id = payment_order.order_id;

drop table if exists dwd.dwd_Bethune_top_up_order_detail;

rename table dwd.dwd_Bethune_top_up_order_detail_tmp to dwd.dwd_Bethune_top_up_order_detail;
"
