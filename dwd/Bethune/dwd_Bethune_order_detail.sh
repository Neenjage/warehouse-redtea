#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Bethune_order_detail_tmp;

create table dwd.dwd_Bethune_order_detail_tmp
Engine=MergeTree
order by id as
select
t1.*,
data_plan.name as data_plan_name
from
(select
orders.*,
orders_device.payment_method,
orders_device.model,
orders_device.brand,
orders_device.user_ip
from
(select
  id,
  user_id,
  data_plan_id,
  imei,
  device_id,
  order_no,
  create_time,
  count,
  amount,
  type,
  status,
  resource_order_id as Einstein_order_id
from
ods.ods_Bethune_orders
where invalid_time = '2105-12-31 23:59:59') orders
left join
ods.ods_Bethune_orders_device orders_device
on orders.id = orders_device.order_id) t1
left join
dim.dim_Bethune_data_plan data_plan
on t1.data_plan_id = data_plan.id
;

drop table if exists dwd.dwd_Bethune_order_detail;

rename table dwd.dwd_Bethune_order_detail_tmp to dwd.dwd_Bethune_order_detail;
"