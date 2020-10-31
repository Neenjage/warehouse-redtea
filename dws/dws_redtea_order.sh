#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

#将话单表中transaction_id为0，已转换为-1的数据纳入成本中(没有匹配到相关订单，但有流量消耗)
clickhouse-client --user $user --password '' --multiquery --multiline --max_memory_usage 30000000000 -q"
drop table if exists dws.dws_redtea_order_tmp;

create table dws.dws_redtea_order_tmp
Engine=MergeTree
order by order_id as
select
total2.*,
if(cdr.total_usage is null,0,cdr.total_usage) as total_usage,
if(cdr.cost is null,0,cdr.cost) as cost,
if((total2.order_CNYamount-total2.transation_fee-total2.revenue_share-cdr.cost) is null,0,
   (total2.order_CNYamount-total2.transation_fee-total2.revenue_share-cdr.cost)) as net_amount,
if(toDate(addHours(order_time,8)) = toDate(addHours(register_time,8)),1,0) as new_user_order_flag
from
(select
total1.*,
bundle_detail.bundle_id,
bundle_detail.bundle_name,
bundle_detail.carrier_id,
bundle_detail.carrier_name,
bundle_detail.bundle_group_id,
bundle_detail.bundle_group_name
from
(select
total.*,
if(transaction.transaction_id = 0,-2,transaction.transaction_id) as transaction_id
FROM
(SELECT
  t2.order_id,
  t2.source,
  t2.agent_id as agent_id,
  t2.agent_name,
  t2.data_plan_id,
  t2.data_plan_name,
  t2.data_plan_type,
  t2.data_plan_volume,
  t2.order_location_name,
  t2.imsi,
  t2.order_CNYamount,
  t2.payment_method_id,
  t2.payment_method_name,
  t2.transation_fee,
  t2.revenue_share,
  t2.currency_id,
  t2.currency_name ,
  t2.currency_CNY_rate,
  t2.user_id,
  t2.email,
  t2.device_id as device_id,
  t2.brand as brand,
  t2.order_status,
  t2.activate_time,
  t2.end_time,
  t2.order_time,
  t2.update_time,
  t2.purchased_ip,
  t2.effective_time,
  t2.invalid_time,
  t2.transaction_code,
  t2.bundle_code,
  device.residence,
  device.register_time
from
dwd.dwd_Einstein_device_detail device inner join
(SELECT
  t1.*,
  oipr.transaction_code,
  oipr.bundle_code
FROM
(SELECT
    toString(order_detail.order_id) as order_id,
    order_detail.order_source as source,
    toInt8(order_detail.agent_id) as agent_id,
    order_detail.agent_name,
    order_detail.data_plan_id,
    data_plan_detail.data_plan_name,
    data_plan_detail.data_plan_type,
    data_plan_detail.data_plan_volume,
    data_plan_detail.location_remark as order_location_name,
    order_detail.imsi,
    order_detail.order_CNYamount,
    order_detail.payment_method_id,
    order_detail.payment_method_name,
    if(order_detail.payment_method_id = 4,order_detail.order_CNYamount*6/1000,
        if(order_detail.payment_method_id = 9,order_detail.order_CNYamount*8/1000,
          order_detail.order_CNYamount*4/100)) as transation_fee,
    if(order_detail.agent_id = 1 or order_detail.agent_id = 14,
        if(startsWith(order_detail.imsi,'460'),order_detail.order_CNYamount*0.3,order_detail.order_CNYamount*0.18),
        if(order_detail.agent_id = 9 and startsWith(order_detail.imsi,'460'),order_detail.order_CNYamount*0.15,
           order_detail.order_CNYamount*0.1)) as revenue_share,
    order_detail.currency_id,
    order_detail.currency_name,
    order_detail.currency_CNY_rate,
    toInt32(-1) as user_id,
    'Einstein' as email,
    order_detail.device_id,
    order_detail.agent_name as brand,
    order_detail.order_status,
    order_detail.activate_time,
    order_detail.expiration_time as end_time,
    order_detail.order_time,
    order_detail.update_time,
    order_detail.order_ip as purchased_ip,
    order_detail.effective_time,
    order_detail.invalid_time
FROM dwd.dwd_Einstein_orders_detail order_detail
left join
(SELECT
  data_plan_id,
  data_plan_name,
  data_plan_type,
  data_plan_volume,
  location_remark
FROM
dwd.dwd_Einstein_data_plan_detail) data_plan_detail
on order_detail.data_plan_id = toString(data_plan_detail.data_plan_id)) t1
left join dwd.dwd_Einstein_order_imsi_profile_relation oipr on t1.order_id = toString(oipr.order_id)) t2
on t2.device_id = device.device_id
union all
select
t2.*,
users.register_time
from
(select
t1.*,
resource_detail.bundle_code,
'Not_cn' as residence
from
(select
  order_detail.order_id,
  order_detail.source,
  toInt8(order_detail.agent_id) as agent_id,
  order_detail.agent_name,
  toString(order_detail.day_client_resource_id) as data_plan_id,
  toString(data_plan_detail.data_plan_volume) as data_plan_name,
  0 as data_plan_type,
  data_plan_detail.data_plan_volume,
  order_detail.location_name as order_location_name,
  order_detail.imsi,
  order_detail.order_CNYamount,
  order_detail.payment_method_id,
  order_detail.payment_method_name,
  toFloat64(0) as transation_fee,
  toFloat64(0) as revenue_share,
  order_detail.currency_id,
  order_detail.currency_name,
  order_detail.currency_CNY_rate,
  order_detail.user_id,
  order_detail.email,
  order_detail.device_id,
  order_detail.model as brand,
  order_detail.order_status,
  order_detail.start_time as activate_time,
  order_detail.end_time as end_time,
  order_detail.create_time as order_time,
  order_detail.last_update_time as update_time,
  order_detail.ip as purchased_ip,
  order_detail.effective_time,
  order_detail.invalid_time,
  order_detail.transaction_code
FROM
dwd.dwd_Nobel_orders_detail order_detail
left join
(SELECT
  day_client_resource_id,
  data_plan_volume
FROM
dwd.dwd_Nobel_data_plan_detail where import_time = '$import_time')  data_plan_detail
on order_detail.day_client_resource_id = data_plan_detail.day_client_resource_id) t1
left join
(select
transaction_code,
bundle_code
from dwd.dwd_Bell_imsi_resource_detail where transaction_code is not null and transaction_code != '') resource_detail
on t1.transaction_code = resource_detail.transaction_code) t2
left join dwd.dwd_Nobel_users_detail users
on t2.email = users.email)  total
left join dwd.dwd_Bumblebee_imsi_transaction_detail transaction on total.transaction_code = transaction.transaction_code) total1
left join
(select
  bundle_code,
  bundle_id,
  bundle_name,
  carrier_id,
  carrier_name,
  bundle_group_id,
  bundle_group_name
from dwd.dwd_Bumblebee_bundle_detail
where import_time = '$import_time') bundle_detail on total1.bundle_code = bundle_detail.bundle_code) total2
left join
(select
  cdr_raw.transaction_id,
  sum(cdr_raw.total_usage) as total_usage,
  sum(cdr_raw.cost) as cost
from
dwd.dwd_Bumblebee_imsi_transaction_cdr_raw cdr_raw
where cdr_raw.transaction_id != -1
group by
  cdr_raw.transaction_id
) as cdr on total2.transaction_id = cdr.transaction_id;

drop table if exists dws.dws_redtea_order;

rename table dws.dws_redtea_order_tmp to dws.dws_redtea_order;

drop table if exists dws.dws_redtea_order_tmp1;

create table dws.dws_redtea_order_tmp1
Engine=MergeTree
order by order_id as
select
order.* ,
if(first_order.device_id is null,0,1) as user_first_order_flag
from
dws.dws_redtea_order as order
left join
(select
  device_id,
  min(order_time) as first_order_time
from
dws.dws_redtea_order
where source = 'Einstein'
and order_status not in ('REFUNDED','REFUNDING33','RESERVED')
and order_CNYamount > 0
and invalid_time = '2105-12-31 23:59:59'
group by device_id) first_order
on order.device_id = first_order.device_id and order.order_time = first_order.first_order_time;

drop table if exists dws.dws_redtea_order;

rename table dws.dws_redtea_order_tmp1 to dws.dws_redtea_order;

alter table dws.dws_redtea_order delete where transaction_id = -1;

INSERT INTO TABLE dws.dws_redtea_order(
order_id,
currency_name,
source,
transaction_id,
bundle_id,
carrier_id,
bundle_group_id,
total_usage,
cost,
transation_fee,
revenue_share,
net_amount)
select
  -1,
  'unknown',
  'unknown',
  cdr_raw.transaction_id,
  -1,
  -1,
  -1,
  sum(cdr_raw.total_usage) as total_usage,
  sum(cdr_raw.cost) as cost,
  0,
  0,
  -sum(cdr_raw.cost)
from
dwd.dwd_Bumblebee_imsi_transaction_cdr_raw cdr_raw
where cdr_raw.transaction_id = -1
group by
  cdr_raw.transaction_id;
"