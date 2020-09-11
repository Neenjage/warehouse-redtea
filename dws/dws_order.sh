#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi


clickhouse-client -u$user --multiquery -q"
create table if not exists dws.dws_order_tmp
Engine=MergeTree
order by order_id as
select
t4.*,
dpd.data_plan_name,
if(payment_method_id = 4,order_CNYamount*6/1000,if(payment_method_id = 9,order_CNYamount*8/1000,order_CNYamount*4/100)) as transation_fee,
if(agent_id = 1 or agent_id = 14,
    if(startsWith(imsi,'460'),order_CNYamount*0.3,order_CNYamount*0.18),
    if(agent_id = 9 and startsWith(imsi,'460'),order_CNYamount*0.15,order_CNYamount*0.1)) as revenue_share,
(order_CNYamount-transation_fee-revenue_share) as net_amount_without_cost,
(order_CNYamount-transation_fee-revenue_share-cost) as net_amount
from
(select
t3.*,
bundle.bundle_id,
bundle.bundle_name,
bundle.carrier_id,
bundle.carrier_name,
bundle.bundle_group_id,
bundle.bundle_group_name
from
(select
  t2.* ,
  cdr.total_usage,
  cdr.cost
from
(select
t1.*,
transaction.transaction_id
from
(select
order_detail.*,
oipr.transaction_code,
oipr.bundle_code
from dwd.dwd_Einstein_orders_detail order_detail
left join dwd.dwd_Einstein_order_imsi_profile_relation oipr on order_detail.order_id = oipr.order_id ) t1
left join dwd.dwd_Bumblebee_imsi_transaction_detail transaction on t1.transaction_code = transaction.transaction_code) t2
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
) as cdr on t2.transaction_id = cdr.transaction_id) t3
left join
(select
bundle_id,
bundle_name,
carrier_id,
carrier_name,
bundle_group_id,
bundle_group_name
from dwd.dwd_Bumblebee_bundle_detail
where import_time = '$import_time') as  bundle on t3.bundle_code = bundle.bundle_code) t4
left join
(SELECT
data_plan_id,
data_plan_name
FROM
dwd.dwd_Einstein_data_plan_detail where import_time = '$import_time') as dpd on t4.data_plan_id = toString(dpd.data_plan_id)
"

clickhouse-client -u$user --multiquery -q"
drop table dws.dws_order
"

clickhouse-client -u$user --multiquery -q"
rename table dws.dws.dws_order_tmp to dws.dws_order
"

clickhouse-client -u$user --multiquery -q"
alter table dws.dws_order delete where transaction_id = -1
"

#将话单表中transaction_id为0，已转换为-1的数据纳入成本中(没有匹配度相关订单，但有流量消耗)
clickhouse-client -u$user --multiquery -q"
INSERT INTO TABLE dws.dws_order(
order_id,
order_province,
order_address,
currency_name,
order_source,
transaction_id,
bundle_id,
carrier_id,
bundle_group_id,
total_usage,
cost,
transation_fee,
revenue_share,
net_amount_without_cost,
net_amount)
select
  -1,
  'unknown',
  'unknown',
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
  0,
  -sum(cdr_raw.cost)
from
dwd.dwd_Bumblebee_imsi_transaction_cdr_raw cdr_raw
where cdr_raw.transaction_id = -1
group by
  cdr_raw.transaction_id
"


create table if not exists dws.dws_order_tmp
Engine=MergeTree
order by order_id as
select
t2.*,
dpd.data_plan_name,
if(payment_method_id = 4,order_CNYamount*6/1000,if(payment_method_id = 9,order_CNYamount*8/1000,order_CNYamount*4/100)) as transation_fee,
if(agent_id = 1 or agent_id = 14,
    if(startsWith(imsi,'460'),order_CNYamount*0.3,order_CNYamount*0.18),
    if(agent_id = 9 and startsWith(imsi,'460'),order_CNYamount*0.15,order_CNYamount*0.1)) as revenue_share,
(order_CNYamount-transation_fee-revenue_share) as net_amount_without_cost,
(order_CNYamount-transation_fee-revenue_share-cost) as net_amount
from



select
t2.*,
dpd.data_plan_name
from
(select
t1.*,
transaction.transaction_id
from
(select
order_detail.*,
oipr.transaction_code,
oipr.bundle_code
from dwd.dwd_Einstein_orders_detail order_detail
left join dwd.dwd_Einstein_order_imsi_profile_relation oipr on order_detail.order_id = oipr.order_id ) t1
left join dwd.dwd_Bumblebee_imsi_transaction_detail transaction on t1.transaction_code = transaction.transaction_code) t2
left join
(SELECT
data_plan_id,
data_plan_name
FROM
dwd.dwd_Einstein_data_plan_detail where import_time = '$import_time') as dpd on t2.data_plan_id = toString(dpd.data_plan_id)
union all


select
order_detail.*,
resource_detail.bundle_code,
resource_detail.iccid
from dwd.dwd_Nobel_orders_detail order_detail
left join dwd.dwd_Bell_imsi_resource_detail resource_detail
on order_detail.transaction_code = resource_detail.transaction_code





(select
t3.*,
bundle.bundle_id,
bundle.bundle_name,
bundle.carrier_id,
bundle.carrier_name,
bundle.bundle_group_id,
bundle.bundle_group_name
from

left join
(select
bundle_id,
bundle_name,
carrier_id,
carrier_name,
bundle_group_id,
bundle_group_name
from dwd.dwd_Bumblebee_bundle_detail
where import_time = '$import_time') as  bundle on t2.bundle_code = bundle.bundle_code) t3






(select
  t2.* ,
  cdr.total_usage,
  cdr.cost
from

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
) as cdr on t2.transaction_id = cdr.transaction_id) t3


