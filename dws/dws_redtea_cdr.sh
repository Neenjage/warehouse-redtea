#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline --max_memory_usage 30000000000 -q"
drop table if exists dws.dws_redtea_cdr_tmp;

create table dws.dws_redtea_cdr_tmp
Engine=MergeTree
order by transaction_id as
select
t4.*,
local_carrier.location_id,
local_carrier.location_name,
local_carrier.local_carrier_id,
local_carrier.local_carrier_name,
local_carrier.plmn
FROM
(select
t3.*,
tmp2.order_id,
tmp2.agent_id,
tmp2.agent_name,
tmp2.data_plan_id,
tmp2.data_plan_name
from
(select
t2.*,
bd.bundle_id,
bd.bundle_name,
bd.bundle_group_id,
bd.carrier_id,
bd.carrier_name
from
(select
t1.*,
if(itd.transaction_code='','-1',itd.transaction_code) as transaction_code,
itd.iccid,
itd.merchant_name
from
(SELECT
    transaction_id,
    imsi,
    bundle_code,
    merchant_id,
    country,
    carrier,
    plmn as plmn_code,
    location_code,
    start_time,
    end_time,
    total_upload,
    total_download,
    total_usage,
    unit_price,
    cost,
    cdr_date
FROM
dwd.dwd_Bumblebee_imsi_transaction_cdr_raw
) as t1
left join dwd.dwd_Bumblebee_imsi_transaction_detail itd on t1.transaction_id = itd.transaction_id) t2
left join
(select
bundle_id,
bundle_code,
bundle_group_id,
bundle_name,
carrier_id,
carrier_name
from dwd.dwd_Bumblebee_bundle_detail
where import_time = '$import_time' ) as bd on t2.bundle_code = bd.bundle_code) t3
left join
(select
tmp1.transaction_code,
toString(tmp1.order_id) as order_id,
toInt32(tmp1.agent_id) as agent_id,
tmp1.agent_name,
tmp1.data_plan_id,
dpd.data_plan_name
FROM
(select
  oipr.transaction_code,
  oipr.order_id,
  od.agent_id,
  od.agent_name,
  od.data_plan_id
from
dwd.dwd_Einstein_order_imsi_profile_relation oipr
left join
(select
  order_id,
  agent_id,
  agent_name,
  data_plan_id
from
dwd.dwd_Einstein_orders_detail where invalid_time = '2105-12-31 23:59:59') od on oipr.order_id = od.order_id) tmp1
left join (
SELECT
  data_plan_id,
  data_plan_name
FROM
dwd.dwd_Einstein_data_plan_detail
) dpd on tmp1.data_plan_id = toString(dpd.data_plan_id)
union all
select
  od.transaction_code,
  od.order_id,
  toInt32(0) as agent_id,
  'redtea_go' as agent_name,
  toString(od.day_client_resource_id) as data_plan_id,
  toString(dpd.data_plan_volume) as data_plan_name
FROM
(select
  order_id,
  transaction_code,
  day_client_resource_id
from dwd.dwd_Nobel_orders_detail where invalid_time = '2105-12-31 23:59:59') od
left join
(select
  day_client_resource_id,
  data_plan_volume
from
dwd.dwd_Nobel_data_plan_detail where import_time = '$import_time') dpd on od.day_client_resource_id = dpd.day_client_resource_id) tmp2
on t3.transaction_code = tmp2.transaction_code) t4
left join
(
SELECT
  *
FROM
dwd.dwd_Bumblebee_local_carrier_detail where import_time = '$import_time') local_carrier
on t4.location_code = local_carrier.location_code and toString(t4.bundle_group_id) = toString(local_carrier.bundle_group_id);

drop table if exists dws.dws_redtea_cdr;

rename table dws.dws_redtea_cdr_tmp to dws.dws_redtea_cdr;
"
