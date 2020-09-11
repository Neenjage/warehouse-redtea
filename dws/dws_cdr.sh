#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi


clickhouse-client -u$user --multiquery -q"
create table IF NOT EXISTS dws.dws_cdr_tmp
Engine=MergeTree
order by transaction_id as
select
t6.*,
local_carrier.location_id,
local_carrier.location_name,
local_carrier.local_carrier_id,
local_carrier.local_carrier_name,
local_carrier.plmn
FROM
(select
t5.*,
dpd.data_plan_name
from
(select
t4.*,
od.agent_id,
od.agent_name,
od.data_plan_id
from
(select
t3.*,
oipr.order_id
from
(select
t2.*,
bd.bundle_name,
bd.bundle_group_id
from
(select
t1.*,
itd.iccid,
itd.merchant_name as merchant_name
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
    import_time
FROM
dwd.dwd_Bumblebee_imsi_transaction_cdr_raw
) as t1
left join dwd.dwd_Bumblebee_imsi_transaction_detail itd on t1.transaction_id = itd.transaction_id) t2
left join
(select
bundle_id,
bundle_code,
bundle_group_id,
bundle_name
from dwd.dwd_Bumblebee_bundle_detail
where import_time = '$import_time' ) as bd on t2.bundle_code = bd.bundle_code) t3
left join dwd.dwd_Einstein_order_imsi_profile_relation oipr on t3.transaction_id = oipr.transaction_id) t4
left join dwd.dwd_Einstein_orders_detail od on t4.order_id = od.order_id) t5
left join (
SELECT
data_plan_id,
data_plan_name
FROM
dwd.dwd_Einstein_data_plan_detail where import_time = '$import_time'
)  dpd on t5.data_plan_id = toString(dpd.data_plan_id)) t6
left join
(
SELECT
  *
FROM
dwd.dwd_Bumblebee_local_carrier_detail where import_time = '$import_time') local_carrier
on t6.location_code = local_carrier.location_code and toString(t6.bundle_group_id) = toString(local_carrier.bundle_group_id)
"


clickhouse-client -u$user --ultiquery -q"
drop table dws.dws_cdr_tmp
"

clickhouse-client -u$user --ultiquery -q"
rename table dws.dws_cdr_tmp to dws.dws_cdr
"


create table dws.dws_cdr_tmp
Engine=MergeTree
order by transaction_id as
select
t6.*,
local_carrier.location_id,
local_carrier.location_name,
local_carrier.local_carrier_id,
local_carrier.local_carrier_name,
local_carrier.plmn
FROM
(select
t5.*,
dpd.data_plan_name
from
(select
t4.*,
od.agent_id,
od.agent_name,
od.data_plan_id
from
(select
t3.*,
oipr.order_id
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
itd.iccid,
itd.merchant_name as merchant_name
from
(SELECT
    transaction_id,
    imsi,
    bundle_code,
    merchant_id,
    country,
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
where import_time = '2020-08-21' ) as bd on t2.bundle_code = bd.bundle_code) t3
left join dwd.dwd_Einstein_order_imsi_profile_relation oipr on t3.transaction_id = oipr.transaction_id) t4
left join dwd.dwd_Einstein_orders_detail od on t4.order_id = od.order_id) t5
left join (
SELECT
data_plan_id,
data_plan_name
FROM
dwd.dwd_Einstein_data_plan_detail where import_time = '2020-08-25'
)  dpd on t5.data_plan_id = toString(dpd.data_plan_id)) t6
left join
(
SELECT
  *
FROM
dwd.dwd_Bumblebee_local_carrier_detail where import_time = '2020-09-07') local_carrier
on t6.location_code = local_carrier.location_code and toString(t6.bundle_group_id) = toString(local_carrier.bundle_group_id)





