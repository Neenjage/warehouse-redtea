#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh


#一个transaction_id对应多个order_id说明该订单为免费订单,将所有订单聚合为-1
clickhouse-client -u$user  --multiquery -q"
create table if not exists dwd.dwd_Einstein_order_imsi_profile_relation_tmp
Engine=MergeTree
order by order_id as
select
t1.*,
b.id as bundle_id
FROM
(SELECT
 oipr.order_id,
 if(it.imsi_transaction_id = 0,toInt32OrNull(oipr.transaction_id),it.imsi_transaction_id) as transaction_id,
 oipr.transaction_id as transaction_code,
 oipr.bundle_id as bundle_code,
 oipr.imsi
FROM
(select
  transaction_id,
  if(count(*) >1,-1,max(order_id)) as order_id,
  imsi,
  bundle_id
from
ods.ods_Einstein_order_imsi_profile_relation
group by
  transaction_id,
  imsi,
  bundle_id) as oipr
left join ods.ods_Bumblebee_imsi_transaction it on oipr.transaction_id = it.code) t1
left join dim.dim_Bumblebee_bundle b on t1.bundle_code = b.code
"

clickhouse-client -u$user --multiquery -q"
drop table dwd.dwd_Einstein_order_imsi_profile_relation
"

clickhouse-client -u$user --multiquery -q"
rename table dwd.dwd_Einstein_order_imsi_profile_relation_tmp to dwd.dwd_Einstein_order_imsi_profile_relation
"




