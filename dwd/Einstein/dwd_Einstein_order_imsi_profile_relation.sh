#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

#一个transaction_id对应多个order_id说明该订单为免费订单,将所有订单聚合为-1
clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists dwd.dwd_Einstein_order_imsi_profile_relation_tmp;

create table dwd.dwd_Einstein_order_imsi_profile_relation_tmp
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
left join
(select
   code,
   id
from dim.dim_Bumblebee_bundle
where import_time = '$import_time') b on t1.bundle_code = b.code;

drop table if exists dwd.dwd_Einstein_order_imsi_profile_relation;

rename table dwd.dwd_Einstein_order_imsi_profile_relation_tmp to dwd.dwd_Einstein_order_imsi_profile_relation;
"




