#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dws.dws_Nobel_recharge_tmp;

create table dws.dws_Nobel_recharge_tmp
Engine=MergeTree
order by id as
select
  id,
  source_type,
  order_price,
  create_time,
  order_status
from
dwd.dwd_Nobel_topup_package_detail;

drop table if exists dws.dws_Nobel_recharge;

rename table dws.dws_Nobel_recharge_tmp to dws.dws_Nobel_recharge;
"