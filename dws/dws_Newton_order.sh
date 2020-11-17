#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dws.dws_Newton_order_tmp;

create table dws.dws_Newton_order_tmp
Engine=MergeTree
order by order_id as
select
*
from
dwd.dwd_Newton_order_detail;

drop table if exists dws.dws_Newton_order;

rename table dws.dws_Newton_order_tmp to dws.dws_Newton_order;
"
