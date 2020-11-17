#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Newton_orders_tmp;

create table ods.ods_Newton_orders_tmp
Engine=MergeTree
order by id as
select
*
from
mysql('ro-newton-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'orders', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');

drop table if exists ods.ods_Newton_orders;

rename table ods.ods_Newton_orders_tmp to ods.ods_Newton_orders;
"