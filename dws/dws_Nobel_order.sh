#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists dws.dws_Nobel_order_tmp;

create table dws.dws_Nobel_order_tmp
ENGINE=MergeTree
order by id as
select
*
from
dwd.dwd_Nobel_orders_detail

"