#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dws.dws_Nobel_wallet_tmp;

create table dws.dws_Nobel_wallet_tmp
Engine=MergeTree
order by transaction_month as
select
  transaction_month,
  sum(sales_amount) as sales
from
dwd.dwd_Nobel_user_wallet_detail
group by transaction_month;

drop table if exists dws.dws_Nobel_wallet;

rename table dws.dws_Nobel_wallet_tmp to dws.dws_Nobel_wallet;
"