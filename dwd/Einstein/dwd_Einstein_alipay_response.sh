#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Einstein_alipay_response_tmp;

create table dwd.dwd_Einstein_alipay_response_tmp
Engine=MergeTree
order by id as
select
id,
out_trade_no,
buyer_id
from
ods.ods_Einstein_global_alipay_response;

drop table if exists dwd.dwd_Einstein_alipay_response;

rename table dwd.dwd_Einstein_alipay_response_tmp to dwd.dwd_Einstein_alipay_response;
"