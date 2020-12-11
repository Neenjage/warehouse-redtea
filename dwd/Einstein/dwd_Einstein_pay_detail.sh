#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Einstein_pay_detail_tmp;

create table dwd.dwd_Einstein_pay_detail_tmp
Engine=MergeTree
order by pay_type as
select
  'alipay' as pay_type,
  buyer_id,
  out_trade_no
from
ods.ods_Einstein_global_alipay_response
union all
select
  'wechat' as pay_type,
  openid as buyer_id,
  out_trade_no
from
ods.d;

drop table if exists dwd.dwd_Einstein_pay_detail;

rename table dwd.dwd_Einstein_pay_detail_tmp to dwd.dwd_Einstein_pay_detail;
"

