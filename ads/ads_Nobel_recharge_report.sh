#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Nobel_recharge_report_tmp;

create table ads.ads_Nobel_recharge_report_tmp
Engine=MergeTree
order by charge_number as
select
  toStartOfDay(create_time) as recharge_time,
  sum(order_price)/10000 as charge_amount,
  count(*) as charge_number
from
dws.dws_Nobel_recharge
where order_status='PAID'
group by toStartOfDay(create_time);
"

for((i=0;i<=$date_number;i++));
do
  date_time=`date -d "$import_time -$i days" +"%Y-%m-%d"`
  clickhouse-client --user $user --password $password --multiquery --multiline -q"
  INSERT INTO ads.ads_Nobel_recharge_report_tmp
  SELECT
  toStartOfDay(toDateTime('$date_time 00:00:00')) as recharge_time,
  0 as charge_amount,
  0 as charge_number;
  "
done


clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Nobel_recharge_report_tmp1;

create table ads.ads_Nobel_recharge_report_tmp1
Engine=MergeTree
order by charge_number as
select
  recharge_time,
  sum(charge_amount) as charge_amount,
  sum(charge_number) as charge_number
from
ads.ads_Nobel_recharge_report_tmp
group by recharge_time;

drop table if exists ads.ads_Nobel_recharge_report;

drop table if exists ads.ads_Nobel_recharge_report_tmp;

rename table ads.ads_Nobel_recharge_report_tmp1 to ads.ads_Nobel_recharge_report;
"
