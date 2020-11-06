#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


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

drop table if exists ads.ads_Nobel_recharge_report;

rename table ads.ads_Nobel_recharge_report_tmp to ads.ads_Nobel_recharge_report;
"
