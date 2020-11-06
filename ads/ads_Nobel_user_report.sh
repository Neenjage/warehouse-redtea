#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Nobel_user_report_tmp;

create table ads.ads_Nobel_user_report_tmp
Engine=MergeTree
order by user_number as
select
  toStartOfDay(register_time) as register_date,
  count(user_id) as user_number
from dws.dws_Nobel_user
where user_status = 'ACTIVE'
and register_time > '2020-02-22 23:59:59'
group by toStartOfDay(register_time);

drop table if exists ads.ads_Nobel_user_report;

rename table ads.ads_Nobel_user_report_tmp to ads.ads_Nobel_user_report;
"