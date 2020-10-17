#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
create table ads.ads_user_add_day_report_tmp
Engine=MergeTree
order by number as
select
 toStartOfDay(addHours(register_time,8)) as register_date,
 count(*) as number
from
dws.dws_redtea_user
where user_status = 'ACTIVE'
group by toStartOfDay(addHours(register_time,8));

drop table ads.ads_user_add_day_report;

rename table ads.ads_user_add_day_report_tmp to ads.ads_user_add_day_report;
"