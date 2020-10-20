#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_user_add_day_einstein_report_tmp;

create table ads.ads_user_add_day_einstein_report_tmp
Engine=MergeTree
order by number as
select
 toStartOfDay(addHours(register_time,8)) as register_date,
 count(*) as number
from
dws.dws_redtea_user
where source = 'Einstein' and user_status = 'ACTIVE'
group by toStartOfDay(addHours(register_time,8));

drop table if exists ads.ads_user_add_day_einstein_report;

rename table ads.ads_user_add_day_einstein_report_tmp to ads.ads_user_add_day_einstein_report;
"