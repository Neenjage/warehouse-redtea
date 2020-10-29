#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_agent_new_user_report_tmp;

create table ads.ads_agent_new_user_report_tmp
Engine=MergeTree
order by number as
select
toStartOfDay(addHours(register_time,8)) as register_date,
agent_name,
residence,
count(*) as number
from
dws.dws_redtea_user
where source = 'Einstein'
group by agent_name,toStartOfDay(addHours(register_time,8)),residence;

drop table if exists ads.ads_agent_new_user_report;

rename table ads.ads_agent_new_user_report_tmp to ads.ads_agent_new_user_report;
"


