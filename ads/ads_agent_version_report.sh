#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_agent_version_report_tmp;

create table ads.ads_agent_version_report_tmp
Engine=MergeTree
order by number as
select
agent_name,
app_version,
count(*) as number
from
dws.dws_redtea_user
where source = 'Einstein'
group by agent_name,app_version;

drop table if exists ads.ads_agent_version_report;

rename table ads.ads_agent_version_report_tmp to ads.ads_agent_version_report;
"

