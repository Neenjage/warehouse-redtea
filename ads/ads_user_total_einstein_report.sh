#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_user_total_einstein_report_tmp;

create table ads.ads_user_total_einstein_report_tmp
Engine=MergeTree
order by number as
select
 agent_name,
 count(*) as number
from
dws.dws_redtea_user
where source = 'Einstein'
and user_status = 'ACTIVE'
group by agent_name;

drop table if exists ads.ads_user_total_einstein_report;

rename table ads.ads_user_total_einstein_report_tmp to ads.ads_user_total_einstein_report;
"