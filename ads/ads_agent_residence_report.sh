#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_agent_residence_report_tmp;

create table ads.ads_agent_residence_report_tmp
Engine=MergeTree
order by number as
select
agent_name,
residence,
count(*) as number
from
dws.dws_redtea_user
where residence != 'CN' and  residence !='' and source = 'Einstein'
group by agent_name,residence;

drop table if exists ads.ads_agent_residence_report;

rename table ads.ads_agent_residence_report_tmp to ads.ads_agent_residence_report;
"


