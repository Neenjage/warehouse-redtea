#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password $password --multiquery --multiline -q"
create table ads.ads_agent_model_report_tmp
Engine=MergeTree
order by number as
select
agent_name,
model,
count(*) as number
from
dws.dws_redtea_user
group by agent_name,model;

drop table ads.ads_agent_model_report;

rename table ads.ads_agent_model_report_tmp to ads.ads_agent_model_report;
"

