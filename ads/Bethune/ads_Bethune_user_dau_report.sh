#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_Bethune_user_dau_report_tmp;

create table ads.ads_Bethune_user_dau_report_tmp
Engine=MergeTree
order by number as
SELECT
  toStartOfDay(addHours(create_time,8)) as day,
  count(distinct user_id) AS number
FROM ods.ods_Bethune_user_device
GROUP BY
toStartOfDay(addHours(create_time,8));

drop table if exists ads.ads_Bethune_user_dau_report;

rename table ads.ads_Bethune_user_dau_report_tmp to ads.ads_Bethune_user_dau_report;
"