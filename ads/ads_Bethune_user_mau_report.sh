#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Bethune_user_mau_report_tmp;

create table ads.ads_Bethune_user_mau_report_tmp
Engine=MergeTree
order by number as
SELECT
  toStartOfMonth(addHours(create_time,8)) as month,
  count(distinct user_id) AS number
FROM ods.ods_Bethune_user_device
GROUP BY
toStartOfMonth(addHours(create_time,8));

drop table if exists ads.ads_Bethune_user_mau_report;

rename table ads.ads_Bethune_user_mau_report_tmp to ads.ads_Bethune_user_mau_report;
"