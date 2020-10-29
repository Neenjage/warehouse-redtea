#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_Bethune_order_report_tmp;

create table ads.ads_Bethune_order_report_tmp
Engine=MergeTree
order by source as
SELECT
  source,
  new_user_order_flag,
  user_first_order_flag,
  toStartOfDay(addHours(create_time,8)) as order_date,
  count(*) as order_number,
  countDistinct(user_id) as user_number
FROM dws.dws_Bethune_order
WHERE type != '1'
AND addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))
AND status not in ('2','0','PAYMENT_CREATE')
GROUP BY source,
          toStartOfDay(addHours(create_time,8)),
          new_user_order_flag,
          user_first_order_flag;

drop table if exists ads.ads_Bethune_order_report;

rename table ads.ads_Bethune_order_report_tmp to ads.ads_Bethune_order_report;
"