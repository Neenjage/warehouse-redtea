#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_Bethune_user_report_tmp;

create table ads.ads_Bethune_user_report_tmp
Engine=MergeTree
order by number as
select
  toStartOfDay(addHours(create_time,8)) as time,
  multiIf(startsWith(brand,'vivo'),'vivo',startsWith(brand,'oppo'),'oppo',brand='full_oppo6763_17101','oppo','其它') as brand_type,
  if(recommend_user != '','好友推荐','非好友推荐') as recommend_type,
  if(is_valid = 1, '有效用户','非有效用户') as user_type,
  count(*) as number
from
dws.dws_Bethune_user
where addHours(create_time, 8) < toStartOfDay(addHours(now(), 8))
group by
if(is_valid = 1, '有效用户','非有效用户'),
if(recommend_user != '','好友推荐','非好友推荐'),
toStartOfDay(addHours(create_time,8)),
multiIf(startsWith(brand,'vivo'),'vivo',startsWith(brand,'oppo'),'oppo',brand='full_oppo6763_17101','oppo','其它');

drop table if exists ads.ads_Bethune_user_report;

rename table ads.ads_Bethune_user_report_tmp to ads.ads_Bethune_user_report;
"