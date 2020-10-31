#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password '' --multiquery --multiline -q"
create table if not exists ads.ads_Bumblebee_imsi_report
(imsi String,
usag float)
Engine=MergeTree
order by imsi
SETTINGS index_granularity = 8192;

TRUNCATE TABLE ads.ads_Bumblebee_imsi_report;

insert into ads.ads_Bumblebee_imsi_report
select imsi ,round(sum(download)/1024/1024/1024,2) as `usage`
from ods.ods_Bumblebee_imsi_transaction_cdr_raw obitcr
where imsi in ('454006109074792','454006109074793','454006109074800','454006109074803','454006109074808','454006109074810')
and end_time > '2020-08-06 00:00:00'
group by imsi
union all
select '合计',round(sum(download)/1024/1024/1024,2) as `usage`
from ods.ods_Bumblebee_imsi_transaction_cdr_raw obitcr
where imsi in ('454006109074792','454006109074793','454006109074800','454006109074803','454006109074808','454006109074810')
and end_time > '2020-08-06 00:00:00'
union all
select '剩余',800-round(sum(download)/1024/1024/1024,2) as `usage`
from ods.ods_Bumblebee_imsi_transaction_cdr_raw obitcr
where imsi in ('454006109074792','454006109074793','454006109074800','454006109074803','454006109074808','454006109074810')
and end_time > '2020-08-06 00:00:00';
"