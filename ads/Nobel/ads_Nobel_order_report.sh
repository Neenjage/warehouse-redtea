#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_Nobel_order_report_tmp;

create table ads.ads_Nobel_order_report_tmp
Engine=MergeTree
order by order_number as
select
toStartOfDay(create_time) as order_time,
if(source_type=0,'Web',if(source_type=2001,'Android',if(source_type=3001,'ios','MreSIM'))) as source,
payment_method_name,
sum(order_price)/10000 as order_amount,
sum(topup_order_price)/10000 as top_order_amount,
sum(order_price)/10000+sum(topup_order_price)/10000 as total_amount,
count(*)  as order_number,
sum(topup_order_count) as topup_order_count,
sum(total_orders) as total_number,
countDistinct(email) as user_number
from
dws.dws_Nobel_order
where pay_status = 1
and data_plan_order_price >= 10000
and create_time > '2020-02-22 23:59:59'
and invalid_time = '2105-12-31 23:59:59'
group by toStartOfDay(create_time),
if(source_type=0,'Web',if(source_type=2001,'Android',if(source_type=3001,'ios','MreSIM'))),
payment_method_name;

drop table if exists ads.ads_Nobel_order_report;

rename table ads.ads_Nobel_order_report_tmp to ads.ads_Nobel_order_report;
"