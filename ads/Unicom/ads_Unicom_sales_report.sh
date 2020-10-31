#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_Unicom_sales_report_tmp;

create table ads.ads_Unicom_sales_report_tmp
Engine=MergeTree
order by number as
select
 toStartOfDay(addHours(order_time,8)) as order_date,
 agent_name,
 data_plan_volume as data_volume_name,
 new_user_order_flag,
 user_first_order_flag,
 countDistinct(device_id) as number,
 sum(data_plan_volume) as data_volume,
 count(*) as order_number,
 sum(if(order_CNYamount is null,0,order_CNYamount)) as total_amount
from
dws.dws_redtea_order
where source = 'Einstein'
and order_status not in ('REFUNDED','REFUNDING33','RESERVED')
and order_CNYamount > 0
and startsWith(imsi, '46001')
and invalid_time = '2105-12-31 23:59:59'
group by toStartOfDay(addHours(order_time,8)),
agent_name,
data_plan_volume,
new_user_order_flag,
user_first_order_flag;

drop table if exists ads.ads_Unicom_sales_report;

rename table ads.ads_Unicom_sales_report_tmp to ads.ads_Unicom_sales_report;
"