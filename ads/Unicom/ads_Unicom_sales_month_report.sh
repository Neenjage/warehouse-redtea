#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_Unicom_sales_month_report_tmp;

create table ads.ads_Unicom_sales_month_report_tmp
Engine=MergeTree
order by number as
select
 toDateTime(toStartOfMonth(addHours(order_time,8))) as order_month,
 user_first_order_flag,
 countDistinct(device_id) as number
from
dws.dws_redtea_order
where source = 'Einstein'
and order_status not in ('REFUNDED','REFUNDING33','RESERVED')
and order_CNYamount > 0
and startsWith(imsi, '46001')
and invalid_time = '2105-12-31 23:59:59'
group by toDateTime(toStartOfMonth(addHours(order_time,8))),
user_first_order_flag;

drop table if exists ads.ads_Unicom_sales_month_report;

rename table ads.ads_Unicom_sales_month_report_tmp to ads.ads_Unicom_sales_month_report;
"