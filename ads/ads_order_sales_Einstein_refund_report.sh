#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_order_sales_Einstein_refund_report_tmp;

create table ads.ads_order_sales_Einstein_refund_report_tmp
Engine=MergeTree
order by order_number as
select
 toStartOfDay(addHours(order_time,8)) as order_date,
 agent_name,
if(data_plan_type=1,'包天套餐',if(data_plan_type=2,'流量套餐',if(data_plan_type=3,'定向流量套餐',if(data_plan_type=5,'小时流量套餐','免费套餐')))) as data_plan_type,
 if(payment_method_name is null,'NoPay',payment_method_name) as payment_method_name,
 order_location_name,
 count(*) as order_number,
 sum(if(order_CNYamount is null,0,order_CNYamount)) as total_amount
from
dws.dws_redtea_order
where source = 'Einstein'
and order_status in ('REFUNDED')
and order_CNYamount > 0
and invalid_time = '2105-12-31 23:59:59'
group by toStartOfDay(addHours(order_time,8)),
          agent_name,
          data_plan_type,
          payment_method_name,
          order_location_name;

drop table if exists ads.ads_order_sales_Einstein_refund_report;

rename table ads.ads_order_sales_Einstein_refund_report_tmp to ads.ads_order_sales_Einstein_refund_report;
"