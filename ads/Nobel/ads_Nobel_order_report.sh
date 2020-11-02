#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists ads.ads_Nobel_order_report_tmp;

create table ads.ads_Nobel_order_report_tmp
Engine=MergeTree
order by order_number as
select
  if(product_type = 'order','套餐订单','流量充值') as type,
  toStartOfDay(create_time) as order_time,
  if(source_type='0','Web',if(source_type='2001','Android',if(source_type='3001','ios','MreSIM'))) as source,
  if(new_user_order_flag = 1,'新用户订单','留存用户订单') as new_user_order,
  if(user_first_order_flag = 1,'首次购买订单','二次及以上订单') as user_first_order,
  sum(order_price)/10000 as order_amount,
  count(*)  as order_number,
  countDistinct(email) as user_number
from
dws.dws_Nobel_order
where status in ('1','SUCCESS')
and order_price >= 10000
and create_time > '2020-02-22 23:59:59'
group by toStartOfDay(create_time),
if(source_type='0','Web',if(source_type='2001','Android',if(source_type='3001','ios','MreSIM'))),
if(product_type = 'order','套餐订单','流量充值'),
new_user_order_flag,
user_first_order_flag;

drop table if exists ads.ads_Nobel_order_report;

rename table ads.ads_Nobel_order_report_tmp to ads.ads_Nobel_order_report;
"