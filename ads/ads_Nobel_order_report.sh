#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Nobel_order_report_tmp;

create table ads.ads_Nobel_order_report_tmp
Engine=MergeTree
order by order_number as
select
  if(product_type = 'order','套餐订单','流量充值') as type,
  case when order_price = 10000 then '新用户套餐'
	     else '普通套餐'
	end as package,
  toStartOfDay(create_time) as order_time,
  if(source_type='0','Web',if(source_type='2001','Android',if(source_type='3001','ios','MreSIM'))) as source,
  if(new_user_order_flag = 1,'新用户订单','留存用户订单') as new_user_order,
  if(user_first_order_flag = 1,'首次购买订单','二次及以上订单') as user_first_order,
  location_name,
  case when payment_method_id = 0 then 'WebPay'
     when payment_method_id = 1 then 'PayPal'
     when payment_method_id = 2 then 'AliPay'
     when payment_method_id = 3 then 'ApplePay'
     when payment_method_id = 4 then 'Credit Card'
     when payment_method_id = 5 then 'Credits'
     when payment_method_id = 6 then 'Credit Card'
     else 'Stripe Apple Pay'
  end as  payment_method,
  sum(order_price)/10000 as order_amount,
  count(*)  as order_number,
  countDistinct(email) as user_number
from
dws.dws_Nobel_order
where status in ('1','SUCCESS')
and order_price >= 10000
and create_time > '2020-02-22 23:59:59'
and create_time < toStartOfDay(now())
group by toStartOfDay(create_time),
if(source_type='0','Web',if(source_type='2001','Android',if(source_type='3001','ios','MreSIM'))),
if(product_type = 'order','套餐订单','流量充值'),
case when order_price = 10000 then '新用户套餐'
     else '普通套餐'
end,
new_user_order_flag,
user_first_order_flag,
location_name,
case when payment_method_id = 0 then 'WebPay'
   when payment_method_id = 1 then 'PayPal'
   when payment_method_id = 2 then 'AliPay'
   when payment_method_id = 3 then 'ApplePay'
   when payment_method_id = 4 then 'Credit Card'
   when payment_method_id = 5 then 'Credits'
   when payment_method_id = 6 then 'Credit Card'
   else 'Stripe Apple Pay'
end;

drop table if exists ads.ads_Nobel_order_report;

rename table ads.ads_Nobel_order_report_tmp to ads.ads_Nobel_order_report;
"