#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

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
  if(source_type='0','Web',
  if(source_type='2001','Android',
  if(source_type='3001','redteago',
  if(source_type='3002','eSIM data',
  if(source_type='3003','IOS_IPAD_APP_INNER','MreSIM'))))) as source,
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
source,
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

select '数据统计结束.....................';

drop table if exists ads.ads_Nobel_order_report_tmp1;

create table ads.ads_Nobel_order_report_tmp1
Engine=MergeTree
order by type as
SELECT
    t1.type as type,
    t2.package as package,
    t3.source as source,
    t4.new_user_order as new_user_order,
    t5.user_first_order as user_first_order,
    t6.location_name as location_name,
    t7.payment_method as payment_method
FROM
(
    SELECT if(product_type = 'order', '套餐订单', '流量充值') AS type
    FROM dws.dws_Nobel_order
    WHERE (status IN ('1', 'SUCCESS')) AND (order_price >= 10000) AND (create_time > '2020-02-22 23:59:59') AND (create_time < toStartOfDay(now()))
    GROUP BY if(product_type = 'order', '套餐订单', '流量充值')
) AS t1
,
(
    SELECT multiIf(order_price = 10000, '新用户套餐', '普通套餐') AS package
    FROM dws.dws_Nobel_order
    WHERE (status IN ('1', 'SUCCESS')) AND (order_price >= 10000) AND (create_time > '2020-02-22 23:59:59') AND (create_time < toStartOfDay(now()))
    GROUP BY multiIf(order_price = 10000, '新用户套餐', '普通套餐')
) AS t2
,
(
    SELECT
      if(source_type='0','Web',
      if(source_type='2001','Android',
      if(source_type='3001','redteago',
      if(source_type='3002','eSIM data',
      if(source_type='3003','IOS_IPAD_APP_INNER','MreSIM'))))) as source
    FROM dws.dws_Nobel_order
    WHERE (status IN ('1', 'SUCCESS')) AND (order_price >= 10000) AND (create_time > '2020-02-22 23:59:59') AND (create_time < toStartOfDay(now()))
    GROUP BY source
) AS t3
,
(
    SELECT if(new_user_order_flag = 1, '新用户订单', '留存用户订单') AS new_user_order
    FROM dws.dws_Nobel_order
    WHERE (status IN ('1', 'SUCCESS')) AND (order_price >= 10000) AND (create_time > '2020-02-22 23:59:59') AND (create_time < toStartOfDay(now()))
    GROUP BY new_user_order
) AS t4
,
(
    SELECT if(user_first_order_flag = 1, '首次购买订单', '二次及以上订单') AS user_first_order
    FROM dws.dws_Nobel_order
    WHERE (status IN ('1', 'SUCCESS')) AND (order_price >= 10000) AND (create_time > '2020-02-22 23:59:59') AND (create_time < toStartOfDay(now()))
    GROUP BY user_first_order
) AS t5
,
(
    SELECT location_name
    FROM dws.dws_Nobel_order
    WHERE (status IN ('1', 'SUCCESS')) AND (order_price >= 10000) AND (create_time > '2020-02-22 23:59:59') AND (create_time < toStartOfDay(now()))
    GROUP BY location_name
) AS t6
,
(
    SELECT multiIf(payment_method_id = 0, 'WebPay', payment_method_id = 1, 'PayPal', payment_method_id = 2, 'AliPay', payment_method_id = 3, 'ApplePay', payment_method_id = 4, 'Credit Card', payment_method_id = 5, 'Credits', payment_method_id = 6, 'Credit Card', 'Stripe Apple Pay') AS payment_method
    FROM dws.dws_Nobel_order
    WHERE (status IN ('1', 'SUCCESS')) AND (order_price >= 10000) AND (create_time > '2020-02-22 23:59:59') AND (create_time < toStartOfDay(now()))
    GROUP BY payment_method
) AS t7;
"

for((i=0;i<=$date_number;i++));
do
  date_time=`date -d "$import_time -$i days" +"%Y-%m-%d"`
  clickhouse-client --user $user --password $password --multiquery --multiline -q"
  INSERT INTO ads.ads_Nobel_order_report_tmp
  SELECT
  t.type,
  t.package,
  toStartOfDay(toDateTime('$date_time 00:00:00')) as order_time,
  t.source,
  t.new_user_order,
  t.user_first_order,
  t.location_name,
  t.payment_method,
  0 as order_amount,
  0 as order_number,
  0 as user_number
FROM
ads.ads_Nobel_order_report_tmp1 t;
"
done

echo "插入0数据成功............................"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Nobel_order_report_tmp2;

create table ads.ads_Nobel_order_report_tmp2
Engine=MergeTree
order by order_number as
select
  total.type,
  total.package,
  total.order_time,
  total.source,
  total.new_user_order,
  total.user_first_order,
  total.location_name,
  total.payment_method,
  sum(total.order_amount) as order_amount,
  sum(total.order_number) as order_number,
  sum(total.user_number) as user_number
from ads.ads_Nobel_order_report_tmp total
group by
  total.type,
  total.package,
  total.order_time,
  total.source,
  total.new_user_order,
  total.user_first_order,
  total.location_name,
  total.payment_method;

drop table if exists ads.ads_Nobel_order_report;

drop table if exists ads.ads_Nobel_order_report_tmp;

drop table if exists ads.ads_Nobel_order_report_tmp1;

rename table ads.ads_Nobel_order_report_tmp2 to ads.ads_Nobel_order_report;
"