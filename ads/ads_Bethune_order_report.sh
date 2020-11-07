#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Bethune_order_report_tmp;

create table ads.ads_Bethune_order_report_tmp
Engine=MergeTree
order by source as
SELECT
  source,
  new_user_order_flag,
  user_first_order_flag,
  case when payment_method = 0 then '未知支付方式'
     when payment_method = 1 then '支付宝支付'
     when payment_method = 2 then '微信支付'
     else '积分兑换'
  end as payment_type,
  toStartOfDay(addHours(create_time,8)) as order_date,
  count(*) as order_number,
  countDistinct(user_id) as user_number
FROM dws.dws_Bethune_order
WHERE type != '1'
AND addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))
AND status not in ('2','0','PAYMENT_CREATE')
GROUP BY
    source,
    toStartOfDay(addHours(create_time,8)),
    new_user_order_flag,
    user_first_order_flag,
    case when payment_method = 0 then '未知支付方式'
          when payment_method = 1 then '支付宝支付'
          when payment_method = 2 then '微信支付'
          else '积分兑换'
    end;

drop table if exists ads.ads_Bethune_order_report_tmp1;

create table ads.ads_Bethune_order_report_tmp1
Engine=MergeTree
order by source as
  SELECT
      t1.source as source,
      t2.payment_type as payment_type,
      t3.new_user_order_flag as new_user_order_flag,
      t4.user_first_order_flag as user_first_order_flag
  FROM
  (
      SELECT source
      FROM dws.dws_Bethune_order AS dbo
      WHERE (type != '1') AND (addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))) AND (status NOT IN ('2', '0', 'PAYMENT_CREATE'))
      GROUP BY source
  ) AS t1
  ,
  (
      SELECT multiIf(payment_method = 0, '未知支付方式', payment_method = 1, '支付宝支付', payment_method = 2, '微信支付', '积分兑换') AS payment_type
      FROM dws.dws_Bethune_order AS dbo2
      WHERE (type != '1') AND (addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))) AND (status NOT IN ('2', '0', 'PAYMENT_CREATE'))
      GROUP BY multiIf(payment_method = 0, '未知支付方式', payment_method = 1, '支付宝支付', payment_method = 2, '微信支付', '积分兑换')
  ) AS t2
  ,
  (
      SELECT new_user_order_flag
      FROM dws.dws_Bethune_order AS dbo3
      WHERE (type != '1') AND (addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))) AND (status NOT IN ('2', '0', 'PAYMENT_CREATE'))
      GROUP BY new_user_order_flag
  ) AS t3
  ,
  (
      SELECT user_first_order_flag
      FROM dws.dws_Bethune_order AS dbo4
      WHERE (type != '1') AND (addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))) AND (status NOT IN ('2', '0', 'PAYMENT_CREATE'))
      GROUP BY user_first_order_flag
  ) AS t4;
"

for((i=0;i<=$date_number;i++));
do
  date_time=`date -d "$import_time -$i days" +"%Y-%m-%d"`
  clickhouse-client --user $user --password $password --multiquery --multiline -q"
  INSERT INTO ads.ads_Bethune_order_report_tmp
  SELECT
    source,
    new_user_order_flag,
    user_first_order_flag,
    payment_type,
    toStartOfDay(toDateTime('$date_time 00:00:00')) as order_date,
    0 as order_number,
    0 as user_number
  FROM
  ads.ads_Bethune_order_report_tmp1
  "
done


clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Bethune_order_report_tmp2;

create table ads.ads_Bethune_order_report_tmp2
Engine=MergeTree
order by source as
select
    source,
    new_user_order_flag,
    user_first_order_flag,
    payment_type,
    order_date,
    sum(order_number) as order_number,
    sum(user_number) as user_number
from
ads.ads_Bethune_order_report_tmp
group by
    source,
    new_user_order_flag,
    user_first_order_flag,
    payment_type,
    order_date;

drop table if exists ads.ads_Bethune_order_report;

drop table if exists ads.ads_Bethune_order_report_tmp;

drop table if exists ads.ads_Bethune_order_report_tmp1;

rename table ads.ads_Bethune_order_report_tmp2 to ads.ads_Bethune_order_report;
"