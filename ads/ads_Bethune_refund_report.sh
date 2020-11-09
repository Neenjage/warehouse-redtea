#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Bethune_refund_report_tmp;

create table ads.ads_Bethune_refund_report_tmp
Engine=MergeTree
order by order_number as
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
  SUM(amount)/100 AS total_amount,
  count(*) as order_number
FROM dws.dws_Bethune_order
WHERE type != '1'
AND addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))
AND status in ('6','7','REFUNDED')
GROUP BY source,
toStartOfDay(addHours(create_time,8)),
new_user_order_flag,
user_first_order_flag,
case when payment_method = 0 then '未知支付方式'
     when payment_method = 1 then '支付宝支付'
     when payment_method = 2 then '微信支付'
     else '积分兑换'
end;

drop table if exists ads.ads_Bethune_refund_report_tmp1;

CREATE TABLE ads.ads_Bethune_refund_report_tmp1
ENGINE = MergeTree
ORDER BY source AS
SELECT
    t1.source AS source,
    t2.new_user_order_flag AS new_user_order_flag,
    t3.user_first_order_flag AS user_first_order_flag,
    t4.payment_type AS payment_type
FROM
(
    SELECT source
    FROM dws.dws_Bethune_order
    WHERE (type != '1') AND (addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))) AND (status IN ('6', '7', 'REFUNDED'))
    GROUP BY source
) AS t1
,
(
    SELECT new_user_order_flag
    FROM dws.dws_Bethune_order
    WHERE (type != '1') AND (addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))) AND (status IN ('6', '7', 'REFUNDED'))
    GROUP BY new_user_order_flag
) AS t2
,
(
    SELECT user_first_order_flag
    FROM dws.dws_Bethune_order
    WHERE (type != '1') AND (addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))) AND (status IN ('6', '7', 'REFUNDED'))
    GROUP BY user_first_order_flag
) AS t3
,
(
    SELECT multiIf(payment_method = 0, '未知支付方式', payment_method = 1, '支付宝支付', payment_method = 2, '微信支付', '积分兑换') AS payment_type
    FROM dws.dws_Bethune_order
    WHERE (type != '1') AND (addHours(create_time, 8) < toStartOfDay(toDateTime(addHours(now(), 8)))) AND (status IN ('6', '7', 'REFUNDED'))
    GROUP BY multiIf(payment_method = 0, '未知支付方式', payment_method = 1, '支付宝支付', payment_method = 2, '微信支付', '积分兑换')
) AS t4;
"

#对每个维度数据加0处理
for((i=0;i<=$date_number;i++));
do
  date_time=`date -d "$import_time -$i days" +"%Y-%m-%d"`
  clickhouse-client --user $user --password $password --multiquery --multiline -q"
  INSERT INTO ads.ads_Bethune_refund_report_tmp
  SELECT
    source,
    new_user_order_flag,
    user_first_order_flag,
    payment_type,
    toStartOfDay(toDateTime('$date_time 00:00:00')) as order_date,
    0 AS total_amount,
    0 as order_number
  FROM ads.ads_Bethune_refund_report_tmp1
  "
done

#清除某些维度存在数据但是多一条0的数据
clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Bethune_refund_report_tmp2;

create table ads.ads_Bethune_refund_report_tmp2
Engine=MergeTree
order by order_number as
select
    source,
    new_user_order_flag,
    user_first_order_flag,
    payment_type,
    order_date,
    sum(total_amount) as total_amount,
    sum(order_number) as order_number
from
ads.ads_Bethune_refund_report_tmp
group by
    source,
    new_user_order_flag,
    user_first_order_flag,
    payment_type,
    order_date;

drop table if exists ads.ads_Bethune_refund_report;

drop table if exists ads.ads_Bethune_refund_report_tmp;

drop table if exists ads.ads_Bethune_refund_report_tmp1;

rename table ads.ads_Bethune_refund_report_tmp2 to ads.ads_Bethune_refund_report;
"