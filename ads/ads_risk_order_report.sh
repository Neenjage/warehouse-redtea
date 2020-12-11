#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

#7天内，同一支付账户超过3台设备
clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_1_account_buy_3_7_days_tmp;

create table ads.ads_1_account_buy_3_7_days_tmp
Engine=MergeTree
order by order_id as
SELECT
    order_time,
    order_id,
    buyer_id,
    payment_method_name,
    device_id,
    data_plan_name
FROM dws.dws_redtea_order AS dro
WHERE (addHours(dro.order_time, 8) >= addDays(toStartOfDay(addHours(now(), -16)), -7)) AND (addHours(dro.order_time, 8) < toStartOfDay(addHours(now(), -16))) AND (buyer_id IN
(
    SELECT t.buyer_id
    FROM
    (
        SELECT
            device_id,
            buyer_id
        FROM dws.dws_redtea_order AS dro
        WHERE (addHours(dro.order_time, 8) >= addDays(toStartOfDay(addHours(now(), -16)), -7)) AND (addHours(dro.order_time, 8) < toStartOfDay(addHours(now(), -16)))
        GROUP BY
            buyer_id,
            device_id
    ) AS t
    WHERE isNotNull(t.buyer_id) AND (t.buyer_id != 'unknown')
    GROUP BY t.buyer_id
    HAVING count(*) > 3
))
ORDER BY buyer_id ASC;

drop table if exists ads.ads_1_account_buy_3_7_days;

rename table ads.ads_1_account_buy_3_7_days_tmp to ads.ads_1_account_buy_3_7_days;
"


#近14天内，订单上传流量值大于套餐流量1000M的订单详情
clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_volume_usage_overload_1000M_report_tmp;

create table ads.ads_volume_usage_overload_1000M_report_tmp
Engine=MergeTree
order by order_id as
SELECT
  order_time,
	device_id,
	order_id,
	data_plan_name ,
	data_plan_volume ,
	volume_usage/1024/1024 as volume_usage
FROM
dws.dws_redtea_order dro
where addHours(dro.order_time, 8) >= addDays(toStartOfDay(addHours(now(), -16)), -14)
AND addHours(dro.order_time, 8) < toStartOfDay(addHours(now(), -16))
AND volume_usage is not null
AND data_plan_volume != -1
AND dro.volume_usage/1024/1024-dro.data_plan_volume >= 1000
and device_id in
(SELECT
device_id
FROM
(SELECT
dro.device_id,
SUM(if(dro.volume_usage/1024/1024-dro.data_plan_volume >= 1000,1,0)) as number
FROM
dws.dws_redtea_order dro
where addHours(dro.order_time, 8) >= addDays(toStartOfDay(addHours(now(), -16)), -7)
AND addHours(dro.order_time, 8) < toStartOfDay(addHours(now(), -16))
AND volume_usage is not null
AND data_plan_volume != -1
GROUP BY dro.device_id) t);

drop table if exists ads.ads_volume_usage_overload_1000M_report;

rename table ads.ads_volume_usage_overload_1000M_report_tmp to ads.ads_volume_usage_overload_1000M_report;
"

#7天内，同一用户有超过5笔过期订单
clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_1_account_OBSOLETE_5_7_days_report_tmp;

create table ads.ads_1_account_OBSOLETE_5_7_days_report_tmp
Engine=MergeTree
order by order_id as
SELECT
	order_time,
	device_id,
	order_id,
	data_plan_name
FROM
dws.dws_redtea_order dro
where dro.order_status = 'OBSOLETE'
and addHours(dro.order_time, 8) >= addDays(toStartOfDay(addHours(now(), -16)), -7)
AND addHours(dro.order_time, 8) < toStartOfDay(addHours(now(), -16))
AND device_id in
(select
device_id
FROM
(SELECT
device_id,
count(*)
FROM
dws.dws_redtea_order dro
where dro.order_status = 'OBSOLETE'
and addHours(dro.order_time, 8) >= addDays(toStartOfDay(addHours(now(), -16)), -7)
AND addHours(dro.order_time, 8) < toStartOfDay(addHours(now(), -16))
GROUP BY device_id
HAVING count(*) > 5) t);

drop table if exists ads.ads_1_account_OBSOLETE_5_7_days_report;

rename table ads.ads_1_account_OBSOLETE_5_7_days_report_tmp to ads.ads_1_account_OBSOLETE_5_7_days_report;
"

#7天内，同一用户有超过5笔退款订单
clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_1_account_refund_5_7_days_report_tmp;

create table ads.ads_1_account_refund_5_7_days_report_tmp
Engine=MergeTree
order by order_id as
SELECT
	order_time,
	device_id,
	order_id,
	data_plan_name
FROM
dws.dws_redtea_order dro
where dro.order_status in ('REFUNDED','REFUNDING','REFUND_CHECKING')
and addHours(dro.order_time, 8) >= addDays(toStartOfDay(addHours(now(), -16)), -7)
AND addHours(dro.order_time, 8) < toStartOfDay(addHours(now(), -16))
AND device_id in
(select
device_id
FROM
(SELECT
device_id,
count(*)
FROM
dws.dws_redtea_order dro
where dro.order_status in ('REFUNDED','REFUNDING','REFUND_CHECKING')
and addHours(dro.order_time, 8) >= addDays(toStartOfDay(addHours(now(), -16)), -7)
AND addHours(dro.order_time, 8) < toStartOfDay(addHours(now(), -16))
GROUP BY device_id
HAVING count(*) > 5) t);

drop table if exists ads.ads_1_account_refund_5_7_days_report;

rename table ads.ads_1_account_refund_5_7_days_report_tmp to ads.ads_1_account_refund_5_7_days_report;
"


#7天内，有高危地区订单情况
clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_risk_area_report_tmp;

create table ads.ads_risk_area_report_tmp
Engine=MergeTree
order by order_id as
SELECT
	order_time,
	device_id,
	order_id,
	data_plan_name,
	order_address
FROM
dws.dws_redtea_order dro
where (dro.order_address like '%云南%'
or dro.order_address like '%海南%'
or dro.order_address like '%新疆%'
or dro.order_address like '%龙岩%'
or dro.order_address like '%防城港%'
or dro.order_address like '%北海%'
or dro.order_address like '%南宁%'
or dro.order_address like '%茂名%'
or dro.order_address like '%湛江%'
or dro.order_address like '%阳江%')
and data_plan_name like '%国内%'
and addHours(dro.order_time, 8) >= addDays(toStartOfDay(addHours(now(), -16)), -7)
AND addHours(dro.order_time, 8) < toStartOfDay(addHours(now(), -16))
AND device_id in
(select
device_id
FROM
(SELECT
device_id,
count(*)
FROM
dws.dws_redtea_order dro
where  addHours(dro.order_time, 8) >= addDays(toStartOfDay(addHours(now(), -16)), -7)
AND addHours(dro.order_time, 8) < toStartOfDay(addHours(now(), -16))
and (dro.order_address like '%云南%'
or dro.order_address like '%海南%'
or dro.order_address like '%新疆%'
or dro.order_address like '%龙岩%'
or dro.order_address like '%防城港%'
or dro.order_address like '%北海%'
or dro.order_address like '%南宁%'
or dro.order_address like '%茂名%'
or dro.order_address like '%湛江%'
or dro.order_address like '%阳江%')
and data_plan_name like '%国内%'
GROUP BY device_id) t);

drop table if exists ads.ads_risk_area_report;

rename table ads.ads_risk_area_report_tmp to ads.ads_risk_area_report;
"