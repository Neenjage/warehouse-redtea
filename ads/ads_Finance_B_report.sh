#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_Finance_B_report_tmp;

create table ads.ads_Finance_B_report_tmp
Engine=MergeTree
order by source as
SELECT
'Newton' as source,
'维沃通信科技有限公司【Vivo-JL-采购模式】' as reseller_name,
toYYYYMM(addHours(order_time,8)) as order_month,
sum(if(pay_price is null,0,pay_price))/100 as amount
FROM
dws.dws_Newton_order
where reseller_id =10
and addHours(order_time,8) >= '2020-09-01 00:00:00'
and addHours(order_time,8) < '2020-10-01 00:00:00'
and status  not in ('RESERVED','REFUNDED','REFUNDING')
and activate_time is not null
group by reseller_name,toYYYYMM(addHours(order_time,8))
union all
SELECT
'Newton' as source,
case when reseller_id = 1 then '北京小米移动软件有限公司【小米-JL-采购模式】'
     when reseller_id = 3 then '珠海市魅族通讯设备有限公司【魅族-JL-采购模式】'
     else '三星（中国）投资有限公司【三星-RT-采购模式】' end as reseller_name,
toYYYYMM(addHours(end_time,8)) as order_month,
sum(if(pay_price is null,0,pay_price))/100 as amount
FROM
dws.dws_Newton_order
where reseller_id in (1,3,7)
and addHours(end_time,8) >= '2020-10-01 00:00:00'
and addHours(end_time,8) < '2020-11-01 00:00:00'
and status  not in ('RESERVED','REFUNDED','REFUNDING')
and activate_time is not null
group by reseller_name,toYYYYMM(addHours(end_time,8));

drop table if exists ads.ads_Finance_B_report;

rename table ads.ads_Finance_B_report_tmp to ads.ads_Finance_B_report;
"