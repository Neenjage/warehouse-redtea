source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ads.ads_order_sales_Newton_report_tmp;

create table ads.ads_order_sales_Newton_report_tmp
Engine=MergeTree
order by order_number as
select
  toStartOfDay(addHours(order_time,8)) as order_date,
    case when reseller_id = 1 then '北京小米移动软件有限公司【小米-JL-采购模式】'
       when reseller_id = 3 then '珠海市魅族通讯设备有限公司【魅族-JL-采购模式】'
       when reseller_id = 10 then '维沃通信科技有限公司【Vivo-JL-采购模式】'
       else '三星（中国）投资有限公司【三星-RT-采购模式】' end as company,
  sum(pay_price)/100 as order_amount,
  count(*) as order_number
from
dws.dws_Newton_order
where status not in ('RESERVED','REFUNDED','REFUNDING')
group by order_date,company;

drop table if exists ads.ads_order_sales_Newton_report;

rename table ads.ads_order_sales_Newton_report_tmp to ads.ads_order_sales_Newton_report;
"