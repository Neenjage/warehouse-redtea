#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh


clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists ads.ads_Finance_B_report
(
    id UInt8,
    company String,
    order_month Nullable(String),
    wechat_income Nullable(Float64),
    wechat_fee Nullable(Float64),
    bank_income Nullable(Float64),
    no_tax_income Nullable(Float64),
    value_add_tax Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY company
SETTINGS index_granularity = 8192;

TRUNCATE TABLE ads.ads_Finance_B_report;

INSERT INTO  ads.ads_Finance_B_report
select
  0 as id,
  '合计' as company,
  toYYYYMM(addHours(end_time,8)) as order_month,
  toDecimal64(sum(pay_price)/100,2) as wechat_income,
  0 as wechat_fee,
  toDecimal64(sum(pay_price - 0)/100,2) as bank_income,
  toDecimal64(sum(pay_price)/100/1.06,2) as no_tax_income,
  toDecimal64(sum(pay_price)/100/1.06 * 0.06,2) as value_add_tax
from
dws.dws_Newton_order
where reseller_id in (1,3,7,10)
and addHours(end_time,8) >= '2020-01-01 00:00:00'
and addHours(end_time,8) < '2020-12-01 00:00:00'
and status  not in ('RESERVED','REFUNDED','REFUNDING')
and activate_time is not null
group by order_month

union all

SELECT
  1 as id,
  case when reseller_id = 1 then '北京小米移动软件有限公司【小米-JL-采购模式】'
       when reseller_id = 3 then '珠海市魅族通讯设备有限公司【魅族-JL-采购模式】'
       when reseller_id = 10 then '维沃通信科技有限公司【Vivo-JL-采购模式】'
       else '三星（中国）投资有限公司【三星-RT-采购模式】' end as company,
  toYYYYMM(addHours(end_time,8)) as order_month,
  toDecimal64(sum(pay_price)/100,2) as wechat_income,
  0 as wechat_fee,
  toDecimal64(sum(pay_price - 0)/100,2) as bank_income,
  toDecimal64(sum(pay_price)/100/1.06,2) as no_tax_income,
  toDecimal64(sum(pay_price)/100/1.06 * 0.06,2) as value_add_tax
FROM
dws.dws_Newton_order
where reseller_id in (1,3,7,10)
and addHours(end_time,8) >= '2020-01-01 00:00:00'
and addHours(end_time,8) < '2020-12-01 00:00:00'
and status  not in ('RESERVED','REFUNDED','REFUNDING')
and activate_time is not null
group by company,order_month;
"