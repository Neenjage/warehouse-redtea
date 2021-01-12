#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh



#vivo: 国内30%，海外18%（imsi like 460%即为国内，其他为海外）
#OPPO: 国内15%，海外10%
#其他： 国内10%，海外10%

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists ads.ads_Finance_churchyard_report
(
    id UInt8,
    company String,
    order_month Nullable(String),
    wechat_income Nullable(Float64),
    wechat_fee Nullable(Float64),
    bank_income Nullable(Float64),
    no_tax_income Nullable(Float64),
    value_add_tax Nullable(Float64),
    agent_revenue Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY company
SETTINGS index_granularity = 8192;

TRUNCATE TABLE ads.ads_Finance_churchyard_report;

INSERT INTO ads.ads_Finance_churchyard_report
SELECT
    id,
    company,
    order_month,
    toDecimal64(wechat_income1,2) as wechat_income,
    toDecimal64(wechat_fee1,2) as wechat_fee,
    toDecimal64(bank_income,2) as bank_income,
    toDecimal64(no_tax_income,2) as no_tax_income,
    toDecimal64(value_add_tax,2) as value_add_tax,
    toDecimal64(agent_revenue,2) as agent_revenue
from(
select
  0 as id,
  '合计' as company,
  order_month,
  sum(wechat_income) as wechat_income1,
  sum(wechat_fee) as wechat_fee1,
  sum(wechat_income - wechat_fee) as bank_income,
  sum(wechat_income / 1.06) as no_tax_income,
  sum(wechat_income / 1.06 * 0.06) as value_add_tax,
  sum(agent_revenue) as agent_revenue
from ads.ads_Finance_C_report
group by order_month

union all

select
  1 as id,
  company,
  order_month,
  sum(wechat_income) as wechat_income1,
  sum(wechat_fee) as wechat_fee1,
  sum(wechat_income - wechat_fee) as bank_income,
  sum(wechat_income / 1.06) as no_tax_income,
  sum(wechat_income / 1.06 * 0.06) as value_add_tax,
  sum(agent_revenue) as agent_revenue
from ads.ads_Finance_C_report
group by order_month,company) t;
"