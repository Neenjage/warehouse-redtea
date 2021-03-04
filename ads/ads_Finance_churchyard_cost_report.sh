#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists ads.ads_Finance_churchyard_cost_report
(
    id UInt8,
    order_month Nullable(UInt32),
    carrier_name Nullable(String),
    total_cost Nullable(Float64),
    value_add_tax Nullable(Float64),
    all_cost Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

ALTER TABLE ads.ads_Finance_churchyard_cost_report delete where order_month = toString(toYYYYMM(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))));

INSERT INTO table ads.ads_Finance_churchyard_cost_report
SELECT
 t.id,
 t.order_month,
 t.actual_carrier_name as carrier_name,
 t.total_cost1 as total_cost,
 t.value_add_tax,
 t.all_cost
FROM
  (SELECT
    0 as id,
    toYYYYMM(addHours(dro.end_time,8)) as order_month,
    case when dro.source = 'Nobel' then 'RedteaGO'
         when dro.carrier_name = 'shinetown&cuhk' then 'Shinetown Telecommunication Limited'
         when dro.carrier_name = 'Shinetown-VietnamMobile' then 'Shinetown Telecommunication Limited'
         when dro.carrier_name = '45412FromMB' then 'Multibyte Info Technology Ltd （MB）'
         when dro.carrier_name = 'AIS_wesim' then 'Advanced Info Services Public Company Limited'
         when dro.carrier_name = 'Dtac_Happy卡' then 'Beijing Zhida Hengxin Technology Co.Ltd.(致达恒信)'
         when dro.carrier_name = '红茶使用_中华电信_卓一' then 'HONGKONG JOY TELECOM CO.,LIMITED.（JOY）'
         when dro.carrier_name = 'CTM' then 'CITIC Talecom International Limited'
         when dro.carrier_name = 'Roamability' then 'Roamability PTE. LTD.'
         when dro.carrier_name = 'MB' then 'Multibyte Info Technology Ltd （MB）'
         when dro.carrier_name = '红茶_IIJ' then 'Internet Initiative Japan Inc.'
         when dro.carrier_name = 'MTX' then 'MTX Connect S.a r.l.'
         when dro.carrier_name = 'MTT' then 'OJSC Multiregional TransitTelecom （MTT）'
         when dro.carrier_name = 'Fareastone' then 'JIEFENG COMMERCE AND TRADE CO., LIMITED'
         when dro.carrier_name = '中移香港-港澳预付费' then 'CHINA MOBILE HONG KONG COMPANY LIMITED'
         when dro.carrier_name = 'KnowRoaming&红茶' then 'KnowRoaming Ltd'
         when dro.carrier_name = '红茶使用_卓一_CSL_业务资源' then 'HONGKONG JOY TELECOM CO.,LIMITED.（JOY）'
         when dro.carrier_name = '卓一&红茶&cuhk(红茶号段)' then 'HONGKONG JOY TELECOM CO.,LIMITED.（JOY）'
         when dro.carrier_name = '45403FromMB' then 'Multibyte Info Technology Ltd （MB）'
         when dro.carrier_name = 'AIS Sim2Fly_亚太' then 'Advanced Info Services Public Company Limited'
         when dro.carrier_name = 'DTAC亚太预付费' then 'DTAC TriNet Co,. LTD.'
         when dro.carrier_name = '连连科技' then '浙江连连科技有限公司'
         when dro.carrier_name = 'IOE联通-手机' then '深圳市艾欧益科技有限公司'
         when dro.carrier_name = '香港联通-越南预付费' then 'Beijing Zhida Hengxin Technology Co.Ltd.(致达恒信)'
         when dro.carrier_name = '佛山移动-手机' then '中国移动通信集团广东有限公司佛山分公司'
         when dro.carrier_name = '联通冰激凌卡-239元80G-API控制关停' then '上海彬山通信工程有限公司'
         when dro.carrier_name = '深圳移动-手机' then '中国移动通信集团广东有限公司深圳分公司'
         when dro.carrier_name = '联通冰激凌卡-129元30G-API控制关停' then '上海彬山通信工程有限公司'
         when dro.carrier_name = '联通冰激凌卡-39元20G' then '上海彬山通信工程有限公司'
         else carrier_name
    end as actual_carrier_name,
    toDecimal64(sum(dro.total_cost)/1.06,2) as total_cost1,
    toDecimal64(sum(dro.total_cost)/1.06*0.06,2) as value_add_tax,
    toDecimal64(sum(dro.total_cost),2) as all_cost
  FROM
  dws.dws_redtea_order dro
  where addHours(dro.end_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
  and addHours(dro.end_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))
  and data_plan_name  like '%国内%'
  and carrier_id != 0 and carrier_name is not null
  and total_cost > 0
  group by actual_carrier_name,
           order_month
  union all
  SELECT
    1 as id,
    toYYYYMM(addHours(dro.end_time,8)) as order_month,
    '合计' as actual_carrier_name,
    toDecimal64(sum(dro.total_cost)/1.06,2) as total_cost1,
    toDecimal64(sum(dro.total_cost)/1.06*0.06,2) as value_add_tax,
    toDecimal64(sum(dro.total_cost),2) as all_cost
  FROM
  dws.dws_redtea_order dro
  where addHours(dro.end_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
  and addHours(dro.end_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))
  and data_plan_name  like '%国内%'
  and carrier_id != 0
  and total_cost > 0
  group by order_month) t
WHERE t.actual_carrier_name is not null
and t.total_cost1 > 0;
"