#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists ads.ads_Finance_C_report
(
    company String,
    wechat_income Nullable(Float32),
    other_income Nullable(Float32),
    agent_revenue Nullable(Float32),
    wechat_fee Nullable(Float32),
    other_fee Nullable(Float32)
)
ENGINE = MergeTree
ORDER BY company
SETTINGS index_granularity = 8192;

TRUNCATE TABLE ads.ads_Finance_C_report;

INSERT into table ads.ads_Finance_C_report
select
t5.company,
toDecimal32(t5.wechat_income,3) as wechat_income,
toDecimal32(t5.other_income,3) as other_income,
toDecimal32(if(t6.agent_revenue is null,0,t6.agent_revenue),3)  as agent_revenue,
toDecimal32(t5.wechat_fee,3) as wechat_fee,
toDecimal32(t5.other_fee,3) as other_fee
from
(select
t3.*,
if(t4.other_income is null,0,t4.other_income) as other_income,
if(t4.other_fee is null,0,t4.other_fee) as other_fee
from
(select
t1.company,
if(t2.wechat_income is null,0,t2.wechat_income) as wechat_income,
if(t2.wechat_fee is null,0,t2.wechat_fee) as wechat_fee
from
(SELECT
t.company
from
(select
multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
        agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
        agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
        agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL ',
        agent_id = 6,'努比亚技术有限公司【nubia-JL】',
        agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
        agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
        agent_id = 5 and brand = 'Smartisan','SMARTISAN(锤子)',
        'unkonwn') as company
FROM
dws.dws_redtea_order drot
where  source = 'Einstein'
and order_status not in ('REFUNDED','REFUNDING','RESERVED')
and agent_id in (1,4,5,6,9,11)
and invalid_time = '2105-12-31 23:59:59') t
where t.company != 'unkonwn'
group by t.company) t1
left join
(SELECT
  multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
          agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
          agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
          agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL ',
          agent_id = 6,'努比亚技术有限公司【nubia-JL】',
          agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
          agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
          agent_id = 5 and brand = 'Smartisan','SMARTISAN(锤子)',
          'unkonwn') as company,
  sum(order_CNYamount) as wechat_income,
  sum(transation_fee) as wechat_fee
FROM
dws.dws_redtea_order drot
where  source = 'Einstein'
and order_status not in ('REFUNDED','REFUNDING','RESERVED')
and agent_id in (1,4,5,6,9,11)
and invalid_time = '2105-12-31 23:59:59'
and payment_method_id = 4
and end_time >= '2020-09-30 16:00:00'
and end_time < '2020-10-30 16:00:00'
group by
  multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
          agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
          agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
          agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL ',
          agent_id = 6,'努比亚技术有限公司【nubia-JL】',
          agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
          agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
          agent_id = 5 and brand = 'Smartisan','SMARTISAN(锤子)',
          'unkonwn')) t2
on t1.company = t2.company) t3
left join
(
SELECT
  multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
          agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
          agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
          agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL ',
          agent_id = 6,'努比亚技术有限公司【nubia-JL】',
          agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
          agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
          agent_id = 5 and brand = 'Smartisan','SMARTISAN(锤子)',
          'unkonwn') as company,
  sum(order_CNYamount) as other_income,
  sum(transation_fee) as other_fee
FROM
dws.dws_redtea_order drot
where  source = 'Einstein'
and order_status not in ('REFUNDED','REFUNDING','RESERVED')
and agent_id in (1,4,5,6,9,11)
and invalid_time = '2105-12-31 23:59:59'
and payment_method_id != 4
and end_time >= '2020-09-30 16:00:00'
and end_time < '2020-10-30 16:00:00'
group by
  multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
          agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
          agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
          agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL ',
          agent_id = 6,'努比亚技术有限公司【nubia-JL】',
          agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
          agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
          agent_id = 5 and brand = 'Smartisan','SMARTISAN(锤子)',
          'unkonwn')
) t4
on t3.company = t4.company ) t5
left join
(
SELECT
  multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
          agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
          agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
          agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL ',
          agent_id = 6,'努比亚技术有限公司【nubia-JL】',
          agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
          agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
          agent_id = 5 and brand = 'Smartisan','SMARTISAN(锤子)',
          'unkonwn') as company,
sum(if(agent_id = 1 or agent_id = 14,if(data_plan_name like '%国内%',0.3,0.18),
  if(agent_id = 9,if(data_plan_name like '%国内%',0.15,0.1),0.1)) * order_CNYamount) as agent_revenue
FROM
dws.dws_redtea_order drot
where  source = 'Einstein'
and order_status not in ('REFUNDED','REFUNDING','RESERVED')
and agent_id in (1,4,5,6,9,11)
and invalid_time = '2105-12-31 23:59:59'
and end_time >= '2020-09-30 16:00:00'
and end_time < '2020-10-30 16:00:00'
group by
  multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
          agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
          agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
          agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL ',
          agent_id = 6,'努比亚技术有限公司【nubia-JL】',
          agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
          agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
          agent_id = 5 and brand = 'Smartisan','SMARTISAN(锤子)',
          'unkonwn')
) t6
on t5.company = t6.company

union all

select
t.company,
toDecimal32(t.wechat_income,3) as wechat_income,
toDecimal32(t1.other_income,3) as other_income,
toDecimal32(0,3) as agent_revenue,
toDecimal32(t.wechat_fee,3) as wechat_fee,
toDecimal32(t1.other_fee,3) as other_fee
from
  (select
  '多多流量宝' as company,
  sum(amount)/100 as wechat_income,
  sum(amount)/100 * 0.006 as wechat_fee
  from
  dws.dws_Bethune_order
  WHERE type != '1'
  AND ((status not in ('2','0','5','3') and source = 'order'
        and addHours(end_time,8) >= '2020-10-01 00:00:00'
        and addHours(end_time,8) < '2020-11-01 00:00:00')
       or
       (status not in ('PAYMENT_CREATE','REFUNDED','PENDING')
       and source = 'top_order'
       and addHours(create_time,8) >= '2020-10-01 00:00:00'
       and addHours(create_time,8) < '2020-11-01 00:00:00'))
  AND payment_method = 2) t
  left join
  (select
  '多多流量宝' as company,
  sum(amount)/100 as other_income,
  sum(amount)/100 * 0.008 as other_fee
  from
  dws.dws_Bethune_order
  WHERE type != '1'
  AND ((status not in ('2','0','5','3') and source = 'order'
        and addHours(end_time,8) >= '2020-10-01 00:00:00'
        and addHours(end_time,8) < '2020-11-01 00:00:00')
       or
       (status not in ('PAYMENT_CREATE','REFUNDED','PENDING')
       and source = 'top_order'
       and addHours(create_time,8) >= '2020-10-01 00:00:00'
       and addHours(create_time,8) < '2020-11-01 00:00:00'))
  AND payment_method = 1) t1
  on t.company = t1.company;
"