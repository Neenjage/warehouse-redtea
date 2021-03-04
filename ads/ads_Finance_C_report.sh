#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi


clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists ads.ads_Finance_C_report
(
    company String,
    order_month Nullable(UInt32),
    account Nullable(String),
    wechat_income Nullable(Float32),
    other_income Nullable(Float32),
    agent_revenue Nullable(Float32),
    wechat_fee Nullable(Float32),
    other_fee Nullable(Float32)
)
ENGINE = MergeTree
ORDER BY company
SETTINGS index_granularity = 8192;

ALTER TABLE ads.ads_Finance_C_report delete where order_month = toString(toYYYYMM(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))));

INSERT INTO table ads.ads_Finance_C_report
select
t5.company,
t5.order_month,
t5.account,
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
t1.order_month,
t1.account,
if(t2.wechat_income is null,0,t2.wechat_income) as wechat_income,
if(t2.wechat_fee is null,0,t2.wechat_fee) as wechat_fee
from
(
    SELECT
    t.company,
    t.order_month,
    t.account
    from
    (select
    multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
            agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
            agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
            agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL】',
            agent_id = 6,'努比亚技术有限公司【nubia-JL】',
            agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
            agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
            agent_id = 5 and brand in ('Smartisan','SMARTISAN'),'SMARTISAN(锤子)',
            '海信-Hisense') as company,
    toYYYYMM(addHours(payment_time,8)) as order_month,
    multiIf(account = '1320939401','上海红茶网络科技有限公司',
            account = 'pay_redteasz@redteamobile.com','深圳红茶移动科技有限公司',
            account = '1605063756','深圳红茶移动科技有限公司',
            account = '1501257801','深圳杰睿联科技有限公司',
            account = 'globalpay_sg@redteamobile.com','REDTEA MOBILE PTE. LTD.',
            'REDTEA MOBILE PTE. LTD.') as account
    FROM
    dws.dws_redtea_order drot
    where  source = 'Einstein'
    and order_status not in ('RESERVED')
    and payment_method_id not in (0,-1)
    and invalid_time = '2105-12-31 23:59:59'
    and addHours(payment_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
    and addHours(payment_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))
    and order_CNYamount > 0.1) t
    group by t.company,order_month,account) t1
left join
    (

    SELECT
       t2_tmp_sales.company,
       t2_tmp_sales.order_month,
       t2_tmp_sales.account,
       (t2_tmp_sales.wechat_income-t2_tmp_refund.wechat_refund) as wechat_income,
       (t2_tmp_sales.wechat_fee-t2_tmp_refund.wechat_refund_fee) as wechat_fee
    FROM
        (SELECT
          multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
                  agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
                  agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
                  agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL】',
                  agent_id = 6,'努比亚技术有限公司【nubia-JL】',
                  agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
                  agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
                agent_id = 5 and brand in ('Smartisan','SMARTISAN'),'SMARTISAN(锤子)',
                '海信-Hisense') as company,
          toYYYYMM(addHours(payment_time,8)) as order_month,
          multiIf(account = '1320939401','上海红茶网络科技有限公司',
                account = 'pay_redteasz@redteamobile.com','深圳红茶移动科技有限公司',
                account = '1605063756','深圳红茶移动科技有限公司',
                account = '1501257801','深圳杰睿联科技有限公司',
                account = 'globalpay_sg@redteamobile.com','REDTEA MOBILE PTE. LTD.',
                'REDTEA MOBILE PTE. LTD.') as account,
          sum(order_CNYamount) as wechat_income,
          sum(transation_fee) as wechat_fee
        FROM
        dws.dws_redtea_order drot
        where  source = 'Einstein'
        and order_status not in ('RESERVED')
        and invalid_time = '2105-12-31 23:59:59'
        and payment_method_id not in (0,-1)
        and account != 'REDTEA MOBILE PTE. LTD.'
        and order_CNYamount > 0.1
        and addHours(payment_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
        and addHours(payment_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))
        group by
         company,
         order_month,
         account)  as t2_tmp_sales
        left join
        (SELECT
          multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
                  agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
                  agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
                  agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL】',
                  agent_id = 6,'努比亚技术有限公司【nubia-JL】',
                  agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
                  agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
                agent_id = 5 and brand in ('Smartisan','SMARTISAN'),'SMARTISAN(锤子)',
                '海信-Hisense') as company,
          toYYYYMM(addHours(payment_time,8)) as order_month,
          multiIf(account = '1320939401','上海红茶网络科技有限公司',
                account = 'pay_redteasz@redteamobile.com','深圳红茶移动科技有限公司',
                account = '1605063756','深圳红茶移动科技有限公司',
                account = '1501257801','深圳杰睿联科技有限公司',
                account = 'globalpay_sg@redteamobile.com','REDTEA MOBILE PTE. LTD.',
                'REDTEA MOBILE PTE. LTD.') as account,
          sum(order_CNYamount) as wechat_refund,
          sum(transation_fee) as wechat_refund_fee
        FROM
        dws.dws_redtea_order drot
        where  source = 'Einstein'
        and order_status in ('REFUNDED')
        and invalid_time = '2105-12-31 23:59:59'
        and payment_method_id not in (0,-1)
        and account != 'REDTEA MOBILE PTE. LTD.'
        and order_CNYamount > 0.1
        and addHours(refund_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
        and addHours(refund_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))
        group by
         company,
         order_month,
         account)  as t2_tmp_refund
        on t2_tmp_sales.company = t2_tmp_refund.company
        and t2_tmp_sales.order_month = t2_tmp_refund.order_month
        and t2_tmp_sales.account = t2_tmp_refund.account

     ) t2
on t1.company = t2.company and t1.order_month = t2.order_month and t1.account = t2.account) t3
left join
(

   SELECT
       t4_tmp_sales.company,
       t4_tmp_sales.order_month,
       t4_tmp_sales.account,
       (t4_tmp_sales.other_income-t4_tmp_refund.other_refund) as other_income,
       (t4_tmp_sales.other_fee-t4_tmp_refund.other_refund_fee) as other_fee
   from
        (SELECT
          multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
                  agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
                  agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
                  agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL】',
                  agent_id = 6,'努比亚技术有限公司【nubia-JL】',
                  agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
                  agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
                agent_id = 5 and brand in ('Smartisan','SMARTISAN'),'SMARTISAN(锤子)',
                '海信-Hisense') as company,
          toYYYYMM(addHours(payment_time,8)) as order_month,
          multiIf(account = '1320939401','上海红茶网络科技有限公司',
                account = 'pay_redteasz@redteamobile.com','深圳红茶移动科技有限公司',
                account = '1605063756','深圳红茶移动科技有限公司',
                account = '1501257801','深圳杰睿联科技有限公司',
                account = 'globalpay_sg@redteamobile.com','REDTEA MOBILE PTE. LTD.',
                'REDTEA MOBILE PTE. LTD.') as account,
          sum(order_CNYamount) as other_income,
          sum(transation_fee) as other_fee
        FROM
        dws.dws_redtea_order drot
        where  source = 'Einstein'
        and order_status not in ('RESERVED')
        and invalid_time = '2105-12-31 23:59:59'
        and account = 'REDTEA MOBILE PTE. LTD.'
        and order_CNYamount > 0.1
        and addHours(payment_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
        and addHours(payment_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))
        group by
         company,
         order_month,
         account) as t4_tmp_sales
         left join
         (SELECT
          multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
                  agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
                  agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
                  agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL】',
                  agent_id = 6,'努比亚技术有限公司【nubia-JL】',
                  agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
                  agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
                agent_id = 5 and brand in ('Smartisan','SMARTISAN'),'SMARTISAN(锤子)',
                '海信-Hisense') as company,
          toYYYYMM(addHours(payment_time,8)) as order_month,
          multiIf(account = '1320939401','上海红茶网络科技有限公司',
                account = 'pay_redteasz@redteamobile.com','深圳红茶移动科技有限公司',
                account = '1605063756','深圳红茶移动科技有限公司',
                account = '1501257801','深圳杰睿联科技有限公司',
                account = 'globalpay_sg@redteamobile.com','REDTEA MOBILE PTE. LTD.',
                'REDTEA MOBILE PTE. LTD.') as account,
          sum(order_CNYamount) as other_refund,
          sum(transation_fee) as other_refund_fee
        FROM
        dws.dws_redtea_order drot
        where  source = 'Einstein'
        and order_status in ('REFUNDED')
        and invalid_time = '2105-12-31 23:59:59'
        and account = 'REDTEA MOBILE PTE. LTD.'
        and order_CNYamount > 0.1
        and addHours(refund_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
        and addHours(refund_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))
        group by
         company,
         order_month,
         account) as t4_tmp_refund
         on t4_tmp_sales.company= t4_tmp_refund.company
         and t4_tmp_sales.order_month= t4_tmp_refund.order_month
         and t4_tmp_sales.account= t4_tmp_refund.account
) t4
on t3.company = t4.company and t3.order_month = t4.order_month and t3.account = t4.account) t5
left join
(
SELECT
  multiIf(agent_id = 1 or agent_id = 14,'维沃通信科技有限公司【vivo-RT】',
          agent_id = 4, '北京神奇工场科技有限公司【Zuk(联想)-RT】',
          agent_id = 9, '东莞市讯怡电子科技有限公司【OPPO-JL】',
          agent_id = 11,'深圳市万普拉斯科技有限公司【One Plus-JL】',
          agent_id = 6,'努比亚技术有限公司【nubia-JL】',
          agent_id = 5 and brand = 'Nokia','HMD  Global  Oy【Nokia-JL】',
          agent_id = 5 and brand = 'SUGAR','深圳市糖果智能通讯有限公司【糖果-JL】',
        agent_id = 5 and brand in ('Smartisan','SMARTISAN'),'SMARTISAN(锤子)',
        '海信-Hisense') as company,
  toYYYYMM(addHours(end_time,8)) as order_month,
  multiIf(account = '1320939401','上海红茶网络科技有限公司',
        account = 'pay_redteasz@redteamobile.com','深圳红茶移动科技有限公司',
        account = '1605063756','深圳红茶移动科技有限公司',
        account = '1501257801','深圳杰睿联科技有限公司',
        account = 'globalpay_sg@redteamobile.com','REDTEA MOBILE PTE. LTD.',
        'REDTEA MOBILE PTE. LTD.') as account,
  sum(if(agent_id = 1 or agent_id = 14,if(data_plan_name like '%国内%',0.3,0.18),
  if(agent_id = 9,if(data_plan_name like '%国内%',0.15,0.1),0.1)) * order_CNYamount) as agent_revenue
FROM
dws.dws_redtea_order drot
where  source = 'Einstein'
and order_status not in ('REFUNDED','REFUNDING','RESERVED')
and invalid_time = '2105-12-31 23:59:59'
and order_CNYamount > 0.1
and addHours(end_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
and addHours(end_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))
group by
  company,
  order_month,
  account
 )t6
on t5.company = t6.company and t5.order_month = t6.order_month and t5.account = t6.account

union all

select
t.company,
t.order_month,
t.account,
toDecimal32(if(t.churchyard_income is null,0,t.churchyard_income),3) as wechat_income,
toDecimal32(if(overseas_income is null,0,overseas_income),3) as other_income,
toDecimal32(0,3) as agent_revenue,
toDecimal32(if(t.churchyard_fee is null,0,t.churchyard_fee),3) as wechat_fee,
toDecimal32(if(overseas_fee is null,0,overseas_fee),3) as other_fee
from
(
select
 t1.company,
 t1.order_month,
 t1.account_company as account,
 if(t1.churchyard_income1-t2.churchyard_refund < 0,0,t1.churchyard_income1-t2.churchyard_refund) as churchyard_income,
 if(t1.overseas_income1-t2.overseas_refund < 0,0,t1.overseas_income1-t2.overseas_refund) as overseas_income,
 churchyard_income*0.006 as churchyard_fee,
 overseas_income*0.008 as overseas_fee
from
  (select
  '多多流量宝' as company,
  toYYYYMM(addHours(payment_time,8)) as order_month,
  '深圳红茶移动科技有限公司' as account_company,
  sum(if(account = '1605063756' or account = 'pay_redteasz@redteamobile.com',amount,0))/100 as churchyard_income1,
  sum(if(account = '1605063756' or account = 'pay_redteasz@redteamobile.com',0,amount))/100 as overseas_income1
  from
  dws.dws_Bethune_order
  WHERE type != '1'
  AND ((status not in ('0','1,''2')
        and source = 'order'
        and addHours(payment_time,8) >=toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
        and addHours(payment_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00')))
       or
       (status not in ('PAYMENT_CREATE','PENDING')
       and source = 'top_order'
       and addHours(payment_time,8) >=toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
       and addHours(payment_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))))
  group by order_month) t1
left join
  (select
  '多多流量宝' as company,
  toYYYYMM(addHours(update_time,8)) as order_month,
  '深圳红茶移动科技有限公司' as account_company,
  sum(if(account = '1605063756' or account = 'pay_redteasz@redteamobile.com',amount,0))/100 as churchyard_refund,
  sum(if(account = '1605063756' or account = 'pay_redteasz@redteamobile.com',0,amount))/100 as overseas_refund
  from
  dws.dws_Bethune_order
  WHERE type != '1'
  AND ((status = '7'
        and source = 'order'
        and addHours(update_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
        and addHours(update_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00')))
       or
       (status = 'REFUNDED'
       and source = 'top_order'
       and addHours(update_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
       and addHours(update_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))))
  group by order_month) t2
on t1.company = t2.company and t1.order_month = t2.order_month and t1.account_company = t2.account_company) t
where t.order_month != '197001'

union all

select
total.company,
total.order_month,
total.account,
toDecimal32(0,3) as wechat_income,
toDecimal32(overseas_income,3) as other_income,
toDecimal32(0,3) as agent_revenue,
toDecimal32(0,3) as wechat_fee,
toDecimal32(overseas_fee,3) as other_fee
from
(select
t2.company,
t2.order_month,
t2.account,
(t2.overseas_income + wallet.sales) as overseas_income,
(t2.overseas_income + wallet.sales) * 0.008 as overseas_fee
from
(select
t.company,
t.order_month,
t.account,
(t.total_income-t1.refund_amount) as overseas_income
from
  (select
  'RedteaGO' as company,
  'REDTEA MOBILE PTE. LTD.' as account,
  case when status in ('1','2','3','4') and product_type = 'order'
        and payment_method_id not in ('0') then toYYYYMM(addHours(payment_time,8))
       when status in ('1','2','3','4') and product_type = 'order'
        and payment_method_id in ('0')  then toYYYYMM(addHours(create_time,8))
       when status = 'SUCCESS' and product_type = 'topup_order'
        and payment_method_id not in ('0') then toYYYYMM(addHours(payment_time,8))
       else toYYYYMM(addHours(create_time,8)) end as order_month,
  sum(total_order_CNYamount) as total_income
  from
  dws.dws_Nobel_order
  where status in ('1','2','3','4','SUCCESS')
  and payment_method_id != 5
  and order_price >= 10000
  AND ((status in ('1','2','3','4') and product_type = 'order'
        and payment_method_id not in ('0')
        and addHours(payment_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
        and addHours(payment_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00')))
       or
       (status in ('1','2','3','4') and product_type = 'order'
        and payment_method_id = '0'
        and addHours(payment_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
        and addHours(create_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00')))
       or
       (status = 'SUCCESS' and product_type = 'topup_order'
        and payment_method_id not in ('0')
        and addHours(payment_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
        and addHours(payment_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00')))
       or
       (status = 'SUCCESS' and product_type = 'topup_order'
        and payment_method_id = '0'
        and addHours(payment_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
        and addHours(create_time,8) < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00'))))
  group by order_month) t
  left join
  (select
    'RedteaGO' as company,
    toYYYYMM(addHours(update_time,8)) as refund_month,
    'REDTEA MOBILE PTE. LTD.' as account,
    sum(total_order_CNYamount) as refund_amount
   from dws.dws_Nobel_order
   where addHours(payment_time,8) >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00'))
   and status = '3' and payment_method_id != 5
   group by refund_month) t1
on t.company = t1.company and t.order_month = t1.refund_month and t.account = t1.account) t2
left join
dws.dws_Nobel_wallet wallet on t2.order_month = wallet.transaction_month) total
order by total.order_month;
"