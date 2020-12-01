#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Nobel_topup_orders_detail_tmp;

create table dwd.dwd_Nobel_topup_orders_detail_tmp
Engine=MergeTree
order by dpo_order_no as
select
  t4.*,
  if(currency.CNY_rate = 0,0.14959754,currency.CNY_rate) AS currency_CNY_rate,
  currency.name AS currency_name,
  currency.remark AS currency_remark,
  if(((t4.order_price / 10000) / currency_CNY_rate) = inf, 0, (t4.order_price / 10000) / currency_CNY_rate) AS order_CNYamount
from
(SELECT
    t3.*,
    Mammon_payment.status AS payment_status,
    Mammon_payment.update_time AS payment_time
FROM
(
    SELECT
        t2.*,
        payment_order.payment_order_id
    FROM
    (
        SELECT
            t1.*,
            pay_order.pay_order_no,
            pay_order.payment_methods_id
        FROM
        (
            SELECT
                dpo_order_no,
                order_no,
                currency_id,
                data_volume,
                create_time,
                update_time,
                source_type,
                order_price,
                top_up_status
            FROM ods.ods_Nobel_data_plan_topup_order
            WHERE (top_up_status = 'SUCCESS') AND (invalid_time = '2105-12-31 23:59:59')
        ) AS t1
        LEFT JOIN ods.ods_Nobel_pay_order_info AS pay_order ON t1.order_no = pay_order.biz_order_no
    ) AS t2
    LEFT JOIN
    (
        SELECT *
        FROM ods.ods_Nobel_payment_order_info
        WHERE goods_type = 'PAY_ORDER'
    ) AS payment_order ON toString(t2.pay_order_no) = payment_order.order_id
) AS t3
LEFT JOIN ods.ods_Mammon_payment_order AS Mammon_payment ON t3.payment_order_id = Mammon_payment.order_id)
t4 left join
    (
        SELECT
            currency_name.id,
            currency_name.name,
            currency_name.remark,
            currency_rate.CNY_rate,
            currency_rate.import_time
        FROM
        (
            SELECT
                id,
                name,
                remark
            FROM dim.dim_Nobel_currency
            WHERE import_time = '$import_time'
        ) AS currency_name
        LEFT JOIN
        (
            SELECT
                name,
                CNY_rate,
                toDateTime(concat(toString(import_time), ' 00:00:00')) AS import_time
            FROM dim.dim_Bumblebee_currency_rate
        ) AS currency_rate ON currency_name.name = currency_rate.name
    ) AS currency ON (t4.currency_id = currency.id) AND (toStartOfDay(t4.payment_time) = currency.import_time)
;

drop table if exists dwd.dwd_Nobel_topup_orders_detail;

rename table dwd.dwd_Nobel_topup_orders_detail_tmp to dwd.dwd_Nobel_topup_orders_detail;
"
