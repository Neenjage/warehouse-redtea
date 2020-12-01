#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
,import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
create table dwd.dwd_Nobel_user_wallet_detail
Engine=MergeTree
order by user_id as
select
user_id,
transaction_month,
sum(if(t.total_actual_amount = 0,0,if(total_actual_amount >= total_buy_amount-total_refund_amount,buy_amount-refund_amount,actual_amount))) as sales_amount
from
(select
t1.*,
t2.total_actual_amount,
t2.total_buy_amount,
t2.total_share_amount,
t2.total_refund_amount
from
(SELECT
    t.user_id,
    toYYYYMM(addHours(t.update_time,8)) as transaction_month,
    sum(if(t.biz_type = 'RECHARGE', order_price, 0)*t.currency_rate) / 10000 AS actual_amount,
    0-sum(if(t.biz_type = 'PURCHASE', amount, 0)*t.currency_rate) / 10000 AS buy_amount,
    sum(if(t.biz_type = 'REWARD', amount, 0)*t.currency_rate) / 10000 AS share_amount,
    sum(if(t.biz_type = 'REFUND', amount, 0)*t.currency_rate) / 10000 AS refund_amount
FROM
(
    SELECT
        wallet.*,
        if(currency_rate.USD_rate = 0, 6.684602, currency_rate.USD_rate) AS currency_rate
    FROM
    (
        SELECT
            wallet.*,
            topup_package.order_price
        FROM
        (
            SELECT
                *,
                if(isNull(sub_biz_id), biz_id, sub_biz_id) AS biz_no
            FROM ods.ods_Nobel_user_wallet_transaction
            WHERE (user_id NOT IN (0, 1, 7, 8, 137, 346, 360, 600, 880, 918, 940, 1085, 1098, 1209, 1215, 1239, 1357, 1418, 1480, 1507, 1560, 1638, 4243, 18522, 20194, 26475, 30578)) AND (update_time < '2020-10-31 16:00:00')
        ) AS wallet
        LEFT JOIN
        (
            SELECT *
            FROM ods.ods_Nobel_topup_package_order
            WHERE user_id NOT IN (0, 1, 7, 8, 137, 346, 360, 600, 880, 918, 940, 1085, 1098, 1209, 1215, 1239, 1357, 1418, 1480, 1507, 1560, 1638, 4243, 18522, 20194, 26475, 30578)
        ) AS topup_package ON wallet.biz_no = topup_package.order_no
    ) AS wallet
    LEFT JOIN
    (
        SELECT
            USD_rate,
            toDateTime(concat(toString(import_time), ' 00:00:00')) AS import_time
        FROM dim.dim_Bumblebee_currency_rate
        WHERE name = 'CNY'
    ) AS currency_rate ON toStartOfDay(addHours(wallet.update_time, 8)) = currency_rate.import_time
) AS t
group by user_id,transaction_month
order by user_id,transaction_month) t1
left join

(SELECT
    user_id,
    sum(if(biz_type = 'RECHARGE', order_price, 0)) / 10000 AS total_actual_amount,
    0-sum(if(biz_type = 'PURCHASE', amount, 0)) / 10000 AS total_buy_amount,
    sum(if(biz_type = 'REWARD', amount, 0)) / 10000 AS total_share_amount,
    sum(if(biz_type = 'REFUND', amount, 0)) / 10000 AS total_refund_amount
FROM
(
    SELECT
        wallet.*,
        topup_package.order_price
    FROM
    (
        SELECT
            *,
            if(isNull(sub_biz_id), biz_id, sub_biz_id) AS biz_no
        FROM ods.ods_Nobel_user_wallet_transaction
        WHERE user_id NOT IN (0,1, 7, 8, 137, 346, 360, 600, 880, 918, 940, 1085, 1098, 1209, 1215, 1239, 1357, 1418, 1480, 1507, 1560, 1638, 4243, 18522, 20194, 26475, 30578)
    ) AS wallet
    LEFT JOIN
    (
        SELECT *
        FROM ods.ods_Nobel_topup_package_order
        WHERE user_id NOT IN (0,1, 7, 8, 137, 346, 360, 600, 880, 918, 940, 1085, 1098, 1209, 1215, 1239, 1357, 1418, 1480, 1507, 1560, 1638, 4243, 18522, 20194, 26475, 30578)
    ) AS topup_package ON wallet.biz_no = topup_package.order_no
) AS t
group by user_id
order by user_id) t2
on t1.user_id = t2.user_id) t
group by user_id,transaction_month;
"
