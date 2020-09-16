#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client -u$user --multiquery -q"
CREATE TABLE dws.dws_user_tmp
ENGINE = MergeTree
ORDER BY user_id AS
SELECT
    user.*,
    order.total_order,
    order.total_amount,
    order.total_usage,
    order.total_cost
FROM dwd.dwd_Nobel_users_detail AS user
LEFT JOIN
(
    SELECT
        order_temp2.user_id,
        sum(order_temp2.total_orders) AS total_order,
        sum(if(isNull(order_temp2.order_CNYamount), 0, order_temp2.order_CNYamount)) AS total_amount,
        sum(if(isNull(order_temp2.total_usage), 0, order_temp2.total_usage)) AS total_usage,
        sum(if(isNull(order_temp2.cost), 0, order_temp2.cost)) AS total_cost
    FROM
    (
        SELECT
            order_temp1.user_id,
            order_temp1.total_orders,
            order_temp1.order_CNYamount,
            cdr_raw.total_usage,
            cdr_raw.cost
        FROM
        (
            SELECT
                order_temp.*,
                itd.transaction_id
            FROM
            (
                SELECT
                    user_id,
                    total_orders,
                    order_CNYamount,
                    transaction_code
                FROM dwd.dwd_Nobel_orders_detail
                WHERE (invalid_time = '2105-12-31 23:59:59') AND (pay_status = 1)
            ) AS order_temp
            LEFT JOIN
            (
                SELECT
                    transaction_code,
                    transaction_id
                FROM dwd.dwd_Bumblebee_imsi_transaction_detail
            ) AS itd ON order_temp.transaction_code = itd.transaction_code
        ) AS order_temp1
        LEFT JOIN
        (
            SELECT
                cdr_raw.transaction_id,
                sum(cdr_raw.total_usage) AS total_usage,
                sum(cdr_raw.cost) AS cost
            FROM dwd.dwd_Bumblebee_imsi_transaction_cdr_raw AS cdr_raw
            WHERE cdr_raw.transaction_id != -1
            GROUP BY cdr_raw.transaction_id
        ) AS cdr_raw ON order_temp1.transaction_id = cdr_raw.transaction_id
    ) AS order_temp2
    GROUP BY order_temp2.user_id
) AS order ON user.user_id = order.user_id
"

clickhouse-client -u$user --multiquery -q"
drop table dws.dws_user
"

clickhouse-client -u$user --multiquery -q"
rename table dws.dws_user_tmp to dws.dws_user
"

