#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi


#dwd.dwd_Einstein_order_detail查询条件中order_amount != 0 表示过滤掉了免费流量的订单即也关联不到免费订单的话单表，即没有免费订单的成本

clickhouse-client --user $user --password $password --multiquery --multiline  --max_memory_usage 30000000000 -q"
drop table if exists dws.dws_redtea_user_tmp;

CREATE TABLE dws.dws_redtea_user_tmp
ENGINE = MergeTree
ORDER BY user_id AS
SELECT
    toString(user.user_id) AS user_id,
    user.source,
    user.model AS brand,
    user.model,
    user.user_status,
    0 as agent_id,
    'unknown' AS agent_name,
    'unknown' AS app_version,
    'RTG' AS residence,
    user.email,
    user.register_time,
    user.last_login_time,
    order.last_order_time,
    if(isNull(order.total_order), 0, order.total_order) AS total_orders,
    if(isNull(order.total_amount), 0, order.total_amount) AS total_amount,
    if(isNull(order.total_cost), 0, order.total_cost) AS total_cost
FROM dwd.dwd_Nobel_users_detail AS user
LEFT JOIN
(
    SELECT
        order_temp2.user_id,
        sum(order_temp2.total_orders) AS total_order,
        sum(if(isNull(order_temp2.order_CNYamount), 0, order_temp2.order_CNYamount)) AS total_amount,
        sum(if(isNull(order_temp2.total_usage), 0, order_temp2.total_usage)) AS total_usage,
        sum(if(isNull(order_temp2.cost), 0, order_temp2.cost)) AS total_cost,
        max(order_temp2.order_time) AS last_order_time
    FROM
    (
        SELECT
            order_temp1.user_id,
            order_temp1.total_orders,
            order_temp1.order_CNYamount,
            order_temp1.order_time,
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
                    create_time AS order_time,
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
UNION ALL
SELECT
    device.device_id AS user_id,
    'Einstein' AS source,
    device.brand,
    device.model,
    'ACTIVE' AS user_status,
    device.agent_id as agent_id,
    multiIf(device.agent_id = 1, 'Vivo',
            device.agent_id = 2, 'Smartisan',
            device.agent_id = 3, 'LeTV',
            device.agent_id = 4, 'ZUK',
            device.agent_id = 5, if((brand = 'SUGAR') OR (brand = 'Hisense') OR (brand = 'Nokia') OR (brand = 'SMARTISAN'), brand, 'Redtea'),
            device.agent_id = 6, 'Nubia',
            device.agent_id = 7, 'Infinix',
            device.agent_id = 9, 'OPPO',
            device.agent_id = 10, 'ZTE MIFI',
            device.agent_id = 11, 'One-plus',
            device.agent_id = 12, 'moto',
            device.agent_id = 13, 'Lenovo-domestic',
            device.agent_id = 14, 'Vivo_aboard', '其它') AS agent_name,
    device.app_version,
    device.residence,
    'unknown' AS email,
    device.register_time,
    device.last_login_time,
    order_detail.last_order_time,
    order_detail.order_number AS total_orders,
    if(isNull(order_detail.toal_amount), 0, order_detail.toal_amount) AS toal_amount,
    if(isNull(order_detail.total_cost), 0, order_detail.total_cost) AS total_cost
FROM dwd.dwd_Einstein_device_detail AS device
LEFT JOIN
(
    SELECT
        total.device_id,
        count(device_id) AS order_number,
        sum(if(isNull(total.order_CNYamount), 0, total.order_CNYamount)) AS toal_amount,
        sum(if(isNotNull(total.bundle_price), total.bundle_price, if(isNotNull(total.cost), total.cost, 0))) AS total_cost,
        max(total.order_time) AS last_order_time
    FROM
    (
        SELECT
            order_transcation.device_id,
            order_transcation.order_CNYamount,
            order_transcation.order_time,
            order_transcation.bundle_price,
            cdr.cost
        FROM
        (
            SELECT
                order_imsi.order_id,
                order_imsi.device_id,
                order_imsi.order_CNYamount,
                order_imsi.order_time,
                order_imsi.transaction_id,
                order_imsi.bundle_code,
                bundle_detail.bundle_price
            FROM
            (
                SELECT
                    order.order_id,
                    order.device_id,
                    order.order_CNYamount,
                    order.order_time,
                    relation.transaction_id,
                    relation.bundle_code
                FROM
                (
                    SELECT
                        order_id,
                        device_id,
                        order_CNYamount,
                        order_time
                    FROM dwd.dwd_Einstein_orders_detail
                    WHERE (invalid_time = '2105-12-31 23:59:59')
                    AND (order_status IN ('ACTIVATED', 'EXPIRED', 'PURCHASED', 'OBSOLETE', 'USEDUP'))
                    AND (order_amount != 0)
                ) AS order
                LEFT JOIN dwd.dwd_Einstein_order_imsi_profile_relation AS relation ON order.order_id = relation.order_id
            ) AS order_imsi
            LEFT JOIN dwd.dwd_Bumblebee_bundle_detail AS bundle_detail ON order_imsi.bundle_code = bundle_detail.bundle_code
        ) AS order_transcation
        LEFT JOIN
        (SELECT
          transaction_id,
          sum(cost) as cost
        FROM dwd.dwd_Bumblebee_imsi_transaction_cdr_raw
        GROUP BY transaction_id) AS cdr ON order_transcation.transaction_id = cdr.transaction_id
    ) AS total
    GROUP BY total.device_id
) AS order_detail ON order_detail.device_id = device.device_id
UNION ALL
SELECT
    toString(user.user_id) AS user_id,
    user.source,
    user.brand,
    user.model,
    user.user_status,
    0 as agent_id,
    'unknown' AS agent_name,
    'unknown' AS app_version,
    'CN' AS residence,
    user.email,
    user.register_time,
    user.last_login_time,
    order.last_order_time,
    if(isNull(order.total_orders), 0, order.total_orders) AS total_orders,
    if(isNull(order.total_amount), 0, order.total_amount) AS total_amount,
    if(isNull(order.total_cost), 0, order.total_cost) AS total_cost
FROM
(
    SELECT
        user_id,
        'Bethune' AS source,
        brand,
        model,
        user_status,
        'unknown' AS email,
        create_time AS register_time,
        login_time AS last_login_time
    FROM dwd.dwd_Bethune_user_detail
) AS user
LEFT JOIN
(
    SELECT
        total.user_id,
        sum(if(amount = 0, 0, 1)) AS total_orders,
        sum(total.amount) AS total_amount,
        sum(total.cost) AS total_cost,
        max(total.create_time) AS last_order_time
    FROM
    (
        SELECT
            order2.user_id,
            order2.amount,
            order2.create_time,
            if(isNull(cdr.cost), 0, cdr.cost) AS cost
        FROM
        (
            SELECT
                order1.user_id,
                order1.amount,
                order1.create_time,
                relation.transaction_id
            FROM
            (
                SELECT
                    order.user_id,
                    order.amount,
                    order.create_time,
                    Einstein_order.order_id
                FROM
                (
                    SELECT
                        user_id,
                        if(status = 2, 0, amount / 100) AS amount,
                        create_time,
                        Einstein_order_id
                    FROM dwd.dwd_Bethune_order_detail
                    WHERE status NOT IN (0, 3, 5)
                ) AS order
                LEFT JOIN
                (
                    SELECT
                        order_id,
                        order_no
                    FROM dwd.dwd_Einstein_orders_detail
                    WHERE invalid_time = '2105-12-31 23:59:59'
                ) AS Einstein_order ON order.Einstein_order_id = Einstein_order.order_no
            ) AS order1
            LEFT JOIN dwd.dwd_Einstein_order_imsi_profile_relation AS relation ON order1.order_id = relation.order_id
        ) AS order2
        LEFT JOIN
        (SELECT
          transaction_id,
          sum(cost) as cost
        FROM dwd.dwd_Bumblebee_imsi_transaction_cdr_raw
        GROUP BY transaction_id)AS cdr ON order2.transaction_id = cdr.transaction_id
    ) AS total
    GROUP BY total.user_id
) AS order ON user.user_id = order.user_id;

drop table if exists dws.dws_redtea_user;

rename table dws.dws_redtea_user_tmp to dws.dws_redtea_user;
"