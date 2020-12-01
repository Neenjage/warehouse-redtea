#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Bethune_order_detail_tmp;

CREATE TABLE dwd.dwd_Bethune_order_detail_tmp
ENGINE = MergeTree
ORDER BY id AS
SELECT
    t3.*,
    Mammon_payment_order.status as payment_status,
    Mammon_payment_order.update_time AS payment_time
FROM
(
    SELECT
        t2.*,
        payment_order_info.payment_order_id
    FROM
    (
        SELECT
            t1.*,
            data_plan.name AS data_plan_name
        FROM
        (
            SELECT
                orders.*,
                orders_device.payment_method,
                orders_device.model,
                orders_device.brand,
                orders_device.user_ip
            FROM
            (
                SELECT
                    id,
                    user_id,
                    data_plan_id,
                    imei,
                    device_id,
                    order_no,
                    create_time,
                    update_time,
                    count,
                    amount,
                    type,
                    status,
                    resource_order_id AS Einstein_order_id
                FROM ods.ods_Bethune_orders
                WHERE invalid_time = '2105-12-31 23:59:59'
            ) AS orders
            LEFT JOIN ods.ods_Bethune_orders_device AS orders_device ON orders.id = orders_device.order_id
        ) AS t1
        LEFT JOIN dim.dim_Bethune_data_plan AS data_plan ON t1.data_plan_id = data_plan.id
    ) AS t2
    LEFT JOIN ods.ods_Bethune_payment_order_info AS payment_order_info ON toString(t2.id) = payment_order_info.order_id
) AS t3
LEFT JOIN ods.ods_Mammon_payment_order AS Mammon_payment_order ON t3.payment_order_id = Mammon_payment_order.order_id
;

drop table if exists dwd.dwd_Bethune_order_detail;

rename table dwd.dwd_Bethune_order_detail_tmp to dwd.dwd_Bethune_order_detail;
"