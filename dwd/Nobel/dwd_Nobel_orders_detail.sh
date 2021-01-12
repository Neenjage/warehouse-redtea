#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Nobel_orders_detail_tmp;

CREATE TABLE dwd.dwd_Nobel_orders_detail_tmp
ENGINE = MergeTree
ORDER BY order_id AS
SELECT 
    t8.*,
    payment.name AS payment_method_name
FROM 
(
    SELECT 
        t7.*,
        if(currency.CNY_rate = 0,0.14959754,currency.CNY_rate) AS currency_CNY_rate,
        currency.name AS currency_name,
        currency.remark AS currency_remark,
        if(((t7.order_price / 10000) / currency_CNY_rate) = inf, 0, (t7.order_price / 10000) / currency_CNY_rate) AS order_CNYamount
    FROM 
    (
        SELECT 
            t6.*,
            Mammon_payment.status AS payment_status,
            Mammon_payment.update_time AS payment_time
        FROM 
        (
            SELECT 
                t5.*,
                payment_order.payment_order_id
            FROM 
            (
                SELECT 
                    t4.*,
                    ip_address.ip,
                    ip_address.address,
                    ip_address.country,
                    ip_address.province,
                    ip_address.city
                FROM 
                (
                    SELECT 
                        t3.*,
                        user_device.model,
                        user_device.app_version
                    FROM 
                    (
                        SELECT 
                            t1.*,
                            user.id AS user_id
                        FROM 
                        (
                            SELECT 
                                dpo.id,
                                dpo.order_id AS order_id,
                                'Nobel' AS source,
                                0 AS agent_id,
                                'redtea_go' AS agent_name,
                                dpo.cid,
                                dpo.iccid,
                                dpo.start_time,
                                dpo.end_time,
                                dpo.order_price AS data_plan_order_price,
                                topup_order.order_price AS topup_order_price,
                                dpo.order_price + topup_order.order_price AS order_price,
                                topup_order.topup_order_count,
                                topup_order.topup_order_count + 1 AS total_orders,
                                dpo.create_time,
                                dpo.last_update_time,
                                dpo.email_box AS email,
                                dpo.resource_status,
                                dpo.resource_id,
                                dpo.location_name,
                                dpo.qr_resource_id,
                                dpo.source_type,
                                dpo.area_id,
                                dpo.data_plan_volume_id,
                                dpo.data_plan_day_id,
                                dpo.qr_iccid,
                                dpo.payment_methods_id AS payment_method_id,
                                dpo.currency_id,
                                dpo.status AS pay_status,
                                dpo.order_status,
                                dpo.day_client_resource_id,
                                dpo.qr_imsi AS imsi,
                                dpo.qr_transaction_id AS transaction_code,
                                dpo.device_id,
                                dpo.user_id,
                                dpo.effective_time,
                                dpo.invalid_time
                            FROM
                            (select
                              *
                            from
                            ods.ods_Nobel_data_plan_order
                            where create_time > '2020-02-22 23:59:59') AS dpo
                            LEFT JOIN 
                            (
                                SELECT 
                                    dpo_order_no,
                                    sum(order_price) AS order_price,
                                    count(*) AS topup_order_count
                                FROM ods.ods_Nobel_data_plan_topup_order
                                WHERE (top_up_status = 'SUCCESS') AND (invalid_time = '2105-12-31 23:59:59')
                                GROUP BY dpo_order_no
                            ) AS topup_order ON dpo.order_id = topup_order.dpo_order_no
                        ) AS t1
                        LEFT JOIN 
                        (
                            SELECT *
                            FROM ods.ods_Nobel_users
                            WHERE (status = 'ACTIVE') AND (invalid_time = '2105-12-31 23:59:59')
                        ) AS user ON t1.email = user.email
                    ) AS t3
                    LEFT JOIN 
                    (
                        SELECT 
                            user_id,
                            max(model) AS model,
                            max(app_version) AS app_version
                        FROM ods.ods_Nobel_user_device
                        GROUP BY user_id
                    ) AS user_device ON t3.user_id = user_device.user_id
                ) AS t4
                LEFT JOIN ods.ods_Nobel_order_ip_address AS ip_address ON t4.order_id = ip_address.order_id
            ) AS t5
            LEFT JOIN 
            (
                SELECT *
                FROM ods.ods_Nobel_payment_order_info
                WHERE goods_type = 'DATA_PLAN'
            ) AS payment_order ON toString(t5.id) = payment_order.order_id
        ) AS t6
        LEFT JOIN ods.ods_Mammon_payment_order AS Mammon_payment ON t6.payment_order_id = Mammon_payment.order_id
    ) AS t7
    LEFT JOIN 
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
        ) AS currency_name
        LEFT JOIN 
        (
            SELECT 
                name,
                CNY_rate,
                toDateTime(concat(toString(import_time), ' 00:00:00')) AS import_time
            FROM dim.dim_Bumblebee_currency_rate
        ) AS currency_rate ON currency_name.name = currency_rate.name
    ) AS currency ON (t7.currency_id = currency.id) AND (toStartOfDay(t7.payment_time) = currency.import_time)
) AS t8
LEFT JOIN 
(
    SELECT *
    FROM dim.dim_Nobel_payment_methods
) AS payment ON t8.payment_method_id = payment.id;

drop table if exists dwd.dwd_Nobel_orders_detail;

rename table dwd.dwd_Nobel_orders_detail_tmp to dwd.dwd_Nobel_orders_detail;
"
