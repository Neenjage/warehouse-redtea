#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists dwd.dwd_Einstein_orders_detail_tmp;

CREATE TABLE dwd.dwd_Einstein_orders_detail_tmp
ENGINE=MergeTree
ORDER BY order_id AS
SELECT
    t7.*,
    order_channel.type AS channel_type,
    'Einstein' as order_source
FROM
(
    SELECT
        t6.*,
        currency.CNY_rate as currency_CNY_rate,
        currency.name AS currency_name,
        currency.remark AS currency_remark,
        if(t6.order_amount/100/currency.CNY_rate=inf,0,t6.order_amount/100/currency.CNY_rate) as order_CNYamount
    FROM
    (
        SELECT
            t5.*,
            payment_methods.name AS payment_method_name
        FROM
        (
            SELECT
                t4.*,
                provider.name AS provider_name
            FROM
            (
                SELECT
                    t3.*,
                    agent.name AS agent_name,
                    agent.status AS agent_status
                FROM
                (
                    SELECT
                        t2.*,
                        volume.volume_usage AS volume_usage
                    FROM
                    (
                        SELECT
                            t1.*,
                            ipaddress.ip AS order_ip,
                            ipaddress.province AS order_province,
                            ipaddress.address AS order_address
                        FROM
                        (
                            SELECT
                                orders.id AS order_id,
                                orders.order_no AS order_no,
                                orders.device_id AS device_id,
                                orders.data_plan_id AS data_plan_id,
                                orders.count AS order_count,
                                orders.order_time AS order_time,
                                orders.status AS order_status,
                                orders.update_time AS update_time,
                                orders.activate_time AS activate_time,
                                orders.imsi AS imsi,
                                orders.amount AS order_amount,
                                orders.agent_id AS agent_id,
                                orders.provider_id AS provider_id,
                                orders.payment_method_id AS payment_method_id,
                                orders.expiration_time AS expiration_time,
                                orders.currency_id AS currency_id,
                                orders.refund_reason AS refund_reason,
                                orders.channel_id AS channel_id,
                                orders.effective_time AS effective_time,
                                orders.invalid_time AS invalid_time,
                                appraises.purchase_score AS purchase_score,
                                appraises.network_stability_score AS network_stability_score,
                                appraises.internet_speed_score AS internet_speed_score
                            FROM
                            (
                                SELECT
                                    id,
                                    order_no,
                                    device_id,
                                    data_plan_id,
                                    count,
                                    order_time,
                                    status,
                                    update_time,
                                    activate_time,
                                    imsi,
                                    amount,
                                    agent_id,
                                    provider_id,
                                    payment_method_id,
                                    expiration_time,
                                    uid,
                                    if(currency_id is null,1,currency_id) as currency_id,
                                    refund_reason,
                                    channel_id,
                                    effective_time,
                                    invalid_time
                                FROM ods.ods_Einstein_orders
                            ) AS orders
                            LEFT JOIN
                            (
                                SELECT
                                    order_id,
                                    purchase_score,
                                    network_stability_score,
                                    internet_speed_score
                                FROM ods.ods_Einstein_order_appraises
                            ) AS appraises ON orders.id = appraises.order_id
                        ) AS t1
                        LEFT JOIN
                        (
                            SELECT
                                order_no,
                                ip,
                                province,
                                address
                            FROM ods.ods_Einstein_order_ipaddress
                        ) AS ipaddress ON t1.order_no = ipaddress.order_no
                    ) AS t2
                    LEFT JOIN
                    (
                        SELECT
                           order_id,
                           MAX(volume_usage) AS volume_usage
                         FROM ods.ods_Einstein_order_volume
                         GROUP BY order_id
                    ) AS volume ON t2.order_id = volume.order_id
                ) AS t3
                LEFT JOIN
                (
                    SELECT
                        id,
                        name,
                        status
                    FROM dim.dim_Einstein_agent
                    WHERE import_time = '$import_time'
                ) AS agent ON t3.agent_id = agent.id
            ) AS t4
            LEFT JOIN
            (
                SELECT
                    id,
                    name
                FROM dim.dim_Einstein_provider
                WHERE import_time = '$import_time'
            ) AS provider ON t4.provider_id = provider.id
        ) AS t5
        LEFT JOIN
        (
            SELECT
                id,
                name
            FROM dim.dim_Einstein_payment_methods
            WHERE import_time = '$import_time'
        ) AS payment_methods ON t5.payment_method_id = payment_methods.id
    ) AS t6
    LEFT JOIN
    (
        SELECT
          currency_name.id,
          currency_name.name,
          currency_name.remark,
          currency_rate.CNY_rate
        FROM
            (SELECT
                id,
                name,
                remark
            FROM dim.dim_Einstein_currency
            WHERE import_time = '$import_time') currency_name
        left join
            (select
              name,
              CNY_rate
            FROM dim.dim_Bumblebee_currency_rate
            WHERE import_time = '$import_time') currency_rate
        ON currency_name.name = currency_rate.name
    ) AS currency ON t6.currency_id = currency.id
) AS t7
LEFT JOIN
(
    SELECT
        id,
        type
    FROM dim.dim_Einstein_order_channel
    WHERE import_time = '$import_time'
) AS order_channel ON t7.channel_id = order_channel.id;

drop table if exists dwd.dwd_Einstein_orders_detail;

rename table dwd.dwd_Einstein_orders_detail_tmp to dwd.dwd_Einstein_orders_detail;
"





