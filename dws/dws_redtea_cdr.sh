#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline --max_memory_usage 30000000000 -q"
drop table if exists dws.dws_redtea_cdr_tmp;

CREATE TABLE dws.dws_redtea_cdr_tmp
ENGINE = MergeTree
ORDER BY transaction_id AS
SELECT
    t4.*,
    local_carrier.location_id,
    local_carrier.location_name,
    local_carrier.local_carrier_id,
    local_carrier.local_carrier_name,
    local_carrier.plmn
FROM
(
    SELECT
        t3.*,
        tmp2.order_id,
        tmp2.agent_id,
        tmp2.agent_name,
        tmp2.data_plan_id,
        tmp2.data_plan_name
    FROM
    (
        SELECT
            t2.*,
            bd.bundle_id,
            bd.bundle_name,
            bd.bundle_group_id,
            bd.carrier_id,
            bd.carrier_name
        FROM
        (
            SELECT
                t1.*,
                if(itd.transaction_code = '', '-1', itd.transaction_code) AS transaction_code,
                itd.iccid,
                itd.merchant_name
            FROM
            (
                SELECT
                    transaction_id,
                    imsi,
                    bundle_code,
                    merchant_id,
                    country,
                    carrier,
                    plmn AS plmn_code,
                    location_code,
                    start_time,
                    end_time,
                    total_upload,
                    total_download,
                    total_usage,
                    unit_price,
                    cost,
                    cdr_date
                FROM dwd.dwd_Bumblebee_imsi_transaction_cdr_raw
            ) AS t1
            LEFT JOIN dwd.dwd_Bumblebee_imsi_transaction_detail AS itd ON t1.transaction_id = itd.transaction_id
        ) AS t2
        LEFT JOIN
        (
            SELECT
                bundle_id,
                bundle_code,
                bundle_group_id,
                bundle_name,
                carrier_id,
                carrier_name
            FROM dwd.dwd_Bumblebee_bundle_detail
        ) AS bd ON t2.bundle_code = bd.bundle_code
    ) AS t3
    LEFT JOIN
    (
        SELECT
            tmp1.transaction_code,
            toString(tmp1.order_id) AS order_id,
            toInt32(tmp1.agent_id) AS agent_id,
            tmp1.agent_name,
            tmp1.data_plan_id,
            dpd.data_plan_name
        FROM
        (
            SELECT
                oipr.transaction_code,
                oipr.order_id,
                od.agent_id,
                od.agent_name,
                od.data_plan_id
            FROM dwd.dwd_Einstein_order_imsi_profile_relation AS oipr
            LEFT JOIN
            (
                SELECT
                    order_id,
                    agent_id,
                    agent_name,
                    data_plan_id
                FROM dwd.dwd_Einstein_orders_detail
                WHERE invalid_time = '2105-12-31 23:59:59'
            ) AS od ON oipr.order_id = od.order_id
        ) AS tmp1
        LEFT JOIN
        (
            SELECT
                data_plan_id,
                data_plan_name
            FROM dwd.dwd_Einstein_data_plan_detail
        ) AS dpd ON tmp1.data_plan_id = toString(dpd.data_plan_id)
        UNION ALL
        SELECT
            od.transaction_code,
            od.order_id,
            toInt32(0) AS agent_id,
            'redtea_go' AS agent_name,
            toString(od.day_client_resource_id) AS data_plan_id,
            toString(dpd.data_plan_volume) AS data_plan_name
        FROM
        (
            SELECT
                order_id,
                transaction_code,
                day_client_resource_id
            FROM dwd.dwd_Nobel_orders_detail
            WHERE invalid_time = '2105-12-31 23:59:59'
        ) AS od
        LEFT JOIN
        (
            SELECT
                day_client_resource_id,
                data_plan_volume
            FROM dwd.dwd_Nobel_data_plan_detail
        ) AS dpd ON od.day_client_resource_id = dpd.day_client_resource_id
    ) AS tmp2 ON t3.transaction_code = tmp2.transaction_code
) AS t4
LEFT JOIN
(
    SELECT *
    FROM dwd.dwd_Bumblebee_local_carrier_detail
) AS local_carrier ON (t4.location_code = local_carrier.location_code) AND (toString(t4.bundle_group_id) = toString(local_carrier.bundle_group_id));

drop table if exists dws.dws_redtea_cdr;

rename table dws.dws_redtea_cdr_tmp to dws.dws_redtea_cdr;
"
