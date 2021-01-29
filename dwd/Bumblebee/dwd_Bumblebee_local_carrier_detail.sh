#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Bumblebee_local_carrier_detail_tmp;

CREATE TABLE dwd.dwd_Bumblebee_local_carrier_detail_tmp
ENGINE = MergeTree
ORDER BY local_carrier_id AS
SELECT
    local_carrier.*,
    local_carrier_info.plmn AS plmn,
    local_carrier_info.mcc AS mcc,
    local_carrier_info.mnc AS mnc
FROM
(
    SELECT
        ab.local_carrier_id,
        ab.location_id,
        multiIf(ab.location_code = 'GBRJT_Bundle', 'GBRJT', length(ab.location_code) = 6, replaceAll(location_code, 'F', ''), length(ab.location_code) > 6, substr(ab.location_code, length(ab.location_code) - 4, 5), ab.location_code) AS location_code,
        ab.carrier_id,
        ab.carrier_name,
        ab.local_carrier_info_id,
        ab.local_carrier_name,
        ab.location_name,
        ab.create_time,
        ab.last_update_time,
        ab.status,
        ab.detail,
        ab.tadig,
        ab.bundle_group_id,
        ab.bundle_group_name,
        ac.local_carrier_price,
        import_time
    FROM
    (
        SELECT *
        FROM dim.dim_Bumblebee_local_carrier
    ) AS ab
    ALL INNER JOIN
    (
        SELECT
            t1.max_local_carrier_id,
            t2.local_carrier_price
        FROM
        (
            SELECT max(local_carrier_id) AS max_local_carrier_id
            FROM
            (
                SELECT
                    multiIf(location_code = 'GBRJT_Bundle', 'GBRJT', length(location_code) = 6, replaceAll(location_code, 'F', ''), length(location_code) > 6, substr(location_code, length(location_code) - 4, 5), location_code) AS location_code,
                    bundle_group_id,
                    local_carrier_id
                FROM dim.dim_Bumblebee_local_carrier
            ) AS tmp
            GROUP BY
                bundle_group_id,
                location_code
        ) AS t1
        LEFT JOIN
        (
            SELECT
                local_carrier_id,
                max(network_price)/10000 AS local_carrier_price
            FROM dim.dim_Bumblebee_local_carrier_price_history
            WHERE status = 1
            GROUP BY local_carrier_id
        ) AS t2 ON t1.max_local_carrier_id = t2.local_carrier_id
    ) AS ac ON ab.local_carrier_id = ac.max_local_carrier_id
    WHERE isNotNull(max_local_carrier_id) AND isNotNull(location_code)
) AS local_carrier
LEFT JOIN
(
    SELECT
        id,
        plmn,
        mcc,
        mnc
    FROM dim.dim_Bumblebee_local_carrier_info
) AS local_carrier_info ON local_carrier.local_carrier_info_id = local_carrier_info.id;

drop table if exists dwd.dwd_Bumblebee_local_carrier_detail;

rename table dwd.dwd_Bumblebee_local_carrier_detail_tmp to dwd.dwd_Bumblebee_local_carrier_detail;
"