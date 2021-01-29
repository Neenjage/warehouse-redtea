#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Bumblebee_bundle_detail_tmp;

CREATE TABLE dwd.dwd_Bumblebee_bundle_detail_tmp
ENGINE = MergeTree
ORDER BY bundle_id AS
SELECT
    t4.*,
    bundle_price.bundle_price
FROM
(
    SELECT
        t2.bundle_id,
        t2.bundle_name,
        t2.bundle_code,
        t2.bundle_data_volume,
        t2.bundle_location,
        t2.local_carrier_plmns,
        t2.local_carrier_price,
        t2.bundle_enable_time,
        t2.carrier_id,
        t2.carrier_name,
        t2.carrier_status,
        t2.channel_id,
        t2.channel_name,
        t2.channel_country,
        t3.bundle_group_id,
        t3.bundle_group_name
    FROM
    (
        SELECT
            t1.bundle_id AS bundle_id,
            t1.bundle_code AS bundle_code,
            t1.bundle_name AS bundle_name,
            t1.bundle_data_volume AS bundle_data_volume,
            t1.bundle_location AS bundle_location,
            t1.local_carrier_plmns,
            t1.local_carrier_price,
            t1.bundle_enable_time AS bundle_enable_time,
            t1.carrier_id AS carrier_id,
            t1.carrier_name AS carrier_name,
            t1.carrier_status AS carrier_status,
            channel.channel_id AS channel_id,
            channel.channel_name AS channel_name,
            channel.channel_country AS channel_country
        FROM
        (
            SELECT
                bundle.id AS bundle_id,
                bundle.code AS bundle_code,
                bundle.name AS bundle_name,
                bundle.data_volume AS bundle_data_volume,
                bundle.location AS bundle_location,
                bundle.local_carrier_plmns,
                bundle.local_carrier_price,
                bundle.enable_time AS bundle_enable_time,
                carrier.channel_id AS channel_id,
                carrier.carrier_id AS carrier_id,
                carrier.carrier_name AS carrier_name,
                carrier.carrier_status AS carrier_status
            FROM
            (
                SELECT
                    code,
                    max(id) AS id,
                    max(carrier_id) AS carrier_id,
                    max(name) AS name,
                    max(data_volume) AS data_volume,
                    max(location) AS location,
                    max(local_carrier_id) AS local_carrier_id,
                    max(local_carrier_plmn) AS local_carrier_plmns,
                    max(local_carrier_price) / 10000 AS local_carrier_price,
                    max(enable_time) AS enable_time
                FROM
                (
                    SELECT
                        t3.*,
                        t4.local_carrier_price
                    FROM
                    (
                        SELECT
                            t1.*,
                            t2.local_carrier_id
                        FROM
                        (
                            SELECT
                                id,
                                carrier_id,
                                code,
                                name,
                                data_volume,
                                location,
                                arrayJoin(splitByChar(',', CAST(if(isNotNull(local_carrier_plmns), local_carrier_plmns, plmns), 'String'))) AS local_carrier_plmn,
                                enable_time
                            FROM dim.dim_Bumblebee_bundle
                            WHERE (isNotNull(plmns) AND (plmns NOT IN (''))) OR (isNotNull(local_carrier_plmns) AND (local_carrier_plmns NOT IN ('')))
                        ) AS t1
                        LEFT JOIN
                        (
                          SELECT
                            local_carrier_id,
                            carrier_id,
                            multiIf(splitByChar('-',cast(local_carrier_name as String))[3] != '',splitByChar('-',cast(local_carrier_name as String))[3],
                                    splitByChar('-',cast(local_carrier_name as String))[2] != '',splitByChar('-',cast(local_carrier_name as String))[2],
                                    multiIf(location_code = 'GBRJT_Bundle', 'GBRJT',
                                           length(location_code) = 6, replaceAll(location_code, 'F', ''),
                                           length(location_code) > 6, substr(location_code, length(location_code) - 4, 5), location_code)) AS location_code
                          FROM dim.dim_Bumblebee_local_carrier
                        ) AS t2 ON (t1.carrier_id = t2.carrier_id) AND (t1.local_carrier_plmn = t2.location_code)
                    ) AS t3
                    LEFT JOIN
                    (
                        SELECT
                            local_carrier_id,
                            max(network_price) AS local_carrier_price
                        FROM dim.dim_Bumblebee_local_carrier_price_history
                        GROUP BY local_carrier_id
                    ) AS t4 ON t3.local_carrier_id = t4.local_carrier_id
                ) AS t
                GROUP BY t.code
            ) AS bundle
            LEFT JOIN
            (
                SELECT
                    id AS carrier_id,
                    channel_id,
                    name AS carrier_name,
                    status AS carrier_status
                FROM dim.dim_Bumblebee_carrier
            ) AS carrier ON bundle.carrier_id = carrier.carrier_id
        ) AS t1
        LEFT JOIN
        (
            SELECT
                id AS channel_id,
                channel_name,
                channel_country
            FROM dim.dim_Bumblebee_channel
        ) AS channel ON t1.channel_id = channel.channel_id
    ) AS t2
    LEFT JOIN
    (
        SELECT
            bgd.bundle_id AS bundle_id,
            bg.bundle_group_id AS bundle_group_id,
            bg.bundle_group_name AS bundle_group_name
        FROM
        (
            SELECT
                bundle_id,
                bundle_group_id
            FROM dim.dim_Bumblebee_bundle_group_bundle
        ) AS bgd
        LEFT JOIN
        (
            SELECT
                bundle_group_id,
                bundle_group_name
            FROM dim.dim_Bumblebee_bundle_group
        ) AS bg ON bgd.bundle_group_id = bg.bundle_group_id
    ) AS t3 ON t2.bundle_id = t3.bundle_id
) AS t4
LEFT JOIN
(
    SELECT
        bundle_code,
        max(price / 10000) AS bundle_price
    FROM dim.dim_Bumblebee_bundle_price
    WHERE id NOT IN (2)
    GROUP BY bundle_code
) AS bundle_price ON t4.bundle_code = bundle_price.bundle_code;

drop table if exists dwd.dwd_Bumblebee_bundle_detail;

rename table dwd.dwd_Bumblebee_bundle_detail_tmp to dwd.dwd_Bumblebee_bundle_detail;
"

