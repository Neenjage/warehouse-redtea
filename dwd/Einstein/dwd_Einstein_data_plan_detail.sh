#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=date +%F

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dwd.dwd_Einstein_data_plan_detail
(
    data_plan_id Int32,
    data_plan_name Nullable(String),
    data_plan_price Nullable(Int32),
    data_plan_status Nullable(String),
    data_lan_update_time Nullable(DateTime),
    data_plan_volume Nullable(Int32),
    data_plan_expiration_days Nullable(Int32),
    location_id Nullable(Int32),
    currency_id Nullable(Int32),
    promotion_id Nullable(Int32),
    data_plan_level Nullable(Int32),
    location_name Nullable(String),
    location_continent Nullable(String),
    location_remark Nullable(String),
    location_status Nullable(String),
    currency_name String,
    currency_remark Nullable(String),
    title Nullable(String),
    status Nullable(String),
    start_time Nullable(DateTime),
    end_time Nullable(DateTime),
    data_plan_group_id Int32,
    data_plan_group_name String,
    provider_id Nullable(Int32),
    provider_name Nullable(String),
    import_time String
)
ENGINE = MergeTree
ORDER BY data_plan_id
SETTINGS index_granularity = 8192
"


clickhouse-client --user $user --password $password --multiquery --multiline -q"
alter table dwd.dwd_Einstein_data_plan_detail delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO TABLE dwd.dwd_Einstein_data_plan_detail
SELECT
    t5.*,
    t6.provider_id,
    t6.provider_name,
    '$import_time' AS import_time
FROM
(
    SELECT
        t3.*,
        t4.data_plan_group_id,
        t4.data_plan_group_name
    FROM
    (
        SELECT
            t2.*,
            promotions.title,
            promotions.status,
            promotions.start_time,
            promotions.end_time
        FROM
        (
            SELECT
                t1.*,
                currency.name AS currency_name,
                currency.remark AS currency_remark
            FROM
            (
                SELECT
                    data_plan.id AS data_plan_id,
                    data_plan.short_name AS data_plan_name,
                    data_plan.price AS data_plan_price,
                    data_plan.status AS data_plan_status,
                    data_plan.update_time AS data_lan_update_time,
                    data_plan.data_volume AS data_plan_volume,
                    data_plan.expiration_days AS data_plan_expiration_days,
                    data_plan.location_id,
                    data_plan.currency_id,
                    data_plan.promotion_id,
                    data_plan.data_plan_level,
                    location.name AS location_name,
                    location.continent AS location_continent,
                    location.remark AS location_remark,
                    location.status AS location_status
                FROM
                (
                    SELECT
                        id,
                        short_name,
                        price,
                        status,
                        update_time,
                        data_volume,
                        expiration_days,
                        location_id,
                        currency_id,
                        promotion_id,
                        data_plan_level
                    FROM dim.dim_Einstein_data_plan
                    WHERE import_time = '$import_time'
                ) AS data_plan
                LEFT JOIN
                (
                    SELECT
                        id,
                        name,
                        continent,
                        remark,
                        status
                    FROM dim.dim_Einstein_location
                    WHERE import_time = '$import_time'
                ) AS location ON data_plan.location_id = location.id
            ) AS t1
            LEFT JOIN
            (
                SELECT
                    id,
                    name,
                    remark
                FROM dim.dim_Einstein_currency
                WHERE import_time = '$import_time'
            ) AS currency ON t1.currency_id = currency.id
        ) AS t2
        LEFT JOIN
        (
            SELECT
                id,
                title,
                status,
                start_time,
                end_time
            FROM dim.dim_Einstein_promotions
            WHERE import_time = '$import_time'
        ) AS promotions ON t2.promotion_id = promotions.id
    ) AS t3
    LEFT JOIN
    (
        SELECT
            dprg.data_plan_id,
            dprg.data_plan_group_id,
            dpg.name AS data_plan_group_name
        FROM
        (
            SELECT
                data_plan_id,
                data_plan_group_id
            FROM dim.dim_Einstein_data_plan_rel_group
            WHERE import_time = '$import_time'
        ) AS dprg
        LEFT JOIN
        (
            SELECT
                id,
                name
            FROM dim.dim_Einstein_data_plan_group
            WHERE import_time = '$import_time'
        ) AS dpg ON dprg.data_plan_group_id = dpg.id
    ) AS t4 ON t3.data_plan_id = t4.data_plan_id
) AS t5
ANY LEFT JOIN
(
    SELECT
        dpp.data_plan_id,
        dpp.provider_id AS provider_id,
        provider.name AS provider_name
    FROM
    (
        SELECT
            data_plan_id,
            provider_id
        FROM dim.dim_Einstein_data_plan_provider
        WHERE import_time = '$import_time'
    ) AS dpp
    LEFT JOIN
    (
        SELECT
            id,
            name
        FROM dim.dim_Einstein_provider
        WHERE import_time = '$import_time'
    ) AS provider ON dpp.provider_id = provider.id
) AS t6 ON t5.data_plan_id = t6.data_plan_id
"



