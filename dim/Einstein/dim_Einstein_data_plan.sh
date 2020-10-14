#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Einstein_data_plan
(
    id Int32,
    short_name Nullable(String),
    name Nullable(String),
    price Nullable(Int32),
    status Nullable(String),
    update_time Nullable(DateTime),
    duration Nullable(Int32),
    data_volume Nullable(Int32),
    description Nullable(String),
    promo_price Nullable(Int32),
    purchased_count Nullable(Int32),
    max_days Nullable(Int32),
    expiration_days Nullable(Int32),
    promo_banner_url Nullable(String),
    location_id Nullable(Int32),
    min_days Nullable(Int32),
    is_white Nullable(Int8),
    hplmn Nullable(String),
    rplmn Nullable(String),
    fplmn Nullable(String),
    rat Nullable(Int8),
    mcc_whith_list Nullable(String),
    daily_inventory Nullable(Int32),
    day_sales Nullable(Int32),
    tariff Nullable(String),
    data_plan_level Nullable(Int32),
    spn Nullable(String),
    currency_id Nullable(Int32),
    type Nullable(Int32),
    pluto Float32,
    mcc_mnc Nullable(String),
    sort_no Nullable(Int32),
    is_visible Nullable(Int32),
    short_description Nullable(String),
    tags Nullable(String),
    need_volume_control Nullable(Int8),
    promotion_id Nullable(Int32),
    upgrade_price Nullable(Int32),
    mcc Nullable(String),
    is_shipping_activate Nullable(Int8),
    description_tags Nullable(String),
    import_time Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(import_time)
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
ALTER TABLE ods_Einstein.data_plan DELETE WHERE import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dim.dim_Einstein_data_plan
SELECT
    id,
    short_name,
    name,
    price,
    status,
    update_time,
    duration,
    data_volume,
    description,
    promo_price,
    purchased_count,
    max_days,
    expiration_days,
    promo_banner_url,
    location_id,
    min_days,
    is_white,
    hplmn,
    rplmn,
    fplmn,
    rat,
    mcc_whith_list,
    daily_inventory,
    day_sales,
    tariff,
    data_plan_level,
    spn,
    currency_id,
    type,
    pluto,
    mcc_mnc,
    sort_no,
    is_visible,
    short_description,
    tags,
    need_volume_control,
    promotion_id,
    upgrade_price,
    mcc,
    is_shipping_activate,
    description_tags,
    '$import_time'
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'data_plan', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')"
