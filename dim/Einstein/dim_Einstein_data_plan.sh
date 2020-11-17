#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Einstein_data_plan_tmp;

CREATE TABLE dim.dim_Einstein_data_plan_tmp
ENGINE = MergeTree
ORDER BY id as
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
    '$import_time' as import_time
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'data_plan', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

drop table if exists dim.dim_Einstein_data_plan;

rename table dim.dim_Einstein_data_plan_tmp to dim.dim_Einstein_data_plan;
"
