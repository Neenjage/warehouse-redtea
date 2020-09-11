#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi


clickhouse-client -u$user --multiquery -q"
CREATE TABLE IF NOT EXISTS dim.dim_Einstein_data_plan_price
(
    `id` Int32,
    `data_plan_id` Int32,
    `currency_id` Int32,
    `origin_price` Nullable(Int32),
    `price` Int32,
    `import_time` Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(import_time)
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
alter table dim.dim_Einstein_data_plan_price delete where import_time = '$import_time'
"

clickhouse-client -u$user --multiquery -q"
INSERT INTO dim.dim_Einstein_data_plan_price
SELECT
    id,
    data_plan_id,
    currency_id,
    origin_price,
    price,
    '$import_time'
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'data_plan_price', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')"
