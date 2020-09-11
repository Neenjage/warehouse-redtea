#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client -u$user --multiquery -q"
CREATE TABLE IF NOT EXISTS dim.dim_Nobel_data_plan_day
(
    `id` Int32,
    `data_plan_volume_id` Int32,
    `day` Int32,
    `price` Int32,
    `status` String,
    `update_time` Nullable(DateTime),
    `create_time` Nullable(DateTime),
    `import_time` Date DEFAULT toDate(now())
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
ALTERT TABLE dim.dim_Nobel_data_plan_day delete where import_time = '$import_time'
"

clickhouse-client -u$user --multiquery -q"
INSERT INTO dim.dim_Nobel_data_plan_day
SELECT
    id,
    data_plan_volume_id,
    day,
    price,
    status,
    update_time,
    create_time,
    '$import_time'
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'data_plan_day', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"

