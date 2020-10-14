#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Nobel_data_plan_volume
(
    id Int32,
    area_id Int32,
    volume Int32,
    language_code String,
    resource_id Int32,
    data_plan_info String,
    status String,
    sort_no Int32,
    apn String,
    activate String,
    network String,
    local_operator String,
    use_method String,
    update_time Nullable(DateTime),
    create_time Nullable(DateTime),
    timezone_fix Int32,
    currency_id Int32,
    coverage_area String,
    import_time Date DEFAULT toDate(now())
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
ALTER table dim.dim_Nobel_data_plan_volume delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dim.dim_Nobel_data_plan_volume
SELECT
    id,
    area_id,
    volume,
    language_code,
    resource_id,
    data_plan_info,
    status,
    sort_no,
    apn,
    activate,
    network,
    local_operator,
    use_method,
    update_time,
    create_time,
    timezone_fix,
    currency_id,
    coverage_area,
    '$import_time'
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'data_plan_volume', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"