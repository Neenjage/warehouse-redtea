#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Einstein_data_plan_provider
(
    data_plan_id Nullable(Int32),
    provider_id Nullable(Int32),
    provider_data_plan_id Nullable(String),
    status Nullable(String),
    is_default Nullable(Int8),
    pool_size Nullable(Int32),
    is_synchronized Nullable(Int8),
    id Int32,
    import_time Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
alter table dim.dim_Einstein_data_plan_provider delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dim.dim_Einstein_data_plan_provider
SELECT
    data_plan_id,
    provider_id,
    provider_data_plan_id,
    status,
    is_default,
    pool_size,
    is_synchronized,
    id,
    '$import_time'
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'data_plan_provider', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
"