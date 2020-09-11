#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi


clickhouse-client -u$user --multiquery -q"
CREATE TABLE IF NOT EXISTS dim.dim_Einstein_provider
(
    `id` Int32,
    `name` Nullable(String),
    `status` Nullable(String),
    `ws_url` Nullable(String),
    `provider_key` Nullable(String),
    `token` Nullable(String),
    `access_key` Nullable(String),
    `secret_key` Nullable(String),
    `import_time` Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;
"

clickhouse-client -u$user --multiquery -q"
alter table dim.dim_Einstein_provider delete where import_time = '$import_time'
"

clickhouse-client -u$user --multiquery -q"
INSERT INTO dim.dim_Einstein_provider
SELECT
    id,
    name,
    status,
    ws_url,
    provider_key,
    token,
    access_key,
    secret_key,
    '$import_time'
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'provider', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
"