#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=date +%F

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Einstein_location
(
    id Int32,
    name Nullable(String),
    logo_url Nullable(String),
    cover_url Nullable(String),
    mcc Nullable(String),
    continent Nullable(String),
    remark Nullable(String),
    status Nullable(String),
    sort_no Nullable(Int32),
    operator Nullable(String),
    netstandard Nullable(String),
    net_standard Nullable(String),
    location_code Nullable(String),
    import_time Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(import_time)
ORDER BY id
SETTINGS index_granularity = 8192;
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
ALTER TABLE dim.dim_Einstein_location delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dim.dim_Einstein_location
SELECT
    id,
    name,
    logo_url,
    cover_url,
    mcc,
    continent,
    remark,
    status,
    sort_no,
    operator,
    netstandard,
    net_standard,
    location_code,
    '$import_time'
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'location', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')"


