#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Einstein_agent
(
    id Int32,
    name Nullable(String),
    remark Nullable(String),
    status Nullable(String),
    auto_refund Nullable(Int8),
    import_time Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(import_time)
ORDER BY id
SETTINGS index_granularity = 8192;

ALTER TABLE dim.dim_Einstein_agent DELETE WHERE import_time = '$import_time';

INSERT INTO dim.dim_Einstein_agent
SELECT
    id,
    name,
    remark,
    status,
    auto_refund,
    '${import_time}'
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'agent', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');
"