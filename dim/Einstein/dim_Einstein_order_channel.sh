#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=date +%F

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Einstein_order_channel
(
    id Int8,
    type Nullable(String),
    remark Nullable(String),
    import_time Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
ALTER TABLE dim.dim_Einstein_order_channel delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dim.dim_Einstein_order_channel
SELECT
    id,
    type,
    remark,
    '$import_time'
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'order_channel', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
"
