#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Einstein_register_device
(
    id Int32,
    device_id Nullable(String),
    register_time Nullable(DateTime),
    import_time Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(import_time)
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO ods.ods_Einstein_register_device
SELECT
    id,
    device_id,
    register_time,
    '$import_time'
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'register_device', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Einstein_register_device
);
"