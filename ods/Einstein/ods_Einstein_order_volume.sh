#!/bin/bash

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Einstein_order_volume
(
    id Int32,
    order_id Nullable(Int32),
    volume_usage Nullable(Int64),
    upload_time Nullable(DateTime),
    import_time Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

ALTER TABLE ods.ods_Einstein_order_volume DELETE WHERE import_time >= '$import_time';

INSERT INTO ods.ods_Einstein_order_volume
SELECT
    id,
    order_id,
    volume_usage,
    upload_time,
    toDate(addHours(upload_time, 8))
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'order_volume', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Einstein_order_volume
);
"