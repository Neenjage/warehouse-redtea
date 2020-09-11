#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
CREATE TABLE IF NOT EXISTS ods.ods_Einstein_netless_roaming_imsi_usage
(
    `id` Int32,
    `device_id` Nullable(String),
    `imsi` Nullable(String),
    `use_time` DateTime,
    `upload_time` Nullable(DateTime)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(use_time)
ORDER BY id
SETTINGS index_granularity = 8192;
"

clickhouse-client -u$1 --multiquery -q"
INSERT INTO ods.ods_Einstein_netless_roaming_imsi_usage (
  id,
  device_id,
  imsi,
  use_time,
  upload_time)
SELECT
    id,
    device_id,
    imsi,
    use_time,
    upload_time
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'netless_roaming_imsi_usage', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Einstein_netless_roaming_imsi_usage
);
"

