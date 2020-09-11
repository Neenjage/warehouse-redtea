#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
CREATE TABLE ods.ods_Einstein_netless_roaming_iccid_usage
(
    `id` Int32,
    `device_id` Nullable(String),
    `iccid` Nullable(String),
    `use_time` DateTime,
    `type` Nullable(Int32),
    `consume_time` Nullable(Int32),
    `mcc` Nullable(String),
    `count` Nullable(Int32),
    `upload_time` Nullable(DateTime)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(use_time)
ORDER BY id
SETTINGS index_granularity = 8192;
"

clickhouse-client -u$1 --multiquery -q"
INSERT INTO ods.ods_Einstein_netless_roaming_iccid_usage (
  id,
  device_id,
  iccid,
  use_time,
  type,
  consume_time,
  mcc,
  count,
  upload_time)
SELECT
    id,
    device_id,
    iccid,
    use_time,
    type,
    consume_time,
    mcc,
    count,
    upload_time
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'netless_roaming_iccid_usage', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Einstein_netless_roaming_iccid_usage
);
"

