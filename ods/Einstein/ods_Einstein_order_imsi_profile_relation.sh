#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists ods.ods_Einstein_order_imsi_profile_relation
(
    id Int32,
    order_id Int32,
    iccid Nullable(String),
    imsi Nullable(String),
    transaction_id Nullable(String),
    bundle_id Nullable(String),
    status Nullable(String),
    reused Nullable(Int8)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO ods.ods_Einstein_order_imsi_profile_relation
SELECT
    id,
    order_id,
    iccid,
    imsi,
    transaction_id,
    bundle_id,
    status,
    reused
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'order_imsi_profile_relation', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
WHERE id >
(
    SELECT MAX(id)
    FROM ods.ods_Einstein_order_imsi_profile_relation
);
"
