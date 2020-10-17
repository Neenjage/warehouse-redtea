#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Einstein_order_appraises
(
    id Int32,
    device_id Nullable(String),
    order_id Nullable(Int32),
    data_plan_id Nullable(Int32),
    purchase_score Nullable(Int32),
    network_stability_score Nullable(Int32),
    internet_speed_score Nullable(Int32),
    update_time Nullable(DateTime)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO ods.ods_Einstein_order_appraises
SELECT
    id,
    device_id,
    order_id,
    data_plan_id,
    purchase_score,
    network_stability_score,
    internet_speed_score,
    update_time
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'order_appraises', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Einstein_order_appraises
);
"