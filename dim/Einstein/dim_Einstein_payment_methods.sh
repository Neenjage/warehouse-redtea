#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Einstein_payment_methods
(
    id Int32,
    name Nullable(String),
    description Nullable(String),
    secret_key Nullable(String),
    app_id Nullable(String),
    notify_url Nullable(String),
    refund_notify_url Nullable(String),
    status Nullable(String),
    refund_check Nullable(Int8),
    import_time Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(import_time)
ORDER BY id
SETTINGS index_granularity = 8192;

ALTER TABLE dim.dim_Einstein_payment_methods delete where import_time = '$import_time';

INSERT INTO dim.dim_Einstein_payment_methods
SELECT
    id,
    name,
    description,
    secret_key,
    app_id,
    notify_url,
    refund_notify_url,
    status,
    refund_check,
    '$import_time'
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'payment_methods', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');
"
