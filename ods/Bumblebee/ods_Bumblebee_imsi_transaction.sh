#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

#generate_time 与自增id基本一致的顺序性
clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Bumblebee_imsi_transaction
(
    imsi_transaction_id Int32,
    imsi Nullable(String),
    bundle_id Nullable(String),
    status Nullable(String),
    generate_time Nullable(DateTime),
    activate_time Nullable(DateTime),
    suspend_time Nullable(DateTime),
    is_limited Nullable(Int8),
    extend_count Nullable(Int32),
    imsi_profile_id Nullable(Int32),
    merchant_id Nullable(Int32),
    is_test Nullable(Int32),
    parent_transaction_id Nullable(Int32),
    order_id Nullable(String),
    code String,
    parent_code String,
    url Nullable(String),
    threshold Nullable(String),
    ac String,
    import_time Date
)
ENGINE = MergeTree
ORDER BY imsi_transaction_id
SETTINGS index_granularity = 8192;

ALTER TABLE ods.ods_Bumblebee_imsi_transaction delete where import_time >='$import_time';

INSERT INTO TABLE ods.ods_Bumblebee_imsi_transaction
SELECT
    imsi_transaction_id,
    imsi,
    bundle_id,
    status,
    generate_time,
    activate_time,
    suspend_time,
    is_limited,
    extend_count,
    imsi_profile_id,
    merchant_id,
    is_test,
    parent_transaction_id,
    order_id,
    code,
    parent_code,
    url,
    threshold,
    ac,
    toDate(addHours(generate_time,8)) as import_time
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'imsi_transaction', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')
WHERE imsi_transaction_id >
(
  SELECT max(imsi_transaction_id) FROM ods.ods_Bumblebee_imsi_transaction
);
"
