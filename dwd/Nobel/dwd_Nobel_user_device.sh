#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
create table if not exists dwd.dwd_Nobel_user_device(
    id Int32,
    user_id Int32,
    model Nullable(String),
    app_version Nullable(String),
    ios_version Nullable(String),
    android_version Nullable(String),
    os Nullable(String),
    update_time Nullable(DateTime),
    create_time Nullable(DateTime),
    model_name Nullable(String),
    system_version Nullable(String),
    support_esim Nullable(String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dwd.dwd_Nobel_user_device
SELECT
    id Int32,
    user_id Int32,
    model Nullable(String),
    app_version Nullable(String),
    ios_version Nullable(String),
    android_version Nullable(String),
    os Nullable(String),
    update_time Nullable(DateTime),
    create_time Nullable(DateTime),
    model_name Nullable(String),
    system_version Nullable(String),
    support_esim Nullable(String)
FROM ods.ods_Nobel_user_device
WHERE update_time >
(
    SELECT
      max(update_time)
    FROM dwd.dwd_Nobel_user_device
)
"