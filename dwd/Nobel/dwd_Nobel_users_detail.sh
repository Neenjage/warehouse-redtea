#!/bin/bash

user=$1

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dwd.dwd_Nobel_users_detail
(
    id Int32,
    email String,
    status Nullable(String),
    register_time Nullable(DateTime),
    update_time Nullable(DateTime),
    active_time Nullable(DateTime),
    login_time Nullable(DateTime),
    create_time Nullable(DateTime),
    source_type Int32,
    country String,
    continent String,
    address String,
    client_id Nullable(Int32),
    login_times Nullable(Int32),
    effective_time DateTime,
    invalid_time Nullable(DateTime)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE dwd.dwd_Nobel_users_detail_tmp
ENGINE=MergeTree
ORDER BY id
SELECT
    id,
    email,
    status,
    register_time,
    update_time,
    active_time,
    login_time,
    create_time,
    source_type,
    country,
    continent,
    address,
    client_id,
    login_times,
    effective_time,
    invalid_time
FROM ods.ods_Nobel_users
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table dwd.dwd_Nobel_users_detail
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
rename table dwd.dwd_Nobel_users_detail_tmp to dwd.dwd_Nobel_users_detail
"




