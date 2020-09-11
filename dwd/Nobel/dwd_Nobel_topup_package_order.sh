#!/bin/bash

user=$1

clickhouse-client -u$user --multiquery -q"
create table dwd.dwd_Nobel_topup_package
(
    `id` Int32,
    `order_no` String,
    `out_order_no` Nullable(String),
    `topup_package_id` Int32,
    `topup_package_name` Nullable(String),
    `topup_package_price` Int32,
    `order_price` Int32,
    `status` String,
    `currency_id` Int32,
    `payment_methods_id` Int32,
    `create_time` DateTime,
    `update_time` DateTime,
    `order_status` String,
    `refund_reason` Nullable(String),
    `refund_time` Nullable(DateTime),
    `amount` Int32,
    `user_id` Nullable(Int32),
    `source_type` Nullable(Int32),
    `effective_time` DateTime,
    `invalid_time` DateTime
)
Engine=MergeTree
order by id
SETTINGS index_granularity = 8192
"


clickhouse-client -u$user --multiquery -q"
create table if not exists dwd.dwd_Nobel_topup_package_tmp
Engine=MergeTree
order by id bash
select
    `id` Int32,
    `order_no` String,
    `out_order_no` Nullable(String),
    `topup_package_id` Int32,
    `topup_package_name` Nullable(String),
    `topup_package_price` Int32,
    `order_price` Int32,
    `status` String,
    `currency_id` Int32,
    `payment_methods_id` Int32,
    `create_time` DateTime,
    `update_time` DateTime,
    `order_status` String,
    `refund_reason` Nullable(String),
    `refund_time` Nullable(DateTime),
    `amount` Int32,
    `user_id` Nullable(Int32),
    `source_type` Nullable(Int32),
    `effective_time` DateTime,
    `invalid_time` DateTime
FROM ods.ods_Nobel_topup_package
"

clickhouse-client -u$user --multiquery -q"
drop table if exists dwd.dwd_Nobel_topup_package
"

clickhouse-client -u$uer --multiquery -q"
rename table dwd.dwd_Nobel_topup_package_tmp to dwd.dwd_Nobel_topup_package
"