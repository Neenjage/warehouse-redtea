#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

#记录的用户信息包含当前拥有的豆子与砖石
clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists ods.ods_Bethune_user
(
    id Int32,
    telephone Nullable(String),
    status Nullable(String),
    points Int32,
    login_ip Nullable(String),
    recommend_user Nullable(String),
    create_time Nullable(DateTime),
    login_time Nullable(DateTime),
    group_id Int32,
    locker_key Nullable(String),
    locker_expired Nullable(DateTime),
    is_valid UInt8,
    balance Int32,
    available_balance Int32,
    available_balance_update_time DateTime,
    channel String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192；

INSERT INTO ods.ods_Bethune_user
SELECT
  id,
  telephone,
  status,
  points,
  login_ip,
  recommend_user,
  create_time,
  login_time,
  group_id,
  locker_key,
  locker_expired,
  hex(is_valid) as is_valid,
  balance,
  available_balance,
  available_balance_update_time,
  channel
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Bethune_user
);
"

