#!/bin/bash

user=$1

clickhouse-client --user $user --password $password --multiquery --multiline -q"
create table if not exists dwd.dwd_Nobel_user_modification_recorder(
    id Int32,
    email Nullable(String),
    action Nullable(String),
    success Nullable(String),
    create_time Nullable(DateTime))
Engine=MereTree
order by id
SETTINGS index_granularity = 8192
"


clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO table dwd.dwd_Nobel_user_modification_recorder
SELECT
   id Int32,
    email Nullable(String),
    action Nullable(String),
    success Nullable(String),
    create_time
FROM ods.ods_Nobel_user_modification_recorder
WHERE id >
(
  SELECT
    MAX(id)
  FROM
  dwd.dwd_Nobel_user_modification_recorder
)
"