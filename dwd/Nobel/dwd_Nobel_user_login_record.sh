#!/bin/bash

user=$1

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE dwd.dwd_Nobel_user_login_record
(
    id Int32,
    email Nullable(String),
    login_time DateTime,
    login_type Nullable(String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"
clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO TABLE dwd.dwd_Nobel_user_login_record
SELECT
id,
email
login_time,
login_type
FROM ods.ods_Nobel_user_login_record
WHERE id >
(
  SELECT
    MAX(id)
  FORM dwd.dwd_Nobel_user_login_record
)
"