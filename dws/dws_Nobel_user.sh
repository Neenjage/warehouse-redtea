#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dws.dws_Nobel_user_tmp;

CREATE TABLE dws.dws_Nobel_user_tmp
ENGINE = MergeTree
ORDER BY user_id AS
SELECT
    user_id,
    email,
    register_time,
    source_type,
    user_status
FROM dwd.dwd_Nobel_users_detail;

drop table if exists dws.dws_Nobel_user;

rename table dws.dws_Nobel_user_tmp to dws.dws_Nobel_user;
"

