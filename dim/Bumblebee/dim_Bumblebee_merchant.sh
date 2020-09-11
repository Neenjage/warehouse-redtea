#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client -u$user --multiquery -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bumblebee_merchant
(
    `id` Int32,
    `code` Nullable(String),
    `name` Nullable(String),
    `status` Nullable(String),
    `access_key` Nullable(String),
    `secret_key` Nullable(String),
    `channel_id` Nullable(Int32),
    `group_code` String,
    `import_time` Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
ALTER table dim.dim_Bumblebee_merchant delete where import_time = '$import_time'
"


clickhouse-client -u$user --multiquery -q"
INSERT INTO TABLE dim.dim_Bumblebee_merchant
SELECT
    id,
    code,
    name,
    status,
    access_key,
    secret_key,
    channel_id,
    group_code,
    '$import_time'
FROM
mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'merchant', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')
"
