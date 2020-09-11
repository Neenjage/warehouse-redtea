#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi


clickhouse-client -u$user --multiquery -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bumblebee_channel
(
    `id` Int32,
    `name` Nullable(String),
    `remark` Nullable(String),
    `implementation` Nullable(String),
    `access_key` Nullable(String),
    `secret_key` Nullable(String),
    `status` Nullable(String),
    `channel_id` Nullable(Int32),
    `support_multi_bundle` Nullable(Int8),
    `import_time` Date DEFAULT toDate(now())
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 8192;
"

clickhouse-client -u$user --multiquery -q"
alter table dim.dim_Bumblebee_channel delete where import_time = '$import_time'
"

clickhouse-client -u$user --multiquery -q"
INSERT INTO dim.dim_Bumblebee_channel
SELECT
    id,
    name,
    remark,
    implementation,
    access_key,
    secret_key,
    status,
    channel_id,
    support_multi_bundle,
    '$import_time'
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'channel', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');
"


