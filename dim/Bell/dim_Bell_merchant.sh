#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bell_merchant
(
    id Int32,
    name Nullable(String),
    code Nullable(String),
    status Nullable(String),
    access_key Nullable(String),
    secret_key Nullable(String),
    remark Nullable(String),
    gaga_merchant_code Nullable(String),
    gaga_access_key Nullable(String),
    gaga_secret_key Nullable(String),
    qr_code_logo_url Nullable(String),
    import_time Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
ALTER TABLE dim.dim_Bell_merchant delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO  table dim.dim_Bell_merchant
SELECT
*,
'$import_time'
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bell', 'merchant', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"

