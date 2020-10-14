#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Nobel_currency
(
    id Int32,
    name String,
    symbol String,
    remark String,
    import_time Date DEFAULT toDate(now())
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
ALTER table dim.dim_Nobel_currency delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dim.dim_Nobel_currency
SELECT
    id,
    name,
    symbol,
    remark,
    '$import_time'
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'currency', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"