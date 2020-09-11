#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client -u$1 --multiquery -q"
CREATE TABLE IF NOT EXISTS dim.dim_Nobel_continent
(
    `id` Int32,
    `name` String,
    `logo_url` String,
    `status` String,
    `sort_no` Int32,
    `cross_regional` Int8,
    `import_time` Date DEFAULT toDate(now())
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
ALTER table dim.dim_Nobel_continent delete where import_time = '$import_time'
"

clickhouse-client -u$1 --multiquery -q"
INSERT INTO dim.dim_Nobel_continent
SELECT
    id,
    name,
    logo_url,
    status,
    sort_no,
    cross_regional,
    '$import_time'
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'continent', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"

