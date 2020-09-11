#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client -u$user --multiquery -q"
CREATE TABLE dim.dim_Nobel_payment_methods
(
    `id` Int32,
    `name` String,
    `logo_url` String,
    `tips` String,
    `secret_key` String,
    `notify_url` String,
    `sort_no` Int32,
    `status` String,
    `support_client` String,
    `import_time` Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
ALTER table dim.dim_Nobel_payment_methods delete where import_time = '$import_time'
"

clickhouse-client -u$user --multiquery -q"
INSERT INTO TABLE dim.dim_Nobel_payment_methods
SELECT
*,
'$import_time'
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'payment_methods', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"