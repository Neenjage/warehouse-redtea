#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi


clickhouse-client -u$user --multiquery -q"
CREATE TABLE IF NOT EXISTS dim.dim_Nobel_day_client_resource
(
    `id` Int32,
    `day_id` Nullable(Int32),
    `support_client` String,
    `package_level` Int32,
    `resource_id` Int32,
    `valid_day` Int32,
    `support_cdr` Int8,
    `status` String,
    `update_time` Nullable(DateTime),
    `create_time` Nullable(DateTime),
    `description` Nullable(String),
    `price` Nullable(Int32),
    `original_price` Nullable(Int32),
    `promotion_id` Int32,
    `import_time` Date DEFAULT toDate(now())
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
ALTER TABLE dim.dim_Nobel_day_client_resource delete where import_time = '$import_time'
"

clickhouse-client -u$user --multiquery -q"
INSERT INTO dim.dim_Nobel_day_client_resource
SELECT
    id,
    day_id,
    support_client,
    package_level,
    resource_id,
    valid_day,
    support_cdr,
    status,
    update_time,
    create_time,
    description,
    price,
    original_price,
    promotion_id,
    '$import_time'
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'day_client_resource', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"

