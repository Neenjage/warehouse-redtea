#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client -u$user --multiquery -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bumblebee_bundle_group_bundle
(
    `id` Int32,
    `bundle_group_id` Nullable(Int32),
    `bundle_id` Nullable(Int32),
    `bundle_code` String,
    `create_time` Nullable(DateTime),
    `import_time` Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
ALTER table dim.dim_Bumblebee_bundle_group_bundle delete where import_time = '$import_time'
"


clickhouse-client -u$user --multiquery -q"
INSERT INTO TABLE dim.dim_Bumblebee_bundle_group_bundle
select
    id,
    bundle_group_id,
    bundle_id,
    bundle_code,
    create_time,
    '$import_time'
from
mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'bundle_group_bundle', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')
"


INSERT INTO TABLE dim.dim_Bumblebee_bundle_group_bundle
select
    id,
    bundle_group_id,
    bundle_id,
    bundle_code,
    create_time,
    '2020-09-04'
from
mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'bundle_group_bundle', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')


