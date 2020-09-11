#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client -u$user --multiquery -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bumblebee_local_carrier
(
    `local_carrier_id` Int32,
    `location_id` Nullable(Int32),
    `location_code` Nullable(String),
    `carrier_id` Nullable(Int32),
    `carrier_name` Nullable(String),
    `local_carrier_info_id` Nullable(Int32),
    `local_carrier_name` Nullable(String),
    `location_name` Nullable(String),
    `create_time` Nullable(DateTime),
    `last_update_time` Nullable(DateTime),
    `detail` Nullable(String),
    `rat` Nullable(Int32),
    `status` Nullable(Int32),
    `tadig` Nullable(String),
    `bundle_group_id` Int32,
    `bundle_group_name` String,
    `import_time` Date
)
ENGINE = MergeTree
ORDER BY local_carrier_id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
alter table dim.dim_Bumblebee_local_carrier delete where import_time = '$import_time'
"


clickhouse-client -u$user --multiquery -q"
insert into table dim.dim_Bumblebee_local_carrier
select
*,
'$import_time'
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'local_carrier', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')
"
insert into table dim.dim_Bumblebee_local_carrier
select
*,
'2020-09-07'
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'local_carrier', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')