#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client -u$user --multiquery -q"
CREATE TABLE if not exists dim.dim_Bumblebee_local_carrier_info
(
    `id` Int32,
    `local_carrier_name` Nullable(String),
    `local_carrier_english_name` Nullable(String),
    `plmn` Nullable(String),
    `mnc` Nullable(String),
    `mcc` Nullable(String),
    `detail` Nullable(String),
    `net_abbr` Nullable(String),
    `tadig` Nullable(String),
    import_time Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
alter table dim.dim_Bumblebee_local_carrier_info delete where import_time = '$import_time'
"


clickhouse-client -u$user --multiquery -q"
insert into  table dim.dim_Bumblebee_local_carrier_info
select
*,
'$import_time'
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'local_carrier_info', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')
"

insert into  table dim.dim_Bumblebee_local_carrier_info
select
*,
'2020-09-07'
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'local_carrier_info', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')