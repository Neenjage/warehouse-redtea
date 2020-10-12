#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=date +%Y-%m-%d

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bumblebee_location_new
(
    id Int32,
    name Nullable(String),
    location_english_name Nullable(String),
    country_code Nullable(String),
    import_time Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
alter table dim.dim_Bumblebee_location_new delete where import_time = '$import_time'
"


clickhouse-client --user $user --password $password --multiquery --multiline -q"
insert into table dim.dim_Bumblebee_location_new
select
*,
'$import_time'
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'location_new', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')
"


