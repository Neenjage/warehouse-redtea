#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Einstein_data_plan_provider_tmp;

CREATE TABLE dim.dim_Einstein_data_plan_provider_tmp
ENGINE = MergeTree
ORDER BY id as
SELECT
    data_plan_id,
    provider_id,
    provider_data_plan_id,
    status,
    is_default,
    pool_size,
    is_synchronized,
    id,
    '$import_time' as import_time
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'data_plan_provider', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

drop table if exists dim.dim_Einstein_data_plan_provider;

rename table dim.dim_Einstein_data_plan_provider_tmp to dim.dim_Einstein_data_plan_provider;
"
