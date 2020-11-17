#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Nobel_data_plan_volume_tmp;

CREATE TABLE IF NOT EXISTS dim.dim_Nobel_data_plan_volume_tmp
ENGINE = MergeTree
ORDER BY id as
SELECT
    id,
    area_id,
    volume,
    language_code,
    resource_id,
    data_plan_info,
    status,
    sort_no,
    apn,
    activate,
    network,
    local_operator,
    use_method,
    update_time,
    create_time,
    timezone_fix,
    currency_id,
    coverage_area,
    '$import_time' as import_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'data_plan_volume', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

drop table if exists dim.dim_Nobel_data_plan_volume;

rename table dim.dim_Nobel_data_plan_volume_tmp to dim.dim_Nobel_data_plan_volume;
"