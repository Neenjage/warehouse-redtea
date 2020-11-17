#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Nobel_data_plan_day_tmp;

CREATE TABLE dim.dim_Nobel_data_plan_day_tmp
ENGINE = MergeTree
ORDER BY id as
SELECT
    id,
    data_plan_volume_id,
    day,
    price,
    status,
    update_time,
    create_time,
    '$import_time' as import_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'data_plan_day', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

drop table if exists dim.dim_Nobel_data_plan_day;

rename table dim.dim_Nobel_data_plan_day_tmp to dim.dim_Nobel_data_plan_day;
"

