#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Nobel_area_tmp;

CREATE TABLE dim.dim_Nobel_area_tmp
ENGINE = MergeTree
ORDER BY id as
SELECT
    id,
    name,
    logo_url,
    status,
    sort_no,
    continent_id,
    top,
    background_image_url,
    iphone_logo_url,
    pixel_logo_url,
    timezone_fix,
    '$import_time' as import_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'area', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

drop table if exists dim.dim_Nobel_area;

rename table dim.dim_Nobel_area_tmp to dim.dim_Nobel_area;
"
