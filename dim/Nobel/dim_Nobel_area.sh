#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE if not exists dim.dim_Nobel_area
(
    id Int32,
    name String,
    logo_url String,
    status String,
    sort_no Int32,
    continent_id Int32,
    top Int8,
    background_image_url String,
    iphone_logo_url String,
    pixel_logo_url String,
    timezone_fix Int32,
    import_time Date DEFAULT toDate(now())
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

ALTER TABLE dim.dim_Nobel_area delete where import_time = '$import_time';

INSERT INTO dim.dim_Nobel_area
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
    '$import_time'
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'area', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');
"
