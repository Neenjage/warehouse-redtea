#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Nobel_day_client_resource_tmp;

CREATE TABLE dim.dim_Nobel_day_client_resource_tmp
ENGINE = MergeTree
ORDER BY id as
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
    '$import_time' as import_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'day_client_resource', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

drop table if exists dim.dim_Nobel_day_client_resource;

rename table dim.dim_Nobel_day_client_resource_tmp to dim.dim_Nobel_day_client_resource;
"
