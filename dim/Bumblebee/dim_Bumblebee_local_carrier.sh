#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Bumblebee_local_carrier_tmp;

CREATE TABLE dim.dim_Bumblebee_local_carrier_tmp
Engine=MergeTree
ORDER BY local_carrier_id as
select
    local_carrier_id,
    location_id,
    location_code,
    carrier_id,
    carrier_name,
    local_carrier_info_id,
    local_carrier_name,
    location_name,
    create_time,
    last_update_time,
    detail,
    rat,
    status,
    tadig,
    bundle_group_id,
    bundle_group_name,
    '$import_time' as import_time
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'local_carrier', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');

drop table if exists dim.dim_Bumblebee_local_carrier;

rename table dim.dim_Bumblebee_local_carrier_tmp to dim.dim_Bumblebee_local_carrier;
"