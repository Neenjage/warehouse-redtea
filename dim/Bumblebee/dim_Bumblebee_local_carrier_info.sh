#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Bumblebee_local_carrier_info_tmp;

CREATE TABLE dim.dim_Bumblebee_local_carrier_info_tmp
ENGINE = MergeTree
ORDER BY id as
select
    id,
    local_carrier_name,
    local_carrier_english_name,
    plmn,
    mnc,
    mcc,
    detail,
    net_abbr,
    tadig,
    '$import_time' as import_time
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'local_carrier_info', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');

drop table if exists dim.dim_Bumblebee_local_carrier_info;

rename table dim.dim_Bumblebee_local_carrier_info_tmp to dim.dim_Bumblebee_local_carrier_info;
"

