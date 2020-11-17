#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Bumblebee_carrier_tmp;

CREATE TABLE IF NOT EXISTS dim.dim_Bumblebee_carrier_tmp
ENGINE = MergeTree()
ORDER BY id as
SELECT
    id,
    name,
    remark,
    implementation,
    access_key,
    secret_key,
    status,
    channel_id,
    support_multi_bundle,
    '$import_time' as import_time
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'carrier', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');

drop table if exists dim.dim_Bumblebee_carrier;

rename table dim.dim_Bumblebee_carrier_tmp to dim.dim_Bumblebee_carrier;
"
