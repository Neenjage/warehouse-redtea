#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Bumblebee_bundle_price_tmp;

CREATE TABLE dim.dim_Bumblebee_bundle_price_tmp
ENGINE = MergeTree
ORDER BY id as
SELECT
     *,
    '$import_time' as import_time
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'bundle_price', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');

drop table if exists dim.dim_Bumblebee_bundle_price;

rename table dim.dim_Bumblebee_bundle_price_tmp to dim.dim_Bumblebee_bundle_price;
"
