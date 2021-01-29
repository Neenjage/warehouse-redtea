#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Bumblebee_local_carrier_price_history_tmp;

CREATE TABLE dim.dim_Bumblebee_local_carrier_price_history_tmp
ENGINE = MergeTree
ORDER BY id as
select
*
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'local_carrier_price_history', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');

drop table if exists dim.dim_Bumblebee_local_carrier_price_history;

rename table dim.dim_Bumblebee_local_carrier_price_history_tmp to dim.dim_Bumblebee_local_carrier_price_history;
"

