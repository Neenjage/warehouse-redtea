#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bumblebee_bundle_group
(
    bundle_group_id Int32,
    bundle_group_name Nullable(String),
    carrier_id Nullable(Int32),
    create_time Nullable(DateTime),
    carrier_name Nullable(String),
    status Nullable(Int32),
    order_policy Int32,
    import_time Date
)
ENGINE = MergeTree
ORDER BY bundle_group_id
SETTINGS index_granularity = 8192;

alter table dim.dim_Bumblebee_bundle_group delete where import_time = '$import_time';

INSERT INTO TABLE dim.dim_Bumblebee_bundle_group
select
    bundle_group_id,
    bundle_group_name,
    carrier_id,
    create_time,
    carrier_name,
    status,
    order_policy,
    '$import_time'
from
mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'bundle_group', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');
"




