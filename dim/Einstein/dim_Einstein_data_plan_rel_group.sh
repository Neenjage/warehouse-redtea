#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE dim.dim_Einstein_data_plan_rel_group
(
    id Int32,
    data_plan_group_id Int32,
    data_plan_id Int32,
    import_time Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

AlTER TABLE dim.dim_Einstein_data_plan_rel_group delete where import_time = '$import_time';

INSERT INTO TABLE dim.dim_Einstein_data_plan_rel_group
SELECT
    id,
    data_plan_group_id,
    data_plan_id,
    '$import_time'
FROM
mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'data_plan_rel_group', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');
"
