#!/bin//bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE if not exists dim.dim_Einstein_data_plan_group
(
    id Int32,
    name String,
    template_data_plan_id Int32,
    create_by String,
    create_time DateTime,
    import_time Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

alter table dim.dim_Einstein_data_plan_group delete where import_time = '$import_time';

INSERT INTO TABLE dim.dim_Einstein_data_plan_group
SELECT
    id,
    name,
    template_data_plan_id,
    create_by,
    create_time,
    '$import_time'
FROM
mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'data_plan_group', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');
"

