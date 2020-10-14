#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE dim.dim_Nobel_topup_package
(
    id Int32,
    rule_name String,
    price_key String,
    sort_no Int32,
    credit_plan_value Int32,
    recommended Int32,
    recommended_desc String,
    status String,
    update_time DateTime,
    create_time DateTime,
    import_time Date DEFAULT toDate(now())
)
ENGINE = MergeTree
PARTITION BY import_time
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
ALTER table dim.dim_Nobel_topup_package delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dim.dim_Nobel_topup_package
SELECT
    id,
    rule_name,
    price_key,
    sort_no,
    credit_plan_value,
    recommended,
    recommended_desc,
    status,
    update_time,
    create_time,
    '$import_time'
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'topup_package', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
";
