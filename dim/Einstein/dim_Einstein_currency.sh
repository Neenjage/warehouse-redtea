#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Einstein_currency
(
    id Int32,
    name String,
    symbol Nullable(String),
    remark Nullable(String),
    import_time Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

alter table dim.dim_Einstein_currency delete where import_time = '$import_time';

INSERT INTO TABLE dim.dim_Einstein_currency
SELECT
  id,
  name,
  symbol,
  remark,
  '$import_time'
FROM
mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'currency', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');
"