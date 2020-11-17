#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Einstein_currency_tmp;

CREATE TABLE dim.dim_Einstein_currency_tmp
ENGINE = MergeTree
ORDER BY id as
SELECT
  id,
  name,
  symbol,
  remark,
  '$import_time' as import_time
FROM
mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'currency', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

drop table if exists dim.dim_Einstein_currency;

rename table dim.dim_Einstein_currency_tmp to dim.dim_Einstein_currency;
"