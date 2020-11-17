#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Einstein_payment_methods_tmp;

CREATE TABLE dim.dim_Einstein_payment_methods_tmp
ENGINE = MergeTree
ORDER BY id as
SELECT
    id,
    name,
    description,
    secret_key,
    app_id,
    notify_url,
    refund_notify_url,
    status,
    refund_check,
    '$import_time' as import_time
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'payment_methods', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

drop table if exists dim.dim_Einstein_payment_methods;

rename table dim.dim_Einstein_payment_methods_tmp to dim.dim_Einstein_payment_methods;
"
