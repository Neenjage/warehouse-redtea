#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Bell_merchant_tmp;

CREATE table dim.dim_Bell_merchant_tmp
Engine=MergeTree
order by id as
SELECT
    id,
    name,
    code,
    status,
    access_key,
    secret_key,
    remark,
    gaga_merchant_code,
    gaga_access_key,
    gaga_secret_key,
    qr_code_logo_url,
    '$import_time' as import_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bell', 'merchant', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

drop table if exists dim.dim_Bell_merchant;

rename table dim.dim_Bell_merchant_tmp to dim.dim_Bell_merchant;
"
