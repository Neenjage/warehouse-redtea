#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi


clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Bumblebee_channel_tmp;

CREATE TABLE dim.dim_Bumblebee_channel_tmp
ENGINE = MergeTree()
ORDER BY id as
SELECT
    id,
    channel_id,
    type,
    merchant_id,
    carrier_id,
    channel_name,
    channel_country,
    channel_contact_name,
    channel_contact_phone,
    channel_contact_email,
    supplementary,
    channel_company_name,
    bank_name,
    bank_account,
    tech_contact_name,
    tech_contact_phone,
    tech_contact_email,
    status,
    create_time,
    last_update_time,
    bank_country,
    '$import_time' as import_time
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'channel', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');

drop table if exists dim.dim_Bumblebee_channel;

rename table dim.dim_Bumblebee_channel_tmp to dim.dim_Bumblebee_channel;
"
