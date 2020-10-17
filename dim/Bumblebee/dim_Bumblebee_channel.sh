#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi


clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bumblebee_channel
(
    id Int32,
    channel_id Nullable(String),
    type Int32,
    merchant_id Nullable(Int32),
    carrier_id Nullable(Int32),
    channel_name String,
    channel_country Nullable(String),
    channel_contact_name Nullable(String),
    channel_contact_phone Nullable(String),
    channel_contact_email Nullable(String),
    supplementary Nullable(String),
    channel_company_name Nullable(String),
    bank_name Nullable(String),
    bank_account Nullable(String),
    tech_contact_name Nullable(String),
    tech_contact_phone Nullable(String),
    tech_contact_email Nullable(String),
    status Int32,
    create_time Nullable(DateTime),
    last_update_time Nullable(DateTime),
    bank_country Nullable(String),
    import_time Date DEFAULT toDate(now())
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 8192;
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
alter table dim.dim_Bumblebee_channel delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dim.dim_Bumblebee_channel
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
    '$import_time'
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'channel', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');
"


