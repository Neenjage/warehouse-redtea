#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Bumblebee_imsi_profile
(
    imsi_profile_id Int32,
    iccid Nullable(String),
    imsi Nullable(String),
    efki Nullable(String),
    efopc Nullable(String),
    msisdn Nullable(String),
    carrier_id Nullable(Int32),
    where_to_use Nullable(String),
    is_test Nullable(Int8),
    reusable Nullable(Int8),
    remark Nullable(String),
    allocated Nullable(Int8),
    activated Nullable(Int8),
    merchant_id Nullable(Int32),
    last_activated_time Nullable(DateTime),
    imsi_type Nullable(Int32),
    batch_id Nullable(String),
    has_csim Nullable(Int8),
    bundle_group_id Nullable(Int32),
    allocation_code Nullable(String)
)
ENGINE = MergeTree
ORDER BY imsi_profile_id
SETTINGS index_granularity = 8192;

INSERT INTO ods.ods_Bumblebee_imsi_profile
SELECT
    imsi_profile_id,
    iccid,
    imsi,
    efki,
    efopc,
    msisdn,
    carrier_id,
    where_to_use,
    is_test,
    reusable,
    remark,
    allocated,
    activated,
    merchant_id,
    last_activated_time,
    imsi_type,
    batch_id,
    has_csim,
    bundle_group_id,
    allocation_code
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'imsi_profile', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg')
WHERE imsi_profile_id >
(
    SELECT max(imsi_profile_id) AS max_c
    FROM ods.ods_Bumblebee_imsi_profile
);
"

