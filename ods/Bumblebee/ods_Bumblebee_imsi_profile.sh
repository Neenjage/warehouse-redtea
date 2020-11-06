#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Bumblebee_imsi_profile
ENGINE = MergeTree
ORDER BY imsi_profile_id as
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
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'imsi_profile', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');

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

