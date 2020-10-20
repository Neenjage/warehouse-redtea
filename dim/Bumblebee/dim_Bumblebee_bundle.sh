#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password '' --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bumblebee_bundle
(
    id Int32,
    bundle_type Nullable(String),
    status Nullable(String),
    code String,
    carrier_id Nullable(Int32),
    location Nullable(String),
    description Nullable(String),
    data_volume Nullable(Int32),
    daily_limit Nullable(Int32),
    duration Nullable(Int32),
    extensible Nullable(Int8),
    plmns Nullable(String),
    fplmns Nullable(String),
    configuration Nullable(String),
    need_qos_control Nullable(Int8),
    apn Nullable(String),
    hplmn Nullable(String),
    hplmns Nullable(String),
    mcc Nullable(String),
    rat Nullable(Int32),
    size Nullable(Int32),
    get_resource_method Nullable(Int32),
    reusable Nullable(Int8),
    sms_num Nullable(Int32),
    call_duration Nullable(Int32),
    is_test Nullable(Int8),
    enable_time Nullable(DateTime),
    local_carrier_group_id Nullable(Int32),
    name Nullable(String),
    rplmn Nullable(String),
    oplmns Nullable(String),
    local_carrier_plmns Nullable(String),
    version Nullable(String),
    smsp Nullable(String),
    reuse_type Int32,
    inactive_code String,
    period_type String,
    import_time Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

ALTER TABLE dim.dim_Bumblebee_bundle delete where import_time = '$import_time';

INSERT INTO table dim.dim_Bumblebee_bundle
SELECT
    id ,
    bundle_type ,
    status ,
    code ,
    carrier_id ,
    location ,
    description ,
    data_volume ,
    daily_limit ,
    duration ,
    extensible ,
    plmns ,
    fplmns ,
    configuration ,
    need_qos_control ,
    apn ,
    hplmn ,
    hplmns ,
    mcc ,
    rat ,
    size ,
    get_resource_method ,
    reusable ,
    sms_num ,
    call_duration ,
    is_test ,
    enable_time ,
    local_carrier_group_id ,
    name ,
    rplmn ,
    oplmns ,
    local_carrier_plmns ,
    version ,
    smsp ,
    reuse_type ,
    inactive_code ,
    period_type ,
    '$import_time'
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'bundle', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');
"