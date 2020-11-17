#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Bumblebee_bundle_tmp;

CREATE TABLE dim.dim_Bumblebee_bundle_tmp
ENGINE = MergeTree
ORDER BY id as
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
    '$import_time' as import_time
FROM mysql('ro-bumblebee-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Newton', 'bundle', 'redtea-ro', 'TecirEk8ph2jukapH83jcefaqAfa4Gpcg');

drop table if exists dim.dim_Bumblebee_bundle;

rename table dim.dim_Bumblebee_bundle_tmp to dim.dim_Bumblebee_bundle;
"