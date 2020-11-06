#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

#取每天最新的数据
clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Bell_imsi_resource_tmp;

create table ods.ods_Bell_imsi_resource_tmp
ENGINE=MergeTree
ORDER BY id AS
select 
    id,
    qr_category_id,
    bundle_id,
    imsi,
    iccid,
    transaction_id,
    status,
    is_allocated,
    is_qr_enable,
    generate_time,
    update_time,
    confirm_code,
    active_time,
    carrier_id,
    provider_id,
    merchant_id,
    batch_no,
    profile_type,
    add_profile,
    lock_key,
    lock_expire_time
from mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bell', 'imsi_resource', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

drop table if exists ods.ods_Bell_imsi_resource;

rename table ods.ods_Bell_imsi_resource_tmp to ods.ods_Bell_imsi_resource;
"



