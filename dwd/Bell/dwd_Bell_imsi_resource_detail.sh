#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
create table if not EXISTS dwd.dwd_Bell_imsi_resource_detail_tmp
Engine=MergeTree
order by id as
select
t1.*,
gaga_merchant.gaga_merchant_id,
gaga_merchant.gaga_merchant_name
from
(select
    imsi_resoure.id,
    imsi_resoure.qr_category_id,
    imsi_resoure.bundle_id as bundle_code,
    imsi_resoure.imsi,
    imsi_resoure.iccid,
    imsi_resoure.transaction_id as transaction_code,
    imsi_resoure.status as imsi_resoure_status,
    imsi_resoure.carrier_id,
    imsi_resoure.provider_id,
    imsi_resoure.merchant_id,
    imsi_resoure.active_time,
    merchant.name as merchant_name,
    merchant.code as merchant_code,
    merchant.status as merchant_status,
    merchant.gaga_merchant_code
from ods.ods_Bell_imsi_resource imsi_resoure
left join
(select
id,
name,
code,
status,
gaga_merchant_code
from
dim.dim_Bell_merchant where import_time = '$import_time') merchant
on imsi_resoure.merchant_id = merchant.id) t1
left join
(select
id as gaga_merchant_id,
code as gaga_merchant_code,
name as gaga_merchant_name
from
dim.dim_Bumblebee_merchant where import_time = '$import_time') gaga_merchant
on t1.gaga_merchant_code = gaga_merchant.gaga_merchant_code
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table dwd.dwd_Bell_imsi_resource_detail
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
rename table dwd.dwd_Bell_imsi_resource_detail_tmp to dwd.dwd_Bell_imsi_resource_detail
"
