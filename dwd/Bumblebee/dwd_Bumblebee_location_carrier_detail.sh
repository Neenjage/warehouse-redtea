#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dwd.dwd_Bumblebee_local_carrier_detail
(
    local_carrier_id Int32,
    location_id Nullable(Int32),
    location_code Nullable(String),
    carrier_id Nullable(Int32),
    carrier_name Nullable(String),
    local_carrier_info_id Nullable(Int32),
    local_carrier_name Nullable(String),
    location_name Nullable(String),
    bundle_group_id Int32,
    bundle_group_name String,
    tadig Nullable(String),
    plmn Nullable(String),
    mnc Nullable(String),
    mcc Nullable(String)
)
ENGINE = MergeTree
ORDER BY local_carrier_id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
alter table dwd.dwd_Bumblebee_local_carrier_detail delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO TABLE dwd.dwd_Bumblebee_local_carrier_detail
SELECT
    local_carrier.*,
    local_carrier_info.plmn as plmn,
    local_carrier_info.mcc as mcc,
    local_carrier_info.mnc as mnc
FROM
  (SELECT
    ab.local_carrier_id,
    ab.location_id,
    multiIf(ab.location_code = 'GBRJT_Bundle','GBRJT',
            length(ab.location_code)=6,replaceAll(location_code,'F',''),
            length(ab.location_code)>6,substr(ab.location_code,length(ab.location_code)-4,5),
            ab.location_code) as location_code,
    ab.carrier_id,
    ab.carrier_name,
    ab.local_carrier_info_id,
    ab.local_carrier_name,
    ab.location_name,
    ab.create_time,
    ab.last_update_time,
    ab.status,
    ab.tadig,
    ab.bundle_group_id,
    ab.bundle_group_name,
    import_time
  FROM (select * from dim.dim_Bumblebee_local_carrier where import_time='$import_time') as ab
  ALL INNER JOIN
  (select
    max(local_carrier_id) as max_local_carrier_id
  from
  (select
      multiIf(location_code = 'GBRJT_Bundle','GBRJT',
            length(location_code)=6,replaceAll(location_code,'F',''),
            length(location_code)>6,substr(location_code,length(location_code)-4,5),
            location_code) as location_code,
      bundle_group_id,
      local_carrier_id
  from dim.dim_Bumblebee_local_carrier where import_time = '$import_time') as tmp
  group by bundle_group_id,location_code ) as ac
  on ab.local_carrier_id = ac.max_local_carrier_id
  where max_local_carrier_id is not null
  and location_code is not null) local_carrier
left join
  (SELECT
  id,
  plmn,
  mcc,
  mnc
  FROM dim.dim_Bumblebee_local_carrier_info where import_time = '$import_time') as local_carrier_info
  ON local_carrier.local_carrier_info_id = local_carrier_info.id
"






