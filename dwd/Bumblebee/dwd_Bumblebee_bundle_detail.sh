#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
create table if not exists dwd.dwd_Bumblebee_bundle_detail
(
  bundle_id Int32,
  bundle_name Nullable(String),
  bundle_code Nullable(String),
  bundle_data_volume Nullable(Int32),
  bundle_location Nullable(String),
  bundle_enable_time Nullable(DateTime),
  carrier_id Int32,
  carrier_name Nullable(String),
  carrier_status Nullable(String),
  channel_id Int32,
  channel_name Nullable(String),
  channel_country Nullable(String),
  bundle_group_id Int16,
  bundle_group_name Nullable(String),
  import_time Date
)
Engine=MergeTree
order by bundle_id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
alter table dwd.dwd_Bumblebee_bundle_detail delete where import_time = '$import_time'
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO TABLE dwd.dwd_Bumblebee_bundle_detail
SELECT
    t2.bundle_id,
    t2.bundle_name,
    t2.bundle_code,
    t2.bundle_data_volume,
    t2.bundle_location,
    t2.bundle_enable_time,
    t2.carrier_id,
    t2.carrier_name,
    t2.carrier_status,
    t2.channel_id,
    t2.channel_name,
    t2.channel_country,
    t3.bundle_group_id,
    t3.bundle_group_name,
    '$import_time'
FROM
(
    SELECT
        t1.bundle_id as bundle_id,
        t1.bundle_code as bundle_code,
        t1.bundle_name as bundle_name,
        t1.bundle_data_volume as bundle_data_volume,
        t1.bundle_location as bundle_location,
        t1.bundle_enable_time as bundle_enable_time,
        t1.carrier_id as carrier_id,
        t1.carrier_name as carrier_name,
        t1.carrier_status as carrier_status,
        channel.channel_id as channel_id,
        channel.channel_name as channel_name,
        channel.channel_country as channel_country
    FROM
    (
        SELECT
            bundle.id AS bundle_id,
            bundle.code as bundle_code,
            bundle.name AS bundle_name,
            bundle.data_volume AS bundle_data_volume,
            bundle.location AS bundle_location,
            bundle.enable_time AS bundle_enable_time,
            carrier.channel_id as channel_id,
            carrier.carrier_id as carrier_id,
            carrier.carrier_name as carrier_name,
            carrier.carrier_status as carrier_status
        FROM dim.dim_Bumblebee_bundle AS bundle where bundle.import_time = '$import_time'
        LEFT JOIN
        (
            SELECT
                id AS carrier_id,
                channel_id,
                name AS carrier_name,
                status AS carrier_status
            FROM dim.dim_Bumblebee_carrier
            WHERE import_time = '$import_time'
        ) AS carrier ON bundle.carrier_id = carrier.carrier_id
    ) AS t1
    LEFT JOIN
    (
        SELECT
            id AS channel_id,
            channel_name,
            channel_country
        FROM dim.dim_Bumblebee_channel
        WHERE import_time = '$import_time'
    ) AS channel ON t1.channel_id = channel.channel_id
) AS t2
LEFT JOIN
(
    SELECT
        bgd.bundle_id as bundle_id,
        bg.bundle_group_id as bundle_group_id,
        bg.bundle_group_name as bundle_group_name
    FROM
      (SELECT
          bundle_id
       FROM dim.dim_Bumblebee_bundle_group_bundle
       where import_time = '$import_time') AS bgd
    LEFT JOIN
      (SELECT
          bundle_group_id,
          bundle_group_name
      FROM dim.dim_Bumblebee_bundle_group
      where import_time = $import_time) AS bg
    ON bgd.bundle_group_id = bg.bundle_group_id
) AS t3 ON t2.bundle_id = t3.bundle_id
"
