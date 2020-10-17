#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

#用户的设备登录信息包含有ip等信息
clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Bethune_user_device
ENGINE = MergeTree
ORDER BY id AS
SELECT
  id，
  user_id,
  imei,
  device_id,
  model,
  app_version,
  brand,
  ip,
  create_time,
  android_id
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user_device', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm');

INSERT INTO ods.ods_Bethune_user_device
SELECT
  id,
  user_id,
  imei,
  device_id,
  model,
  app_version,
  brand,
  ip,
  create_time,
  android_id
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user_device', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Bethune_user_device
);
"

