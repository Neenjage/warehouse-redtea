#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
DROP TABLE IF EXISTS ods.ods_Einstein_device_tmp;

CREATE TABLE IF NOT EXISTS ods.ods_Einstein_device_tmp
ENGINE = MergeTree
ORDER BY device_id AS
SELECT 
    device_id,
    imei,
    token,
    brand,
    model,
    os_name,
    os_version,
    app_version,
    agent_id,
    client_id,
    mac_address,
    last_login_time,
    uid,
    residence,
    residence_mcc,
    android_id
FROM mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'device', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c');

DROP TABLE IF EXISTS ods.ods_Einstein_device;

RENAME TABLE ods.ods_Einstein_device_tmp TO ods.ods_Einstein_device;
"