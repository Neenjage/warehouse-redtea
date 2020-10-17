#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Nobel_user_device
ENGINE = MergeTree
ORDER BY id AS
SELECT 
    id,
    user_id,
    imei,
    model,
    app_version,
    ios_version,
    android_version,
    os,
    eid,
    update_time,
    create_time,
    model_name,
    uuid,
    system_version,
    support_esim
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'user_device', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

INSERT INTO TABLE ods.ods_Nobel_user_device
SELECT
    id,
    user_id,
    imei,
    model,
    app_version,
    ios_version,
    android_version,
    os,
    eid,
    update_time,
    create_time,
    model_name,
    uuid,
    system_version,
    support_esim
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'user_device', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
WHERE update_time >
(
    SELECT max(update_time) AS max_c
    FROM ods.ods_Nobel_user_device
);
"

