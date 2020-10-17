#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Nobel_user_login_record
ENGINE = MergeTree
ORDER BY id AS
SELECT 
    id,
    email,
    login_time,
    login_type,
    user_id,
    ip,
    flag,
    device_token
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'user_login_record', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

INSERT INTO ods.ods_Nobel_user_login_record
SELECT
    id,
    email,
    login_time,
    login_type,
    user_id,
    ip,
    flag,
    device_token
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'user_login_record', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Nobel_user_login_record
);
"
