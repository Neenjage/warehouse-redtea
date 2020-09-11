#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

#记录注册时间及最近的登录时间
clickhouse-client -u$user --multiquery -q"
CREATE TABLE IF NOT EXISTS dwd.dwd_Einstein_device_detail
(
    device_id String,
    brand Nullable(String),
    model Nullable(String),
    os_name Nullable(String),
    os_version Nullable(String),
    app_version Nullable(String),
    agent_id Nullable(Int32),
    last_login_time DateTime,
    register_time DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(register_time)
ORDER BY device_id
SETTINGS index_granularity = 8192
"

#clikhouse中string类型为空的时候用''表示

clickhouse-client -u$user --multiquery -q"
CREATE TABLE dwd.dwd_Einstein_device_detail_tmp
ENGINE = MergeTree
PARTITION BY toYYYYMM(register_time)
ORDER BY device_id AS
SELECT
    di.device_id,
    if(b.device_id != '',b.brand,di.brand) as brand,
    if(b.device_id != '',b.model,di.model) as model,
    if(b.device_id != '',b.os_name,di.os_name) as os_name,
    if(b.device_id != '',b.os_version,di.os_version) as os_version,
    if(b.device_id != '',b.app_version,di.app_version) as app_version,
    if(b.device_id != '',b.agent_id,di.agent_id) as agent_id,
    if(b.device_id != '', b.last_login_time, di.last_login_time) AS last_login_time,
    di.register_time
FROM dwd.dwd_Einstein_device_detail AS di
ANY LEFT JOIN
(
    SELECT
        device_id,
        brand,
        model,
        os_name,
        os_version,
        app_version,
        agent_id,
        last_login_time
    FROM ods.ods_Einstein_device
    WHERE toDate(addHours(last_login_time, 8)) = subtractDays(toDate(addHours(now(), 8)), 1)
) AS b USING (device_id)
"


clickhouse-client -u$user --multiquery -q"
DROP TABLE dwd.dwd_Einstein_device_detail
"

clickhouse-client -u$user --multiquery -q"
RENAME TABLE dwd.dwd_Einstein_device_detail_tmp TO dwd.dwd_Einstein_device_detail
"

clickhouse-client -u$user --multiquery -q"
INSERT INTO dwd.dwd_Einstein_device_info SELECT
    oed.device_id,
    oed.brand,
    oed.model,
    oed.os_name,
    oed.os_version,
    oed.app_version,
    oed.agent_id,
    oed.last_login_time,
    b.register_time
FROM ods.ods_Einstein_device AS oed
INNER JOIN
(
    SELECT
        device_id,
        register_time
    FROM ods.ods_Einstein_register_device
    WHERE toDate(addHours(register_time, 8)) = subtractDays(toDate(addHours(now(), 8)), 1)
) AS b USING (device_id)
"
