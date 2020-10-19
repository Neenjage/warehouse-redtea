#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password '' --multiquery --multiline -q"
drop table if exists dwd.dwd_Nobel_users_detail_tmp;

CREATE TABLE dwd.dwd_Nobel_users_detail_tmp
ENGINE = MergeTree
ORDER BY user_id AS
SELECT
    t1.*,
    device.model
FROM
(
    SELECT
        user.user_id,
        user.source,
        user.email,
        user.register_time,
        user.source_type,
        user.user_status,
        login.last_login_time,
        login.login_number
    FROM
    (
        SELECT
            id AS user_id,
            'Nobel' AS source,
            email,
            register_time,
            source_type,
            status AS user_status
        FROM ods.ods_Nobel_users
        where invalid_time = '2105-12-31 23:59:59'
    ) AS user
    LEFT JOIN
    (
        SELECT
            email,
            max(login_time) AS last_login_time,
            count(*) AS login_number
        FROM ods.ods_Nobel_user_login_record
        GROUP BY email
    ) AS login ON user.email = login.email
) AS t1
LEFT JOIN
(
    SELECT
        user_id,
        max(model) AS model
    FROM ods.ods_Nobel_user_device
    WHERE isNotNull(user_id)
    GROUP BY user_id
) AS device ON t1.user_id = device.user_id;

drop table if exists dwd.dwd_Nobel_users_detail;

rename table dwd.dwd_Nobel_users_detail_tmp to dwd.dwd_Nobel_users_detail;
"