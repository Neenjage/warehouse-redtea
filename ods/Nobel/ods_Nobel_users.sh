#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Nobel_users
ENGINE=MergeTree
order by id as
select
    id,
    email,
    password,
    status,
    register_time,
    salt,
    update_time,
    active_time,
    login_time,
    create_time,
    source_type,
    nick_name,
    country,
    continent,
    address,
    user_level,
    client_id,
    apple_user_id,
    lang,
    device_token,
    login_times,
    update_time as effective_time,
    toDateTime('2105-12-31 23:59:59') AS invalid_time
from mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'users', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13');

drop table if exists ods.ods_Nobel_users_temp;

CREATE TABLE ods.ods_Nobel_users_temp
ENGINE = MergeTree
ORDER BY id AS
SELECT
    id,
    email,
    password,
    status,
    register_time,
    salt,
    update_time,
    active_time,
    login_time,
    create_time,
    source_type,
    nick_name,
    country,
    continent,
    address,
    user_level,
    client_id,
    apple_user_id,
    lang,
    device_token,
    login_times,
    effective_time,
    if(b.id = 0, a.invalid_time, b.update_time) AS invalid_time
FROM ods.ods_Nobel_users AS a
ANY LEFT JOIN
(
    SELECT
        id,
        update_time
    FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'users', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
    WHERE update_time >
    (
        SELECT max(update_time)
        FROM ods.ods_Nobel_users
    )
) AS b USING (id);

DROP TABLE IF EXISTS ods.ods_Nobel_users;

RENAME TABLE ods.ods_Nobel_users_temp TO ods.ods_Nobel_users;

INSERT INTO TABLE ods.ods_Nobel_users
SELECT
      id,
      email,
      password,
      status,
      register_time,
      salt,
      update_time,
      active_time,
      login_time,
      create_time,
      source_type,
      nick_name,
      country,
      continent,
      address,
      user_level,
      client_id,
      apple_user_id,
      lang,
      device_token,
      login_times,
      update_time as effective_time,
      toDateTime('2105-12-31 23:59:59') AS invalid_time
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'users', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
WHERE update_time >
(
    SELECT max(update_time)
    FROM ods.ods_Nobel_users
);
"

