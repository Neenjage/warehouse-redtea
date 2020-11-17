#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists ods.ods_Nobel_users_tmp;

CREATE TABLE ods.ods_Nobel_users_tmp
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

DROP TABLE IF EXISTS ods.ods_Nobel_users;

RENAME TABLE ods.ods_Nobel_users_tmp TO ods.ods_Nobel_users;
"

