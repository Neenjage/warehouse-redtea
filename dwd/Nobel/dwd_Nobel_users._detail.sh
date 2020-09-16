#!/bin/bash

user=$1

clickhouse-client -u$user --multiquery -q"
CREATE TABLE IF NOT EXISTS dwd.dwd_Nobel_users
(
    `id` Int32,
    `email` String,
    `status` Nullable(String),
    `register_time` Nullable(DateTime),
    `update_time` Nullable(DateTime),
    `active_time` Nullable(DateTime),
    `login_time` Nullable(DateTime),
    `create_time` Nullable(DateTime),
    `source_type` Int32,
    `country` String,
    `continent` String,
    `address` String,
    `client_id` Nullable(Int32),
    `login_times` Nullable(Int32),
    `effective_time` DateTime,
    `invalid_time` Nullable(DateTime)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
CREATE TABLE dwd.dwd_Nobel_users_tmp
ENGINE=MergeTree
ORDER BY id
SELECT
    id,
    email,
    status,
    register_time,
    update_time,
    active_time,
    login_time,
    create_time,
    source_type,
    country,
    continent,
    address,
    client_id,
    login_times,
    effective_time,
    invalid_time
FROM ods.ods_Nobel_users
"

clickhouse-client -u$user --multiquery -q"
drop table dwd.dwd_Nobel_users
"

clickhouse-client -u$user --multiquery -q"
rename table dwd.dwd_Nobel_users_tmp to dwd.dwd_Nobel_users
"
create table dwd.dwd_Nobel_users_detail
Engine=MergeTree
order by user_id as
select
t1.*,
device.model
from
(select
user.user_id,
user.source,
user.email,
user.register_time,
user.source_type,
user.user_status,
login.last_login_time,
login.login_number
from
(
select
  id as user_id,
  'Nobel' as source,
  email,
  register_time,
  source_type,
  status as user_status
from
ods.ods_Nobel_users
where invalid_time ='2105-12-31 23:59:59') user
left join
(select
  email,
  max(login_time) as last_login_time,
  count(*) as login_number
from
ods.ods_Nobel_user_login_record
group by email) login on user.email = login.email) t1
left join
(select
  user_id,
  max(model) as model
from
ods.ods_Nobel_user_device
where user_id is not null
group by user_id) device
on t1.user_id = device.user_id



