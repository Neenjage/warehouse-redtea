#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

#记录注册时间及最近的登录时间   join右边数据量目前内存还不能完全接入，故采用分批处理。
clickhouse-client --user $user --password $password --multiquery --multiline --max_memory_usage 30000000000 -q"
drop table if exists dwd.dwd_Einstein_device_detail_tmp1;

create TABLE dwd.dwd_Einstein_device_detail_tmp1
Engine=MergeTree
order by device_id as
SELECT
  oed.device_id,
  oed.uid,
  oed.brand,
  oed.model,
  oed.os_name,
  oed.os_version,
  oed.app_version,
  oed.agent_id,
  oed.residence,
  oed.last_login_time,
  rd.register_time
FROM
(select
  oed.device_id,
  oed.uid,
  oed.brand,
  oed.model,
  oed.os_name,
  oed.os_version,
  oed.app_version,
  oed.agent_id,
  oed.residence,
  oed.last_login_time
from
ods.ods_Einstein_device oed) AS oed
LEFT  JOIN
(SELECT
device_id,
register_time
FROM
ods.ods_Einstein_register_device
where toDate(addHours(register_time,8)) < '2019-01-01') rd
ON oed.device_id = rd.device_id;

drop table if exists dwd.dwd_Einstein_device_detail_tmp2;

create TABLE dwd.dwd_Einstein_device_detail_tmp2
Engine=MergeTree
order by device_id as
SELECT
  oed.device_id,
  oed.uid,
  oed.brand,
  oed.model,
  oed.os_name,
  oed.os_version,
  oed.app_version,
  oed.agent_id,
  oed.residence,
  oed.last_login_time,
  if(rd.device_id is null,oed.register_time,rd.register_time) as register_time
FROM
(select
  oed.device_id,
  oed.uid,
  oed.brand,
  oed.model,
  oed.os_name,
  oed.os_version,
  oed.app_version,
  oed.agent_id,
  oed.residence,
  oed.last_login_time,
  oed.register_time
from
dwd.dwd_Einstein_device_detail_tmp1 oed) AS oed
LEFT  JOIN
(SELECT
device_id,
register_time
FROM
ods.ods_Einstein_register_device
where toDate(addHours(register_time,8)) >= '2019-01-01'
and toDate(addHours(register_time,8)) < '2020-01-01') rd
ON oed.device_id = rd.device_id;

drop table if exists dwd.dwd_Einstein_device_detail_tmp3;

create TABLE dwd.dwd_Einstein_device_detail_tmp3
Engine=MergeTree
order by device_id as
SELECT
  oed.device_id,
  oed.uid,
  oed.brand,
  oed.model,
  oed.os_name,
  oed.os_version,
  oed.app_version,
  oed.agent_id,
  oed.residence,
  oed.last_login_time,
  if(rd.device_id is null,oed.register_time,rd.register_time) as register_time
FROM
(select
  oed.device_id,
  oed.uid,
  oed.brand,
  oed.model,
  oed.os_name,
  oed.os_version,
  oed.app_version,
  oed.agent_id,
  oed.residence,
  oed.last_login_time,
  oed.register_time
from
dwd.dwd_Einstein_device_detail_tmp2 oed) AS oed
LEFT  JOIN
(SELECT
device_id,
register_time
FROM
ods.ods_Einstein_register_device
where toDate(addHours(register_time,8)) >= '2020-01-01'
and toDate(addHours(register_time,8)) < '2021-01-01') rd
ON oed.device_id = rd.device_id;


create TABLE dwd.dwd_Einstein_device_detail_tmp4
Engine=MergeTree
order by device_id as
SELECT
  oed.device_id,
  oed.uid,
  oed.brand,
  oed.model,
  oed.os_name,
  oed.os_version,
  oed.app_version,
  oed.agent_id,
  oed.residence,
  oed.last_login_time,
  if(rd.device_id is null,oed.register_time,rd.register_time) as register_time
FROM
(select
  oed.device_id,
  oed.uid,
  oed.brand,
  oed.model,
  oed.os_name,
  oed.os_version,
  oed.app_version,
  oed.agent_id,
  oed.residence,
  oed.last_login_time,
  oed.register_time
from
dwd.dwd_Einstein_device_detail_tmp3 oed) AS oed
LEFT  JOIN
(SELECT
device_id,
register_time
FROM
ods.ods_Einstein_register_device
where toDate(addHours(register_time,8)) >= '2021-01-01'
and toDate(addHours(register_time,8)) < '2022-01-01') rd
ON oed.device_id = rd.device_id;

drop table if exists dwd.dwd_Einstein_device_detail_tmp;

create table dwd.dwd_Einstein_device_detail_tmp
Engine=MergeTree
order by device_id as
select
tmp4.*,
if(uid.uid is null,-1000,uid.uid_level) as uid_level
from dwd.dwd_Einstein_device_detail_tmp4 tmp4
left join
(SELECT
  uid,
  uid_level
FROM  ods.ods_Einstein_unified_identification oeui
where oeui.uid_level is not NULL) uid
on tmp4.uid = uid.uid;

DROP TABLE if exists dwd.dwd_Einstein_device_detail;

DROP TABLE if exists dwd.dwd_Einstein_device_detail_tmp1;

DROP TABLE if exists dwd.dwd_Einstein_device_detail_tmp2;

DROP TABLE if exists dwd.dwd_Einstein_device_detail_tmp3;

DROP TABLE if exists dwd.dwd_Einstein_device_detail_tmp4;

RENAME TABLE dwd.dwd_Einstein_device_detail_tmp TO dwd.dwd_Einstein_device_detail;
"