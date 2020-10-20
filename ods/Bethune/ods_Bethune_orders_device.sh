#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password '' --multiquery --multiline -q"
create table if not exists ods.ods_Bethune_orders_device
Engine=MergeTree
order by id as
select 
    id,
    order_id,
    payment_method,
    balance,
    model,
    brand,
    user_ip,
    coordinate_type ,
    longitude,
    latitude,
    resource_speed,
    os_name,
    os_version,
    android_version,
    mac,
    app_version_code,
    app_version_name,
    remark 
from
mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'orders_device', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm');

INSERT INTO TABLE ods.ods_Bethune_orders_device
SELECT
    id,
    order_id,
    payment_method,
    balance,
    model,
    brand,
    user_ip,
    coordinate_type ,
    longitude,
    latitude,
    resource_speed,
    os_name,
    os_version,
    android_version,
    mac,
    app_version_code,
    app_version_name,
    remark
FROM
mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'orders_device', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
where id > (select max(id) from ods.ods_Bethune_orders_device);
"






