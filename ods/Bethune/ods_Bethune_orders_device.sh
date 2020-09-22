#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh


clickhouse-client -u$user --multiquery -q"
create table if not exists ods.ods_Bethune_orders_device
Engine=MergeTree
order by id as
select *
from
mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'orders_device', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
"

clickhouse-client -u$user --multiquery -q"
INSERT INTO TABLE ods.ods_Bethune_orders_device
SELECT
*
FROM
mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'orders_device', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
where id > (select max(id) from ods.ods_Bethune_orders_device)
"





