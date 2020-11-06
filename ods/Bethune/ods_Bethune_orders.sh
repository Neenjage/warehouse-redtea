#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Bethune_orders
ENGINE = MergeTree
ORDER BY id AS
SELECT
    id,
    user_id,
    data_plan_id,
    provider_data_plan_id,
    imei,
    device_id,
    order_no,
    count,
    amount,
    type,
    status,
    resource_order_id,
    update_time,
    create_time,
    remark,
    update_time as effective_time,
    toDateTime('2105-12-31 23:59:59') as invalid_time
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'orders', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm');

DROP TABLE IF EXISTS ods.ods_Bethune_orders_temp;

CREATE TABLE ods.ods_Bethune_orders_temp
ENGINE = MergeTree
ORDER BY id AS
SELECT
    id,
    user_id,
    data_plan_id,
    provider_data_plan_id,
    imei,
    device_id,
    order_no,
    count,
    amount,
    type,
    status,
    resource_order_id,
    update_time,
    create_time,
    remark,
    effective_time,
    if(b.id = 0, a.invalid_time, b.update_time) AS invalid_time
FROM ods.ods_Bethune_orders AS a
ANY LEFT JOIN
(
    SELECT
        id,
        update_time
    FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'orders', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
    WHERE update_time >
    (
        SELECT MAX(update_time)
        FROM ods.ods_Bethune_orders
    )
) AS b USING (id);

DROP table if exists ods.ods_Bethune_orders;

RENAME table ods.ods_Bethune_orders_temp to ods.ods_Bethune_orders;

INSERT INTO ods.ods_Bethune_orders
SELECT
    id,
    user_id,
    data_plan_id,
    provider_data_plan_id,
    imei,
    device_id,
    order_no,
    count,
    amount,
    type,
    status,
    resource_order_id,
    update_time,
    create_time,
    remark,
    update_time as effective_time,
    toDateTime('2105-12-31 23:59:59') as invalid_time
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'orders', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
WHERE update_time >  (select MAX(update_time) from ods.ods_Bethune_orders);
"
