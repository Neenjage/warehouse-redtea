#!/bin/bash

#用户的设备登录信息包含有ip等信息
clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS ods.ods_Bethune_user_device
ENGINE = MergeTree
ORDER BY id AS
SELECT *
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user_device', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
";


clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO ods.ods_Bethune_user_device SELECT *
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user_device', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Bethune_user_device
)
";
