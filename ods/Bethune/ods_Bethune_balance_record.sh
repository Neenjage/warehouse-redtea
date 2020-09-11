#!/bin/bash

#用户的砖石记录日志
clickhouse-client -u$1 --multiquery -q"
CREATE TABLE IF NOT EXISTS ods.ods_Bethune_balance_record
ENGINE = MergeTree
ORDER BY id AS
SELECT *
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'balance_record', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
";


clickhouse-client -u$1 --multiquery -q"
INSERT INTO ods.ods_Bethune_balance_record SELECT *
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'balance_record', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
WHERE id >
(
    SELECT max(id)
    FROM ods.ods_Bethune_balance_record
)
";
