#!/bin/bash

create table dim.a Engine=MergeTree order by id as select * from mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user_group', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')

clickhouse-client -u$1 --multiquery -q"
CREATE TABLE dim.dim_Bethune_user_group
(
    `id` Int32,
    `group_name` String,
    `status` String,
    `create_time` DateTime,
    `update_time` DateTime,
    `import_time` Date DEFAULT toDate(now())
)
ENGINE = MergeTree
PARTITION BY import_time
ORDER BY id
SETTINGS index_granularity = 8192
";


clickhouse-client -u$1 --multiquery -q"
INSERT INTO TABLE dim.dim_Bethune_user_group(
  id,
  group_name,
  status,
  create_time,
  update_time,
  import_time)
SELECT
    id,
    group_name,
    status,
    create_time,
    update_time,
    today()
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user_group', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')";