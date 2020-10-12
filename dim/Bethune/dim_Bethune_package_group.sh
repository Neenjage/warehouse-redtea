#!/bin/bash

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bethune_package_group (
    id Int32,
    group_name String,
    status String,
    create_time DateTime,
    update_time DateTime,
    remark String,
    import_time Date DEFAULT toDate(now())
) ENGINE = MergeTree partition by import_time ORDER BY id SETTINGS index_granularity = 8192
";


clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO TABLE dim.dim_Bethune_package_group (
  id,
  group_name,
  status,
  create_time,
  update_time,
  remark,
  import_time)
SELECT
    id,
    group_name,
    status,
    create_time,
    update_time,
    remark,
    today()
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'package_group', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')";
