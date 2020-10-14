#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE dim.dim_Bethune_user_group_package
(
    id Int64,
    user_group_id Int32,
    package_group_id Int32,
    status String,
    create_time DateTime,
    update_time DateTime,
    import_time Date DEFAULT toDate(now())
)
ENGINE = MergeTree
PARTITION BY import_time
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO TABLE dim.dim_Bethune_user_group_package(
  id,
  user_group_id,
  package_group_id,
  status,
  create_time,
  update_time,
  import_time)
SELECT
    id,
    user_group_id,
    package_group_id,
    status,
    create_time,
    update_time,
    today()
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user_group_package', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')"