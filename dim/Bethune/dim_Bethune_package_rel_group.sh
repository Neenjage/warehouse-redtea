#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE dim.dim_Bethune_package_rel_group
(
    id Int64,
    group_id Int32,
    package_id Int32,
    status Int32,
    create_time DateTime,
    update_time DateTime,
    remark Nullable(String),
    import_time Date DEFAULT toDate(now())
)
ENGINE = MergeTree
PARTITION BY import_time
ORDER BY id
SETTINGS index_granularity = 8192
";

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO TABLE dim.dim_Bethune_package_rel_group(
  id,
  group_id,
  package_id,
  status,
  create_time,
  update_time,
  remark,
  import_time)
SELECT
    id,
    group_id,
    package_id,
    status,
    create_time,
    update_time,
    remark,
    today()
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'package_rel_group', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')";