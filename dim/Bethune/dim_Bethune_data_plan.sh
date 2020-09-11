#!/bin/bash

clickhouse-client -u$1 --multiquery -q"
CREATE TABLE IF NOT EXISTS dim.dim_Bethune_data_plan
(
    `id` Int32,
    `name` Nullable(String),
    `status` Nullable(String),
    `price` Nullable(Int32),
    `description` Nullable(String),
    `create_time` Nullable(DateTime),
    `type` Nullable(Int32),
    `group_id` Int32,
    `reminder` Nullable(String),
    `number_optional` String,
    `number_list` String,
    `sort` Int32,
    `import_time` Date DEFAULT toDate(now())
)
ENGINE = MergeTree
PARTITION BY import_time
ORDER BY id
SETTINGS index_granularity = 8192
";

clickhouse-client -u$1 --multiquery -q"
INSERT INTO dim.dim_Bethune_data_plan (
  id,
  name,
  status,
  price,
  description,
  create_time ,
  type,
  group_id,
  reminder,
  number_optional,
  number_list,
  sort,
  import_time)
SELECT
    id,
    name,
    status,
    price,
    description,
    create_time ,
    type,
    group_id,
    reminder,
    number_optional,
    number_list,
    sort,
    today()
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'data_plan', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')";