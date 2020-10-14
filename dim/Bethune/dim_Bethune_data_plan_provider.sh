#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE dim.dim_Bethune_data_plan_provider
(
    data_plan_id Int32,
    status Nullable(String),
    merchant_id Nullable(Int32),
    provider_data_plan Nullable(String),
    import_time Date DEFAULT toDate(now())
)
ENGINE = MergeTree
PARTITION BY import_time
ORDER BY data_plan_id
SETTINGS index_granularity = 8192
";

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dim.dim_Bethune_data_plan_provider (
  data_plan_id,
  status,
  merchant_id,
  provider_data_plan,
  import_time)
SELECT
    data_plan_id,
    status,
    merchant_id,
    provider_data_plan,
    today()
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'data_plan_provider', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')";
