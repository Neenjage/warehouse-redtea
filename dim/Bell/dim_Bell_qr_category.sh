#!/bin/bash

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists dim.dim_Bell_qr_category
(
    id Int32,
    name Nullable(String),
    status Nullable(String),
    update_time Nullable(DateTime),
    bundle_id Nullable(String),
    pool_size Nullable(Int32),
    carrier_id Nullable(Int32),
    provider_id Nullable(Int32),
    import_time Date DEFAULT toDate(now())
)
ENGINE = MergeTree
PARTITION BY import_time
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client --user $user --password $password --multiquery --multiline -q"
INSERT INTO dim.dim_Bell_qr_category (
  id,
  name,
  status,
  update_time,
  bundle_id,
  pool_size,
  carrier_id,
  provider_id,
  import_time) SELECT
    id,
    name,
    status,
    update_time,
    bundle_id,
    pool_size,
    carrier_id,
    provider_id,
    today()
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Bell', 'qr_category', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
"
