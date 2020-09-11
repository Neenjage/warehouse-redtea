#!/bin/bash

create table dim.a Engine=MergeTree order by id as select * from mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'topup_package', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')


clickhouse-client -u$1 --multiquery -q"
CREATE TABLE dim.dim_Nobel_topup_package
(
    `id` Int32,
    `rule_name` String,
    `price_key` String,
    `sort_no` Int32,
    `credit_plan_value` Int32,
    `recommended` Int32,
    `recommended_desc` String,
    `status` String,
    `update_time` DateTime,
    `create_time` DateTime,
    `import_time` Date DEFAULT toDate(now())
)
ENGINE = MergeTree
PARTITION BY import_time
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$1 --multiquery -q"
INSERT INTO dim.dim_Nobel_topup_package (
  id,
  rule_name,
  price_key,
  sort_no,
  credit_plan_value,
  recommended,
  recommended_desc,
  status,
  update_time,
  create_time,
  import_time)
SELECT
    id,
    rule_name,
    price_key,
    sort_no,
    credit_plan_value,
    recommended,
    recommended_desc,
    status,
    update_time,
    create_time,
    today()
FROM mysql('bayer-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Nobel', 'topup_package', 'redtea-ro', 'tOIgwoP1sq94CpM2uVdjxkAmhGokPVG13')
";
