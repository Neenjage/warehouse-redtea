#!/bin/bash

source  /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client -u$user --multiquery -q"
CREATE TABLE dim.dim_Einstein_promotions
(
    `id` Int32,
    `title` Nullable(String),
    `sub_title` Nullable(String),
    `url_pic` Nullable(String),
    `url_html` Nullable(String),
    `share_image` Nullable(String),
    `promotion_rule` Nullable(String),
    `explanation` Nullable(String),
    `promotion_type` Nullable(String),
    `status` Nullable(String),
    `start_time` Nullable(DateTime),
    `end_time` Nullable(DateTime),
    `frequency` Nullable(Int32),
    `strategy_id` Nullable(Int32),
    `strategy_type` Nullable(String),
    `share_title` Nullable(String),
    `share_content` Nullable(String),
    `share_url` Nullable(String),
    `import_time` Date
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
"

clickhouse-client -u$user --multiquery -q"
alter table dim.dim_Einstein_promotions delete where import_time = '$import_time'
"

clickhouse-client -u$user --multiquery -q"
INSERT INTO TABLE dim.dim_Einstein_promotions
SELECT 
    id,
    title,
    sub_title,
    url_pic,
    url_html,
    share_image,
    promotion_rule,
    explanation,
    promotion_type,
    status,
    start_time,
    end_time,
    frequency,
    strategy_id,
    strategy_type,
    share_title,
    share_content,
    share_url,
    '$import_time'
FROM
mysql('ro-einstein-prod.c8vjxxrqkntk.ap-southeast-1.rds.amazonaws.com:3306', 'Einstein', 'promotions', 'redtea', 'DRKn3DNX3ohlsOTQWh4INrCEbgabsn6c')
"