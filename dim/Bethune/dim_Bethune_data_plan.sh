#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dim.dim_Bethune_data_plan_tmp;

create table dim.dim_Bethune_data_plan_tmp
Engine=MergeTree
order by id as
SELECT
    id,
    name,
    status,
    price,
    description,
    create_time,
    type,
    group_id,
    reminder,
    number_optional,
    number_list,
    sort
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'data_plan', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm');

drop table if exists dim.dim_Bethune_data_plan;

rename table dim.dim_Bethune_data_plan_tmp to dim.dim_Bethune_data_plan;
"
