#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%Y-%m-%d`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client -u$user --multiquey -q"
create table if not exists dim.dim_Bumblebee_currency_rate(
id Int32,
name String,
USD_rate Float32,
CNY_rate Float32,
import_time Date
)
Engine=MergeTree
order by id
SETTINGS index_granularity = 8192
"

#获取数据从api接口中
python3.7 /home/ec2-user/dim_Bumblebee_currency_rate.py $import_time
