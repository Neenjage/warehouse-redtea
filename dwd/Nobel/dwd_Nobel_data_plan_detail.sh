#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists dwd.dwd_Nobel_data_plan_detail
(
    day_client_resource_id Int32,
    data_plan_day_id Nullable(Int32),
    support_client String,
    package_level Int32,
    resource_id Int32,
    valid_day Int32,
    support_cdr Int8,
    day_client_resource_status String,
    update_time Nullable(DateTime),
    create_time Nullable(DateTime),
    price Nullable(Int32),
    original_price Nullable(Int32),
    promotion_id Int32,
    data_plan_volume_id Int32,
    day Int32,
    data_plan_day_status String,
    area_id Int32,
    data_plan_volume Int32,
    data_plan_volume_status String,
    data_plan_volume_network String,
    data_plan_volume_local_operator String,
    currency_id Int32,
    coverage_area String,
    area_name String,
    area_status String,
    continent_id Int32,
    continent_name String,
    currency_name String,
    currency_remark String,
    import_time Date
)
ENGINE = MergeTree
ORDER BY day_client_resource_id
SETTINGS index_granularity = 8192;

ALTER table dwd.dwd_Nobel_data_plan_detail delete where import_time = '$import_time';

INSERT INTO TABLE dwd.dwd_Nobel_data_plan_detail
select
  dcr.id as day_client_resource_id,
  dcr.day_id as data_plan_day_id,
  dcr.support_client,
  dcr.package_level,
  dcr.resource_id,
  dcr.valid_day,
  dcr.support_cdr,
  dcr.status as day_client_resource_status,
  dcr.update_time,
  dcr.create_time,
  dcr.price,
  dcr.original_price,
  dcr.promotion_id,
  t4.data_plan_volume_id,
  t4.day,
  t4.status as data_plan_day_status,
  t4.area_id,
  t4.data_plan_volume,
  t4.data_plan_volume_status,
  t4.data_plan_volume_network,
  t4.data_plan_volume_local_operator,
  t4.currency_id,
  t4.coverage_area,
  t4.area_name,
  t4.area_status,
  t4.continent_id,
  t4.continent_name,
  t4.currency_name,
  t4.currency_remark,
  dcr.import_time
from
(select
  id,
  day_id,
  support_client,
  package_level,
  resource_id,
  valid_day,
  support_cdr,
  status,
  update_time,
  create_time,
  price,
  original_price,
  promotion_id,
  import_time
from dim.dim_Nobel_day_client_resource where import_time = '$import_time') dcr
left join
    (select
        dpd.id as data_plan_day_id,
        dpd.data_plan_volume_id,
        dpd.day,
        dpd.status,
        t3.area_id,
        t3.data_plan_volume,
        t3.data_plan_volume_status,
        t3.data_plan_volume_network,
        t3.data_plan_volume_local_operator,
        t3.currency_id,
        t3.coverage_area,
        t3.area_name,
        t3.area_status,
        t3.continent_id,
        t3.continent_name,
        t3.currency_name,
        t3.currency_remark
    FROM
    (select
      id,
      data_plan_volume_id,
      day,
      status
    from dim.dim_Nobel_data_plan_day where import_time = '$import_time') dpd
    left join
      (select
        t2.*,
        currency.name as currency_name,
        currency.remark as currency_remark
      from
      (select
          dpv.id as data_plan_volume_id,
          dpv.area_id as area_id,
          dpv.volume as data_plan_volume,
          dpv.status as data_plan_volume_status,
          dpv.network as data_plan_volume_network,
          dpv.local_operator as data_plan_volume_local_operator,
          dpv.currency_id as currency_id,
          dpv.coverage_area as coverage_area,
          t1.area_name,
          t1.area_status,
          t1.continent_id,
          t1.continent_name
      FROM
        (select
          id,
          area_id,
          volume,
          status,
          network,
          local_operator,
          currency_id,
          coverage_area
        from dim.dim_Nobel_data_plan_volume where import_time = '$import_time') dpv
        left join
           (select
             area.id as area_id,
             area.name as area_name,
             area.status as area_status,
             area.continent_id as continent_id,
             continent.name as continent_name
          from
            (select
              id,
              name,
              status,
              continent_id
            from dim.dim_Nobel_area where import_time = '$import_time') area
            left join
            (select
              id,
              name
            from dim.dim_Nobel_continent where import_time = '$import_time') continent
          ON area.continent_id = continent.id) t1
        on dpv.area_id = t1.area_id) t2
        left join
        (select
           id,
           name,
           remark
        FROM dim.dim_Nobel_currency where import_time = '$import_time') currency
        on t2.currency_id = currency.id) t3
    on dpd.data_plan_volume_id = t3.data_plan_volume_id) t4
on dcr.day_id = t4.data_plan_day_id;
"

