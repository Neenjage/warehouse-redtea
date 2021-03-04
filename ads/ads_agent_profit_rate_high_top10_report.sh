#!/bin/bash

source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
CREATE TABLE if not exists ads.ads_agent_profit_rate_high_top10_report
(
    month String,
    agent_name Nullable(String),
    order_number UInt64,
    data_plan_name String,
    order_sales Nullable(Float64),
    order_cost Nullable(Float64),
    net_amount Nullable(Float64),
    profit_rate Float64,
    rank UInt64
)
ENGINE = MergeTree
ORDER BY order_number
SETTINGS index_granularity = 8192;

ALTER TABLE ads.ads_agent_profit_rate_high_top10_report delete where month = toString(toYYYYMM(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))));

INSERT INTO ads.ads_agent_profit_rate_high_top10_report
SELECT
    toString(toYYYYMM(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))))) as month,
    agent_name,
    array_order_number AS order_number,
    array_data_plan_name AS data_plan_name,
    array_order_sales AS order_sales,
    array_order_cost AS order_cost,
    array_order_net_amount AS net_amount,
    array_profit_rate AS profit_rate,
    rank
FROM
(
    SELECT
        agent_name,
        array_order_number,
        array_data_plan_name,
        array_order_sales,
        array_order_cost,
        array_order_net_amount,
        array_profit_rate,
        rank
    FROM
    (
        SELECT
            agent_name,
            groupArray(order_number) AS array_order_number,
            groupArray(data_plan_name) AS array_data_plan_name,
            groupArray(order_sales) AS array_order_sales,
            groupArray(order_cost) AS array_order_cost,
            groupArray(order_net_amount) AS array_order_net_amount,
            groupArray(profit_rate) AS array_profit_rate,
            arrayEnumerate(array_profit_rate) AS rank
        FROM
        (
            SELECT
                dro.agent_name,
                dro.data_plan_name,
                toDecimal64(sum(dro.order_CNYamount), 2) AS order_sales,
                count(*) AS order_number,
                toDecimal64(sum(total_cost), 2) AS order_cost,
                toDecimal64(sum(net_amount), 2) AS order_net_amount,
                sum(net_amount) / sum(total_cost) AS profit_rate
            FROM dws.dws_redtea_order AS dro
            WHERE (order_status NOT IN ('REFUNDED', 'REFUNDING', 'RESERVED'))
            AND (invalid_time = '2105-12-31 23:59:59')
            AND (order_time >= toDateTime(concat(toString(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00')))),' 00:00:00')))
            AND (order_time < toDateTime(concat(toString(addMonths(toStartOfMonth(toDateTime(concat(toString('$import_time'), ' 00:00:00'))),1)),' 00:00:00')))
            AND (source = 'Einstein')
            AND (order_CNYamount > 0.5)
            GROUP BY
                dro.agent_name,
                dro.data_plan_name
            ORDER BY profit_rate DESC
        )
        GROUP BY agent_name
    )
    ARRAY JOIN
        array_order_number,
        array_data_plan_name,
        array_order_sales,
        array_order_cost,
        array_order_net_amount,
        array_profit_rate,
        rank
) AS t
WHERE t.rank <= 10;
"