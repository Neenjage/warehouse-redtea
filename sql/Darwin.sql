##原mysql的sql语句
select
    m.name,
    c.name as carrierName,
    count(distinct itcr.imsi) as imsiCount ,
    truncate(SUM(itcr.upload + itcr.download)/1024/1024/1024,2) as dataUsage,
    itcr.price as price,
    truncate((SUM(itcr.upload + itcr.download) / 1073741824) * itcr.price, 2) AS cost,
    itcr.country as country,
    itcr.location_code as localCarrier
from carrier c, imsi_transaction_cdr_raw itcr,imsi_profile ip,merchant m
where itcr.imsi = ip.imsi
and ip.carrier_id = c.id
and itcr.end_time between DATE_SUB(curdate(),INTERVAL 1 DAY) and DATE_SUB(curdate(),INTERVAL 0 DAY)
and m.id = itcr.merchant_id
group by itcr.location_code,c.name,m.id
order by m.name,cost desc;


#clickhouse的sql语句
SELECT
    t.merchant_name,
    t.carrier_name,
    countDistinct(t.imsi) AS imsi_count,
    ((SUM(t.upload + t.download) / 1024) / 1024) / 1024 AS data_usage,
    max(t.price) AS unit_price,
    SUM(((t.upload + t.download) * t.price) / 1073741824) AS cost,
    max(t.country) AS country,
    t.location_code AS local_carrier
FROM
(
    SELECT
        itcr.imsi as imsi,
        itcr.upload as upload,
        itcr.download as download,
        itcr.price as price,
        itcr.country as country,
        itcr.location_code as location_code,
        itcr.merchant_id as merchant_id,
        ip.carrier_id as carrier_id,
        m.name AS merchant_name,
        c.name AS carrier_name
    FROM
    (
        SELECT
            imsi,
            upload,
            download,
            price,
            country,
            location_code,
            merchant_id
        FROM ods.ods_Bumblebee_imsi_transaction_cdr_raw
        WHERE (end_time >= (today() - toIntervalDay(1))) AND (end_time <= (today() - toIntervalDay(0)))
    ) AS itcr
    LEFT JOIN ods.ods_Bumblebee_imsi_profile AS ip ON itcr.imsi = ip.imsi
    LEFT JOIN dim.dim_Bumblebee_merchant AS m ON itcr.merchant_id = m.id
    LEFT JOIN dim.dim_Bumblebee_carrier AS c ON ip.carrier_id = c.id
) AS t
GROUP BY
    merchant_name,
    location_code,
    carrier_name;