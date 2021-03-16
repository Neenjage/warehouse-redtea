##原MYSQL的sql语句
select
    m.name as merchant_name,
    c.name as carrier_name,
    count(distinct it.imsi) as count,
    prp.price,
    count(distinct it.imsi) *prp.price as cost ,
    c.id
from carrier c ,imsi_transaction it,bundle b ,merchant m,prepaid_resource_price prp
where c.id = b.carrier_id
and it.bundle_id = b.code
and c.implementation = 'ONCE_ONLY'
and it.merchant_id = m.id
and it.generate_time between DATE_SUB(curdate(),INTERVAL 1 DAY) and DATE_SUB(curdate(),INTERVAL 0 DAY)
and c.id = prp.carrier_id
group by c.id,m.name;


##clickhouse的sql语句
SELECT
    total.*,
    prp.price,
    total.count * prp.price AS cost
FROM
(
    SELECT
        t.carrier_id,
        t.carrier_name,
        t.merchant_name,
        countDistinct(t.imsi) AS count
    FROM
    (
        SELECT
            t2.*,
            m.name AS merchant_name
        FROM
        (
            SELECT
                t1.*,
                c.name AS carrier_name
            FROM
            (
                SELECT
                    it.imsi,
                    it.merchant_id,
                    it.bundle_id,
                    b.carrier_id
                FROM
                (
                    SELECT *
                    FROM ods.ods_Bumblebee_imsi_transaction
                    WHERE (generate_time >= (now() - toIntervalDay(1))) AND (generate_time <= (now() - toIntervalDay(0)))
                ) AS it
                LEFT JOIN dim.dim_Bumblebee_bundle AS b ON it.bundle_id = b.code
            ) AS t1
            INNER JOIN
            (
                SELECT *
                FROM dim.dim_Bumblebee_carrier
                WHERE implementation = 'ONCE_ONLY'
            ) AS c ON t1.carrier_id = c.id
        ) AS t2
        LEFT JOIN dim.dim_Bumblebee_merchant AS m ON t2.merchant_id = m.id
    ) AS t
    GROUP BY
        t.carrier_id,
        t.carrier_name,
        t.merchant_name
) AS total
LEFT JOIN dim.dim_Bumblebee_prepaid_resource_price AS prp ON total.carrier_id = prp.carrier_id