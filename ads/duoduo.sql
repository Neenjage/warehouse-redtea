
select sum(o.amount)/100 as amount
from orders o,data_plan dp
where o.status not in ('RESERVED','REFUNDED','REFUNDING')
and o.data_plan_id = dp.id
and o.amount > 10
and o.payment_method_id = 4
and o.agent_id = 9
and end_time between '2020-10-01' and '2020-11-01'



SELECT
    DestCityName,
    any(total),
    avg(abs(monthly * 12 - total) / total) AS avg_month_diff
FROM
    (
        SELECT
            DestCityName,
            count() AS total
        FROM ontime
        GROUP BY DestCityName
        HAVING total > 100000
    ) as t
ALL INNER JOIN
(
    SELECT
        DestCityName,
        Month,
        count() AS monthly
    FROM ontime
    GROUP BY DestCityName, Month
    HAVING monthly > 10000
) as t1
USING DestCityName
GROUP BY DestCityName
ORDER BY avg_month_diff DESC
LIMIT 20

