clickhouse-client  -u$1 --multiquery -q"
CREATE TABLE IF NOT EXISTS ods.ods_Bethune_user_rel_group
ENGINE = MergeTree
ORDER BY id AS
SELECT *
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user_rel_group', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
 "


 clickhouse-client -u$1 --multiquery -q"
CREATE TABLE ods.ods_Bethune_user_rel_group_temp
ENGINE = MergeTree
ORDER BY id AS
SELECT
    id,
    group_id,
    user_id,
    status,
    create_time,
    update_time,
    remark,
    effective_time,
    if(b.id = 0, a.invalid_time, b.update_time) AS invalid_time
FROM ods.ods_Bethune_user_rel_group AS a
ANY LEFT JOIN
(
    SELECT
        id,
        update_time
    FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user_rel_group', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
    WHERE update_time >
    (
        SELECT MAX(update_time)
        FROM ods.ods_Bethune_user_rel_group
    )
) AS b USING (id)
";

clickhouse-client  -u$1 --multiquery -q"
DROP table ods.ods_Bethune_user_rel_group;
"

clickhouse-client  -u$1 --multiquery -q"
RENAME table ods.ods_Bethune_user_rel_group_temp to ods.ods_Bethune_user_rel_group;
"


clickhouse-client  -u$1 --multiquery -q"
INSERT INTO ods.ods_Bethune_user_rel_group(
    id,
    group_id,
    user_id,
    status,
    create_time,
    update_time,
    remark,
    effective_time,
    invalid_time)
SELECT
    id,
    group_id,
    user_id,
    status,
    create_time,
    update_time,
    remark,
    update_time as effective_time,
    toDateTime('2105-12-31 23:59:59') as invalid_time
FROM mysql('db-cnbj-prod.c34nqvzohzfw.rds.cn-north-1.amazonaws.com.cn:3306', 'Bethune', 'user_rel_group', 'he.jin', 'MUtxodhUx9yD507UDHz2ebD3HbKmHLrXm')
WHERE update_time >  (select MAX(update_time) from ods.ods_Bethune_user_rel_group);
"

