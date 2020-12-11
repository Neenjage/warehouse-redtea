source /home/ops/warehouse-redtea/config/config.sh

import_time=`date +%F`

if [ -n "$1" ];then
  import_time=$1
fi

clickhouse-client --user $user --password $password --multiquery --multiline -q"
drop table if exists dwd.dwd_Bethune_user_detail_tmp;

CREATE TABLE dwd.dwd_Bethune_user_detail_tmp
ENGINE = MergeTree
ORDER BY user_id AS
SELECT
    user.id AS user_id,
    user.telephone AS user_telephone,
    user.status AS user_status,
    user.recommend_user,
    user.create_time,
    user.login_time,
    user.is_valid,
    user_device.imei,
    user_device.device_id,
    user_device.model,
    lower(user_device.brand) AS brand
FROM
(
    SELECT
        id,
        telephone,
        status,
        recommend_user,
        create_time,
        login_time,
        is_valid
    FROM ods.ods_Bethune_user
) AS user
LEFT JOIN
(
    SELECT
        user_id,
        max(imei) AS imei,
        max(device_id) AS device_id,
        max(model) AS model,
        max(brand) AS brand
    FROM ods.ods_Bethune_user_device
    GROUP BY user_id
) AS user_device ON user.id = user_device.user_id;

drop table if exists dwd.dwd_Bethune_user_detail;

rename table dwd.dwd_Bethune_user_detail_tmp to dwd.dwd_Bethune_user_detail;
"