SELECT
sum(use)
FROM
(SELECT imsi,plmn,sum(download) as use FROM ods.ods_Bumblebee_imsi_transaction_cdr_raw x
WHERE imsi in (SELECT
t.imsi
FROM
(SELECT m.imsi,m.plmn,m.use_byte,t.use FROM ods.mb_12 m  left join
(SELECT
imsi,
case when plmn = '302720' then '302072'
     when plmn = '310410' then '310041'
     when plmn = '310260' then '310026'
else plmn end as plmn1,sum(download) as use FROM ods.ods_Bumblebee_imsi_transaction_cdr_raw x
WHERE start_time >= '2020-11-30 18:00:00' AND start_time < '2020-12-31 16:00:00'
GROUP by plmn1,imsi) t on m.imsi = t.imsi and m.plmn = t.plmn1) t
where t.use is NULL and t.use_byte > 0) AND start_time >= '2020-11-30 18:00:00' AND start_time < '2020-12-31 16:00:00'
GROUP by imsi,plmn)




SELECT
sum(t.use_byte)/1024/1024/1024
FROM
(SELECT m.imsi,m.plmn,m.use_byte,t.use FROM ods.mb_12 m  left join
(SELECT
imsi,
case when plmn = '302720' then '302072'
     when plmn = '310410' then '310041'
     when plmn = '310260' then '310026'
else plmn end as plmn1,sum(download) as use FROM ods.ods_Bumblebee_imsi_transaction_cdr_raw x
WHERE start_time >= '2020-11-30 18:00:00' AND start_time < '2020-12-31 16:00:00'
GROUP by plmn1,imsi) t on m.imsi = t.imsi and m.plmn = t.plmn1) t
where t.use is NULL and t.use_byte > 0;


INSERT INTO ods.mb_total
SELECT
t2.imis_start,
t2.roam_area,
t2.imsi,
t2.plmn,
t2.use_byte ,
case when t2.imis_start = '45403' and t2.plmn = '46000' then 16
     when t2.imis_start = '45406' and t2.plmn = '46000' then 525
     else 0 end as days,
case when t2.imis_start = '45403' and t2.plmn = '50218' then 30
  	 when t2.imis_start = '45403' and t2.plmn = '50219' then 18
  	 when t2.imis_start = '45403' and t2.plmn = '45005' then 26
  	 when t2.imis_start = '45403' and t2.plmn = '45008' then 16
  	 when t2.imis_start = '45403' and t2.plmn = '46000' then 17
  	 when t2.imis_start = '45403' and t2.plmn = '46001' then 70
  	 when t2.imis_start = '45406' and t2.plmn = '46001' then 100
  	 when t2.imis_start = '45406' and t2.plmn = '46000' then 12
     else unit_price
 end as unit_price
FROM
(SELECT
	t1.imis_start,
	t1.roam_area,
	t1.imsi,
	t1.plmn,
	t1.use_byte,
	t2.unit_price
FROM ods.mb_12 t1
left join ods.mb_12SUM t2
on t1.imis_start = t2.sim_type
AND t1.roam_area = t2.foam_area) t2;