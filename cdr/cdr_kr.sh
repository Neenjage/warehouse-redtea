#!/bin/bash
source /etc/profile
date=`date +%Y%m%d%H%M`
#date=`date --date='8 hour' +%Y%m%d%H%M`
file="/data/knowroaming/"
cc=""

for fileName in `ls $file`
        do
                bb=${fileName:16:12}
                if [[ $bb > $cc ]] ; then
                   cc=$bb
                fi
        done
dcc="${cc:0:8} ${cc:8:2}:${cc:10:2}"

while [[ $cc < $date ]]
do
	dcc=`date -d "$dcc 1 minute" +"%Y%m%d %H:%M"`
	cc="${dcc:0:8}${dcc:9:2}${dcc:12:2}"
	rgx="LIVE_RedTea_CDR_${cc}*"
#sftp -oIdentityFile=/data/rsa/mvno_rt_245 -oPort=8922 mvno_rt_v28h@203.160.90.245 <<!
lftp -u redtea_dur,3YhdxvZQWGWZ sftp://172.31.18.221:2222 <<!
cd /dur_save/knowroaming_save
lcd /data/knowroaming
mirror -n -I $rgx
!
lftp -u knowroaming_dur,qHxvDlqSL6Uf sftp://172.31.18.221:2222 <<!
cd /upload
lcd /data/knowroaming
mirror -n -I $rgx
!
	for ff in `ls $file$rgx`
		do	echo $ff
			java -jar /home/ops/ods/import_Newton/importData.jar $ff
		done
done
