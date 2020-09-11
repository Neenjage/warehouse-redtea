#!/bin/bash
source /etc/profile
#date=`date +%Y%m%d%H%M`
date=`date --date='8 hour' +%Y%m%d%H%M`
file="/data/cuhk/"
cc=""

for fileName in `ls $file`
        do
                bb=${fileName:10:12}
                if [[ $bb > $cc ]] ; then
                   cc=$bb
                fi
        done

dcc="${cc:0:8} ${cc:8:2}:${cc:10:2}"

while [[ $cc < $date ]]
do
	dcc=`date -d "$dcc 1 minute" +"%Y%m%d %H:%M"`
	cc="${dcc:0:8}${dcc:9:2}${dcc:12:2}"
	rgx="CDR.GS.RT.${cc}*"
sftp -oIdentityFile=/data/rsa/mvno_rt_245 -oPort=8922 mvno_rt_v28h@203.160.90.245 <<!
cd /home/mvno_rt_v28h
lcd /data/cuhk/
get $rgx
!
	for ff in `ls $file$rgx`
		do	echo $ff
			java -jar /home/ops/ods/import_Newton/importData.jar $ff
		done 
done
