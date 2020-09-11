#!/bin/bash
source /etc/profile
date=`date +%Y-%m-%dT%H:%M`
#date=`date --date='8 hour' +%Y%m%d%H%M`
file="/data/mtx/"
cc=""

for fileName in `ls $file`
        do
                bb=${fileName:12:16}
                if [[ $bb > $cc ]] ; then
                   cc=$bb
                fi
        done
dcc="${cc:0:4}${cc:5:2}${cc:8:2} ${cc:11:2}:${cc:14:2}"

while [[ $cc < $date ]]
do
	dcc=`date -d "$dcc 15 minute" +"%Y%m%d %H:%M"`
	cc="${dcc:0:4}-${dcc:4:2}-${dcc:6:2}T${dcc:9:2}:${dcc:12:2}"
	rgx="redteamobile${cc}*"
sftp -oIdentityFile=/data/rsa/id_rsa_redtea -oPort=8022 redtea@cdr-sftp.mtxc.eu <<!
lcd /data/mtx/
get $rgx
!
	for ff in `ls $file$rgx`
		do	
			java -jar /home/ops/ods/import_Newton/importData.jar $ff
		done 
done
