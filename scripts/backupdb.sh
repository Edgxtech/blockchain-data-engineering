#!/bin/bash
## Takes $1; name of data; 'plain', 'vol', 'vol_ext' or 'all

DB_NAME=bde

function backupPlain {
    pg_dump $DB_NAME -t block -t tx -t tx_input -t tx_output -U myuser | zstd -c > ../db_backups/"$DB_NAME"_$1_data_`date +%F`.sql.zst
}

function backupVol {
    pg_dump $DB_NAME -t vol -U myuser | zstd -c > ../db_backups/"$DB_NAME"_$1_data_`date +%F`.sql.zst
}

function backupVolExt {
    pg_dump $DB_NAME -t vol -t vol_by_block -t vol_all_time -U myuser | zstd -c > ../db_backups/"$DB_NAME"_$1_data_`date +%F`.sql.zst
}

function backupAll {
    pg_dump $DB_NAME -U myuser | zstd -c > ../db_backups/"$DB_NAME"_$1_data_`date +%F`.sql.zst
}

if [ -n "$1" ]; then
	if [[ $1 == *"-"* ]]; then
		echo "No backup data name specified (hint: use plain, vol, vol_ext or all)"
	else
	  if [[ $1 == "plain" ]]; then
	    backupPlain $1
		elif [[ $1 == "vol" ]]; then
	    backupVol $1
	  elif [[ $1 == "vol_ext" ]]; then
	    backupVolExt $1
		elif [[ $1 == "all" ]]; then
	    backupAll $1
	  else
	    echo "Wrong backup data name specified (hint: use plain, vol, vol_ext or all)"
	  fi
	fi
else
	echo "No params specified"
fi