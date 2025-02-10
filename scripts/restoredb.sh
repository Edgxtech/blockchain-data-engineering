#!/bin/bash
## Takes $1; name of data; 'plain', 'vol', 'vol_ext' or 'all

DB_NAME=bde
DATE="2025-02-06"

function restore {
    zstd -d -c ../db_backups/"$DB_NAME"_$1_data_"$DATE".sql.zst | psql $DB_NAME
}

if [ -n "$1" ]; then
	if [[ $1 == *"-"* ]]; then
		echo "No backup data name specified (hint: use plain, vol, vol_ext or all)"
	else
		restore $1
	fi
else
	echo "No params specified"
fi