#!/bin/bash

DB_NAME=neo4j
DATE="2025-02-06"

function restore {
    zstd -d -c ../db_backups/"$DB_NAME"_wallets_data_"$DATE".neo4j.zst | neo4j-admin database load neo4j --from-stdin --overwrite-destination true --verbose
}

restore