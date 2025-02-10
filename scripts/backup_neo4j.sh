#!/bin/bash

DB_NAME=neo4j

function backup {
    neo4j-admin database dump neo4j --to-stdout | zstd -c > ../db_backups/"$DB_NAME"_wallets_data_`date +%F`.neo4j.zst
}

backup