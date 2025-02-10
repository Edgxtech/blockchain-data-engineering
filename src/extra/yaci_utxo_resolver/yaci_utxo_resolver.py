from pyspark.sql.functions import *
from pyspark.sql.types import *
import configparser
import json
import pandas as pd
import requests

##
## These are snippets only, need to be substituted in 'etl_vol_transfers/main.py'
##

config = configparser.RawConfigParser()
config.read('app.properties')
config_dict = dict(config.items('APP'))
print(f'Using configs: {config_dict}')

## This is more targeted, call specific tx outputs, and allows batch requests
@udf(returnType=ArrayType(StructType([StructField('address', StringType(), True), StructField('amount', ArrayType(StructType([StructField('quantity', StringType(), True), StructField('unit', StringType(), True)]), True), True), StructField('data_hash', StringType(), True), StructField('inline_datum', StringType(), True), StructField('output_index', LongType(), True), StructField('reference_script_hash', StringType(), True), StructField('tx_hash', StringType(), True)])))
def fetch_inputs_yaci_multiple(txoutput_list: List):
    request_payload = json.dumps(list(map(lambda x: {'tx_hash': x["transaction"]["id"], 'output_index': x["index"]}, txoutput_list)))
    url = f"http://{config_dict['cardano.db.address']}:{config_dict['cardano.db.port']}/api/v1/utxos"
    headers = {'Content-type': 'application/json'}
    response = requests.post(url, data=request_payload, headers=headers)
    df = pd.DataFrame(pd.json_normalize(response.json()), columns=['owner_addr','amounts','data_hash', 'inline_datum', 'output_index','reference_script_hash', 'tx_hash'])
    df = df.rename(columns={'owner_addr': 'address', 'amounts': 'amount'})
    return df.to_dict(orient="records")

## A difference is we need to pass the 'inputs' column to fetch() instead of (tx) hash
## as with the Blockfrost method
def qualify_transactions(df) -> DataFrame:
    block = df \
        .select(col("id").alias("hash"), "height", "slot", "transactions") \
        .withColumn("id", expr("uuid()"))
    tx = block \
        .select(col("id").alias("block_id"), col("hash").alias("block_hash"), col("height"), col("slot"),
                explode(col("transactions")).alias("xtransactions")) \
        .select("block_id", "block_hash", "height", "slot", col("xtransactions.id").alias("hash"),
                col("xtransactions.outputs"), col("xtransactions.inputs"), col("xtransactions.fee")) \
        .withColumn("id", expr("uuid()"))
    tx_with_inputs = tx.select("height", "slot", "hash", "outputs", "fee", "inputs") \
        .withColumn("inputs", fetch_inputs_yaci_multiple("inputs"))
    return tx_with_inputs
