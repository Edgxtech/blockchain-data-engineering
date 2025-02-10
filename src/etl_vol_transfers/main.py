from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import configparser

config = configparser.RawConfigParser()
config.read('app.properties')
config_dict = dict(config.items('APP'))
print(f'Using configs: {config_dict}')

spark = (SparkSession.builder.appName("CardanoETLVolAndTransfers")
            .getOrCreate())

raw_data = spark \
    .readStream \
    .format("socket") \
    .option("host", config_dict['cardano.streamer.address']) \
    .option("port", config_dict['cardano.streamer.port']) \
    .option('includeTimestamp', True) \
    .load()

# Parse the socket 'value' field as json
from util import schema
blocks = (raw_data.select(from_json(col("value"), schema.block_schema).alias("json"))
          .select("json.py/state.*"))

from blockfrost import BlockFrostApi

bfApi = BlockFrostApi(
    project_id=config_dict['cardano.db.apikey'],
    #base_url=ApiUrls.mainnet.value
    base_url=config_dict['cardano.db.address']
)

@udf(returnType=schema.blockfrost_inputs_schema)
def fetch_inputs(tx_hash):
    response = bfApi.transaction_utxos(hash=tx_hash)
    result = [input for input in response.inputs if input.collateral == False]
    return result

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
        .withColumn("inputs", fetch_inputs("hash"))
    return tx_with_inputs.limit(10)

input_column_names=['address', 'amount', 'collateral', 'data_hash', 'inline_datum', 'output_index','reference', 'reference_script_hash', 'tx_hash']
output_column_names=['address', 'datum', 'value']

def sum_ip_vals_for_addr(ip_gp_df):
    reform = ip_gp_df['amount'].apply(lambda y: [(v, int(k)) for k, v in y])
    flat = reform.apply(pd.Series).unstack().reset_index().dropna()
    ip_addr_vals = flat[0].apply(pd.Series).groupby(0)[1].sum().reset_index()
    ip_addr_vals.columns = ['unit','value']
    return ip_addr_vals

import json
def sum_op_vals_for_addr(op_gp_df):
    parsed = op_gp_df["value"].apply(lambda x: json.loads(x))
    flat = parsed.apply(pd.Series).unstack().reset_index().dropna()
    flat.columns = (['policy', '_', 'name_value_pair'])
    flat = flat.drop(columns=['_'])
    reformed = pd.concat([
        flat.drop(columns=['name_value_pair']),
        flat['name_value_pair'].apply(lambda x: pd.Series(x.items()))
    ], axis=1)
    melted = reformed.melt('policy').dropna().drop(columns=['variable'])
    melted.columns = ['policy', 'name_value_pair']
    melted[['name', 'value']] = pd.DataFrame(melted['name_value_pair'].tolist(), index=melted.index)
    melted["unit"] = melted["policy"] + melted["name"]
    melted["unit"] = melted["unit"].apply(lambda x: x if x != 'adalovelace' else 'lovelace')
    melted = melted.drop(['policy', 'name', 'name_value_pair'], axis=1)
    op_addr_vals = melted.groupby("unit").sum()
    return op_addr_vals
    return op_addr_vals

@udf(returnType=ArrayType(StructType([StructField('address', StringType()), StructField('unit', StringType()), StructField('value', LongType())])))
def get_transacted_actions(op,ip):
    ip_gp_by_addr = pd.DataFrame(ip, columns=input_column_names).groupby('address')[['amount']].apply(lambda x: sum_ip_vals_for_addr(x))
    op_gp_by_addr = pd.DataFrame(op, columns=output_column_names).groupby('address')[['value']].apply(lambda x: sum_op_vals_for_addr(x))
    transacted = pd.merge(ip_gp_by_addr, op_gp_by_addr, on=['address','unit'], how='outer').fillna(0)
    transacted['diff']=transacted['value_y']-transacted['value_x']
    transacted = transacted.drop(['value_x','value_y'], axis=1)
    transacted = transacted.loc[transacted['diff'] != 0]
    transacted['diff'] = transacted[["diff"]].astype(int)
    return list(transacted.itertuples(name=None))

with open('data/prices/response_1734675990113.json','r') as file:
    prices_data = json.load(file)

with open('data/decimals/decimals.json','r') as file:
    decimals_data = json.load(file)

import pandas as pd
decimals_df = pd.DataFrame(decimals_data).set_index("unit")
prices_df = pd.DataFrame(prices_data['assets']).rename(columns={'id': 'unit'}).set_index("unit")
merged_price_map = prices_df.merge(decimals_df, on="unit", how="left").T.to_dict()

# Broadcast for use by all Spark executors
br_price_map = spark.sparkContext.broadcast(merged_price_map)

def get_adjusted_price(row):
    adj_price=0
    if row.unit == 'lovelace':
        adj_price = row.value / 10 ** 6
    else:
        price = br_price_map.value.get(row.unit)
        if price is not None:
            if price["decimals"] is not None and price["decimals"] != 0:
                adj_price = (row.value * price["last_price_ada"]) / 10 ** price["decimals"]
            else:
                adj_price = row.value * price["last_price_ada"]
    return adj_price

@udf(returnType=ArrayType(StructType([StructField('unit', StringType()), StructField('value_adj', DoubleType())])))
def reduce_vol(transacted):
    tdf = pd.DataFrame(transacted, columns=['address', 'unit', 'value'])
    filtered = tdf.loc[tdf['value'] > 0]
    filtered = filtered.assign(value_adj=filtered[["value","unit"]].apply(lambda x: get_adjusted_price(x), axis=1))
    return list(filtered.groupby('unit')[['value_adj']].sum().itertuples(name=None))

db_url = f"jdbc:postgresql://{config_dict['app.database.address']}:{config_dict['app.database.port']}/bde"
db_properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver",
    "numPartitions": "1",
    "stringtype": "unspecified"
}

@udf(returnType=ArrayType(StructType([StructField("unit", StringType()), StructField("rx_addr", StringType()), StructField("value", LongType()), StructField("send_addr", StringType()), StructField("value_adj", DoubleType())])))
def compute_transfers(transacted) -> list:
    transacted_df = pd.DataFrame(transacted, columns=['address', 'unit', 'value']).set_index(['unit'])
    transfers_by_token = transacted_df.groupby("unit")[['address','value']] \
        .apply(lambda x: compute_transfers_by_token(x)) \
        .reset_index() \
        .drop(['level_1'], axis=1)
    #transfers_by_token = transfers_by_token.assign(value_adj=transfers_by_token[["value", "unit"]].apply(lambda x: x.value * getPrice(x.unit)["last_price_ada"] if x.unit != "lovelace" else x.value, axis=1))
    transfers_by_token = transfers_by_token.assign(value_adj=transfers_by_token[["value", "unit"]].apply(lambda x: get_adjusted_price(x), axis=1))
    return list(transfers_by_token.set_index('unit').itertuples(name=None))

def compute_transfers_by_token(unit_gp_df):
    senders = unit_gp_df.loc[unit_gp_df['value'] < 0]
    senders = senders.assign(value=senders['value'].abs())
    receivers = unit_gp_df.loc[unit_gp_df['value']>0]
    transfers = receivers.merge(pd.Series(senders['address'], name='send_addr'), how='cross') \
        .rename(columns={'address': 'rx_addr'})
    return transfers

neo4j_properties = {
    "user": "neo4j",
    "password": "mypassword",
    "url": f"neo4j://{config_dict['app.graph.database.address']}:{config_dict['app.graph.database.port']}",
    "db_name": "neo4j"
}

def write_streaming(df, _batch_id) -> None:
    tx_with_inputs = qualify_transactions(df)
    ## Compute the transacted actions
    transacted_by_tx = tx_with_inputs.groupBy("hash").agg(
        get_transacted_actions(any_value("outputs"), any_value("inputs")).alias("transacted"))
    ## Join to create a master DF with all properties
    transacted_by_tx_all = transacted_by_tx.join(tx_with_inputs.select("hash", "height", "slot"), "hash", "inner")
    ## Add volumes exchanged
    vols_by_tx = transacted_by_tx_all.withColumn("vol", reduce_vol("transacted"))
    ## Persist volumes data
    vols_by_tx.select("hash", "height", "slot", explode("vol").alias("vol"))\
        .select("hash", "height", "slot","vol.unit", "vol.value_adj") \
        .write.jdbc(url=db_url, table="vol", mode="append", properties=db_properties)
    ## Compute transfers
    transfers = transacted_by_tx.withColumn("actions", compute_transfers("transacted"))
    ## Persist properties to persist in Neo4j
    transfersx = transfers.select(explode("actions").alias("actions"),"hash").select("actions.*",col("hash").alias("tx_hash"))
    ## Persist transfers data
    ( transfersx.write
        .mode("Overwrite")
        .format("org.neo4j.spark.DataSource")
        .option("url", neo4j_properties["url"])
        .option("authentication.basic.username", neo4j_properties["user"])
        .option("authentication.basic.password", neo4j_properties["password"])
        .option("database", neo4j_properties["db_name"])
        .option("relationship", "SENT_TO")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Wallet")
        .option("relationship.source.node.keys", "send_addr:address")
        .option("relationship.source.node.properties", "send_addr:address")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Wallet")
        .option("relationship.target.node.keys", "rx_addr:address")
        .option("relationship.target.node.properties", "rx_addr:address")
        .option("relationship.properties", "tx_hash,unit,value,value_adj:value_ada")
        .save() )

def run_streaming_job(time_seconds):
    query = blocks.writeStream \
        .foreachBatch(lambda df, batch_id: write_streaming(df, batch_id)) \
        .start()
    import time
    time.sleep(time_seconds)
    query.stop()

if __name__ == "__main__":
    run_streaming_job(time_seconds=200)