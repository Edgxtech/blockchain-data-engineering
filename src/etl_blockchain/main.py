from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from util import schema
import time

spark = (SparkSession.builder.appName("CardanoETLBlockchain")
            .getOrCreate())

streamer_address="127.0.0.1"
streamer_port=12345
raw_data = spark \
    .readStream \
    .format("socket") \
    .option("host", streamer_address) \
    .option("port", streamer_port) \
    .option('includeTimestamp', True) \
    .load()

blocks = (raw_data.select(from_json(col("value"), schema.block_schema).alias("json"))
          .select("json.py/state.*"))

db_url = "jdbc:postgresql://localhost:5432/bde"
db_properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver",
    "numPartitions": "1",
    "stringtype": "unspecified"
}

def write_streaming(df, batch_id) -> None:
    print("Count: ", df.count(), ", Batch: ", batch_id)

    block = df \
        .select(col("id").alias("hash"), "height", "slot", "transactions") \
        .withColumn("id", expr("uuid()"))
    block.select("id", "hash", "height", "slot") \
        .write.jdbc(url=db_url, table="block", mode="append", properties=db_properties)

    ## Flatten out and structure transactions
    tx = block \
        .select(col("id").alias("block_id"), col("hash").alias("block_hash"), col("height"), col("slot"),
                explode(col("transactions")).alias("xtransactions")) \
        .select("block_id", "block_hash", "height", "slot", col("xtransactions.id").alias("hash"),
                col("xtransactions.outputs"), col("xtransactions.inputs"), col("xtransactions.fee")) \
        .withColumn("id", expr("uuid()"))
    tx.select("id", "block_id", "hash") \
        .write.jdbc(url=db_url, table="tx", mode="append", properties=db_properties)

    ## For now simply persisting input/outputs in jsonb, since uses policy as key, name as an inner object key, with value as its quantity
    ## We need a better way to parse this, will deal with it later
    tx_output = tx \
        .select(col("id").alias("tx_id"), to_json("outputs").alias("payload")) \
        .withColumn("id", expr("uuid()"))
    tx_output.write.jdbc(url=db_url, table="tx_output", mode="append", properties=db_properties)

    ## For now we will simply persist these Transaction Input references, we need a better way to resolve their details expecially for the case where this as a streaming app that may not start streaming from the beginning of the chains existence, thus we can't use self-indexed data.
    ## which will involve looking back over historically indexed information to understand their values
    ## For now assume the consumer of this indexed data will implement a join query
    tx_input = tx \
        .select(col("id").alias("tx_id"), to_json("inputs").alias("payload")) \
        .withColumn("id", expr("uuid()"))
    tx_input.write.jdbc(url=db_url, table="tx_input", mode="append", properties=db_properties)

def run():
    query = blocks.writeStream \
        .foreachBatch(lambda df, batch_id: write_streaming(df, batch_id)) \
        .start()
    time.sleep(30)
    query.stop()

if __name__ == "__main__":
    run()