from pyspark.sql.types import StructField, StructType, StringType, ArrayType, LongType, BooleanType

tx_schema=StructType([
    StructField('id', StringType()),
    StructField('inputs', ArrayType(StructType([
        StructField('index', LongType()),
        StructField('transaction', StructType([
            StructField('id', StringType())
        ]))
    ]))),
    StructField('outputs', ArrayType(StructType([
        StructField('address', StringType()),
        StructField('datum', StringType()),
        StructField('value', StringType())
    ]))),
    StructField('fee', StringType())
])

block_schema=StructType([
    StructField('py/state', StructType([
        StructField('blocktype', StringType()),
        StructField('era', StringType()),
        StructField('height', LongType()),
        StructField('id', StringType()),
        StructField('slot', LongType()),
        StructField('transactions', ArrayType(tx_schema))
    ]))
])

blockfrost_inputs_schema = ArrayType(StructType([
    StructField('address', StringType(), True),
    StructField('amount', ArrayType(
    StructType([
        StructField('quantity', StringType(), True),
        StructField('unit', StringType(), True)
    ]), True), True),
    StructField('collateral', BooleanType(), True),
    StructField('data_hash', StringType(), True),
    StructField('inline_datum', StringType(), True),
    StructField('output_index', LongType(), True),
    StructField('reference', BooleanType(), True),
    StructField('reference_script_hash', StringType(), True),
    StructField('tx_hash', StringType(), True)
]))