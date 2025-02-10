from pyspark.sql.types import StructField, StructType, StringType, ArrayType, LongType

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