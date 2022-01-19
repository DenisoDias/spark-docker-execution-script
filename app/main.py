from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, TimestampType
from pyspark.sql.functions import to_timestamp, row_number
from pyspark.sql.window import Window
from datetime import datetime

data_frame_input = [
    [201231, 'rio de janeiro', 'rj', '2022-01-15T10:32:00'],
    [161110, 'sao paulo', 'sp', '2022-01-12T17:25:13'],
    [202310, 'curitiba', 'pr', '2022-01-16T23:14:37'],
    [135211, 'buzios', 'rj', '2022-01-05T09:12:21'],
    [124321, 'campinas', 'sp', '2022-01-02T09:37:12'],
    [124124, 'sao bernardo do campo', 'sp', '2022-01-01T14:22:10'],
    [202350, 'porto alegre', 'rs', '2022-01-17T21:46:23'],
    [152321, 'nova iguacu', 'rj', '2022-01-08T11:25:21'],
    [201320, 'sao paulo', 'sp', '2022-01-15T23:21:17'],
    [201232, 'rio de janeiro', 'rj', '2022-01-15T11:30:00'],
    [202355, 'porto alegre', 'rs', '2022-01-17T22:43:23']
]

spark = SparkSession.builder.appName('ESS').getOrCreate()

schema = StructType(
    [StructField('transacao', IntegerType(), True),
    StructField('municipio', StringType(), True),
    StructField('estado', StringType(), True),
    StructField('data_de_atualizacao', StringType(), True),
    ]
)

df = spark.createDataFrame(data_frame_input, schema=schema)
df = df.withColumn('data_de_atualizacao_conv', to_timestamp(df.data_de_atualizacao)).\
    drop('data_de_atualizacao').\
    withColumnRenamed('data_de_atualizacao_conv', 'data_de_atualizacao')

df = df.orderBy('data_de_atualizacao')
df = df.withColumn('transacao_por_municipio', row_number().over(
    Window.partitionBy('municipio').orderBy('data_de_atualizacao')))

df.show()
