from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .config("spark.hadoop.hadoop.security.authentication", "simple") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("price", DoubleType()),
    StructField("event_timestamp", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Converter para TimestampType
parsed_df = parsed_df.withColumn(
    "event_timestamp",
    to_timestamp(col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
)

# Criar coluna de partição
parsed_df = parsed_df.withColumn(
    "event_date",
    to_date(col("event_timestamp"))
)

# Watermark (agora funciona)
parsed_df = parsed_df.withWatermark("event_timestamp", "10 minutes")

# Console (SEM path)
query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()