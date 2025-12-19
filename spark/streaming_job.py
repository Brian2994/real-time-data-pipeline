from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# -----------------------------
# Criando SparkSession
# -----------------------------
spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .config("spark.hadoop.hadoop.security.authentication", "simple") \
    .getOrCreate()

# Configurar log
spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Definindo schema dos eventos
# -----------------------------
schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("price", DoubleType()),
    StructField("event_timestamp", StringType())
])

# -----------------------------
# Lendo dados do Kafka
# -----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

# -----------------------------
# Parse do JSON
# -----------------------------
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# -----------------------------
# Converter event_timestamp para TimestampType
# -----------------------------
parsed_df = parsed_df.withColumn(
    "event_timestamp",
    to_timestamp(col("event_timestamp"))
)

# -----------------------------
# Adicionar watermark para lidar com dados atrasados
# -----------------------------
parsed_df = parsed_df.withWatermark("event_timestamp", "10 minutes")

# -----------------------------
# Escrevendo dados no console (para testes)
# -----------------------------
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "/opt/data_lake/bronze/ecommerce_events") \
    .option("checkpointLocation", "/opt/data_lake/checkpoints/ecommerce_events") \
    .outputMode("append") \
    .start()

# -----------------------------
# Manter a aplicação rodando
# -----------------------------
query.awaitTermination()