from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def create_spark_session():
    return (SparkSession.builder
        .appName("CryptoOHLCV")
        .master("spark://spark-master:7077")
        .getOrCreate())

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Read dari Kafka ──────────────────────────────────────
    raw_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "crypto-prices")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())

    # ── 2. Parse JSON ───────────────────────────────────────────
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("symbol",    StringType()),
        StructField("price",     DoubleType()),
        StructField("bid",       DoubleType()),
        StructField("ask",       DoubleType()),
        StructField("volume",    DoubleType()),
        StructField("source",    StringType()),
    ])

    parsed_df = (raw_df
        .select(
            F.from_json(F.col("value").cast("string"), schema).alias("data")
        )
        .select("data.*")
        .filter(F.col("symbol").isNotNull())
        .withColumn("event_time", F.to_timestamp("timestamp"))
    )

    # ── 3. Watermark + Window OHLCV ────────────────────────────
    ohlcv_df = (parsed_df
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "1 minute"),
            F.col("symbol")
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("volume").alias("volume"),
            F.count("*").alias("trade_count"),
        )
        .select(
            F.col("window.start").alias("time"),
            F.col("symbol"),
            F.col("open"), F.col("high"),
            F.col("low"),  F.col("close"),
            F.col("volume"), F.col("trade_count"),
        )
    )

    # ── 4. Sink ke TimescaleDB ──────────────────────────────────
    def write_to_timescale(batch_df, batch_id):
        count = batch_df.count()
        print(f"Batch {batch_id}: processing {count} rows")
        if count == 0:
            print(f"Batch {batch_id}: empty, skipping")
            return
        try:
            (batch_df.write
                .format("jdbc")
                .option("url", "jdbc:postgresql://postgres:5432/crypto_db")
                .option("dbtable", "ohlcv_1min")
                .option("user", "pipeline_user")
                .option("password", "pipeline_secret_2024")
                .option("driver", "org.postgresql.Driver")
                .option("isolationLevel", "READ_COMMITTED")
                .mode("append")
                .save())
            print(f"Batch {batch_id}: successfully wrote {count} rows!")
        except Exception as e:
            print(f"Batch {batch_id}: FAILED - {str(e)}")

    query = (ohlcv_df.writeStream
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .foreachBatch(write_to_timescale)
        .option("checkpointLocation", "/opt/spark/checkpoints/ohlcv")
        .start())

    query.awaitTermination()

if __name__ == "__main__":
    main()