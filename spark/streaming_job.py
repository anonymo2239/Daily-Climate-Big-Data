"""
Climate Data Streaming Job
==========================
Kafka'dan akan iklim verilerini okur, Bronze/Silver/Gold katmanları
şeklinde Delta Lake'e yazar.

Bu script notebook'taki (03_spark_streaming.ipynb) tüm streaming
mantığını standalone biçimde içerir; prodüksiyon ortamında
spark-submit ile çalıştırılabilir.

Çalıştırma:
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 \\
        streaming_job.py
"""

import os
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, to_timestamp, current_timestamp,
    year, month, dayofmonth, dayofweek, weekofyear, quarter,
    when, lit, length, avg, sum as _sum, count, max as _max,
    min as _min, stddev, round as _round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)


# ---------- Konfigürasyon ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "climate-events")

DELTA_BASE = os.getenv("DELTA_BASE", "/home/jovyan/work/delta-lake")

BRONZE_PATH = f"{DELTA_BASE}/bronze/climate_events"
SILVER_PATH = f"{DELTA_BASE}/silver/climate_events"
GOLD_FEATURES_PATH = f"{DELTA_BASE}/gold/climate_features"
GOLD_SUMMARY_PATH = f"{DELTA_BASE}/gold/city_monthly_summary"

CHECKPOINT_BASE = f"{DELTA_BASE}/_checkpoints"

TRIGGER_INTERVAL = "10 seconds"


# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ClimateStreamingJob")


# ---------- Şema ----------
CLIMATE_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("measurement_date", StringType(), True),
    StructField("season", StringType(), True),
    StructField("avg_temp_c", DoubleType(), True),
    StructField("min_temp_c", DoubleType(), True),
    StructField("max_temp_c", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
    StructField("snow_depth_mm", DoubleType(), True),
    StructField("avg_wind_dir_deg", DoubleType(), True),
    StructField("avg_wind_speed_kmh", DoubleType(), True),
    StructField("peak_wind_gust_kmh", DoubleType(), True),
    StructField("avg_sea_level_pres_hpa", DoubleType(), True),
    StructField("sunshine_total_min", DoubleType(), True),
])


def create_spark_session() -> SparkSession:
    """Delta Lake destekli Spark Session oluşturur."""
    spark = (
        SparkSession.builder
        .appName("ClimateStreaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_bronze_stream(spark: SparkSession):
    """Kafka'dan oku, JSON parse et, ham veriyi Bronze formatında hazırla."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze = (
        raw.selectExpr(
            "CAST(value AS STRING) as json_str",
            "topic", "partition", "offset",
            "timestamp as kafka_timestamp"
        )
        .select(
            from_json(col("json_str"), CLIMATE_SCHEMA).alias("data"),
            col("topic"), col("partition"), col("offset"),
            col("kafka_timestamp"),
            col("json_str").alias("raw_json"),
        )
        .select(
            "data.*", "topic", "partition", "offset",
            "kafka_timestamp", "raw_json",
            current_timestamp().alias("ingestion_time"),
        )
    )
    return bronze


def build_silver_stream(spark: SparkSession):
    """Bronze'dan oku, temizle ve zenginleştir."""
    src = spark.readStream.format("delta").load(BRONZE_PATH)

    silver = (
        src
        .withColumn("measurement_date", to_date(col("measurement_date")))
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        # Producer bug-fix kalıntısı: tarih formatlı season'ı NULL'a çek
        .withColumn(
            "season",
            when(length(col("season")) > 10, lit(None)).otherwise(col("season"))
        )
        # Veri kalitesi filtreleri
        .filter(
            (col("avg_temp_c").isNull()) |
            ((col("avg_temp_c") >= -90) & (col("avg_temp_c") <= 60))
        )
        .filter(
            (col("min_temp_c").isNull()) |
            ((col("min_temp_c") >= -90) & (col("min_temp_c") <= 60))
        )
        .filter(
            (col("max_temp_c").isNull()) |
            ((col("max_temp_c") >= -90) & (col("max_temp_c") <= 60))
        )
        .filter(col("measurement_date").isNotNull())
        .filter(col("city_name").isNotNull())
        .filter(col("station_id").isNotNull())
        # Tarih bileşenleri
        .withColumn("year",  year(col("measurement_date")))
        .withColumn("month", month(col("measurement_date")))
        .withColumn("day",   dayofmonth(col("measurement_date")))
        .withColumn("processed_at", current_timestamp())
        .drop("raw_json", "topic", "partition", "offset", "timestamp")
    )
    return silver


def build_gold_features_stream(spark: SparkSession):
    """Silver'dan oku, ML için feature'lar üret."""
    src = spark.readStream.format("delta").load(SILVER_PATH)

    gold = (
        src
        # Zaman feature'ları
        .withColumn("day_of_week",  dayofweek(col("measurement_date")))
        .withColumn("week_of_year", weekofyear(col("measurement_date")))
        .withColumn("quarter",      quarter(col("measurement_date")))
        .withColumn(
            "is_weekend",
            when(col("day_of_week").isin([1, 7]), 1).otherwise(0)
        )
        # Sıcaklık türevleri
        .withColumn("temp_range_c", col("max_temp_c") - col("min_temp_c"))
        .withColumn(
            "temp_avg_filled",
            when(col("avg_temp_c").isNotNull(), col("avg_temp_c"))
            .otherwise((col("max_temp_c") + col("min_temp_c")) / 2)
        )
        .withColumn(
            "is_freezing",
            when(col("temp_avg_filled") <= 0, 1).otherwise(0)
        )
        .withColumn(
            "is_hot_day",
            when(col("max_temp_c") >= 30, 1).otherwise(0)
        )
        # Yağış kategorisi
        .withColumn(
            "precipitation_category",
            when(col("precipitation_mm").isNull() | (col("precipitation_mm") == 0), "none")
            .when(col("precipitation_mm") < 2.5, "light")
            .when(col("precipitation_mm") < 10,  "moderate")
            .when(col("precipitation_mm") < 50,  "heavy")
            .otherwise("extreme")
        )
        .withColumn(
            "is_rainy",
            when(col("precipitation_mm") > 0.1, 1).otherwise(0)
        )
        # Rüzgar kategorisi
        .withColumn(
            "wind_category",
            when(col("avg_wind_speed_kmh").isNull(), "unknown")
            .when(col("avg_wind_speed_kmh") < 10, "calm")
            .when(col("avg_wind_speed_kmh") < 30, "moderate")
            .when(col("avg_wind_speed_kmh") < 60, "strong")
            .otherwise("storm")
        )
        # Veri tamlığı skoru
        .withColumn(
            "feature_completeness",
            (when(col("avg_temp_c").isNotNull(), 1).otherwise(0) +
             when(col("min_temp_c").isNotNull(), 1).otherwise(0) +
             when(col("max_temp_c").isNotNull(), 1).otherwise(0) +
             when(col("precipitation_mm").isNotNull(), 1).otherwise(0) +
             when(col("avg_wind_speed_kmh").isNotNull(), 1).otherwise(0) +
             when(col("avg_sea_level_pres_hpa").isNotNull(), 1).otherwise(0))
        )
    )
    return gold


def write_stream(df, path: str, checkpoint: str, partition_cols=None):
    """Bir streaming DataFrame'i Delta'ya yazar."""
    writer = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(processingTime=TRIGGER_INTERVAL)
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    return writer.start(path)


def write_gold_summary_batch(spark: SparkSession):
    """Gold #2: Şehir × ay özet tablosunu batch olarak yaz."""
    silver = spark.read.format("delta").load(SILVER_PATH)

    summary = (
        silver
        .groupBy("city_name", "year", "month")
        .agg(
            _round(avg("avg_temp_c"), 2).alias("avg_temp_c"),
            _round(_min("min_temp_c"), 2).alias("min_temp_c"),
            _round(_max("max_temp_c"), 2).alias("max_temp_c"),
            _round(stddev("avg_temp_c"), 2).alias("temp_stddev"),
            _round(_sum("precipitation_mm"), 2).alias("total_precipitation_mm"),
            _round(avg("precipitation_mm"), 2).alias("avg_precipitation_mm"),
            count(when(col("precipitation_mm") > 0.1, 1)).alias("rainy_days"),
            _round(avg("avg_wind_speed_kmh"), 2).alias("avg_wind_speed"),
            _round(_max("peak_wind_gust_kmh"), 2).alias("peak_gust_kmh"),
            count("*").alias("days_recorded"),
            count("avg_temp_c").alias("days_with_temp"),
        )
        .withColumn(
            "data_completeness_pct",
            _round((col("days_with_temp") / col("days_recorded")) * 100, 1)
        )
        .orderBy("city_name", "year", "month")
    )

    (
        summary.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_SUMMARY_PATH)
    )
    logger.info(f"Gold summary yazıldı: {summary.count()} satır")


def main():
    logger.info("=" * 60)
    logger.info("Climate Streaming Job başlıyor")
    logger.info(f"  Kafka  : {KAFKA_BOOTSTRAP} / topic={KAFKA_TOPIC}")
    logger.info(f"  Delta  : {DELTA_BASE}")
    logger.info("=" * 60)

    spark = create_spark_session()

    # 1) Bronze
    bronze_query = write_stream(
        build_bronze_stream(spark),
        BRONZE_PATH,
        f"{CHECKPOINT_BASE}/bronze",
    )
    logger.info(f"Bronze stream başladı: {bronze_query.id}")

    # Bronze'un ilk batch'ini bekle (Silver Bronze'dan okuyacak)
    time.sleep(15)

    # 2) Silver
    silver_query = write_stream(
        build_silver_stream(spark),
        SILVER_PATH,
        f"{CHECKPOINT_BASE}/silver",
        partition_cols=["year", "month"],
    )
    logger.info(f"Silver stream başladı: {silver_query.id}")

    time.sleep(15)

    # 3) Gold features
    gold_query = write_stream(
        build_gold_features_stream(spark),
        GOLD_FEATURES_PATH,
        f"{CHECKPOINT_BASE}/gold_features",
        partition_cols=["year", "month"],
    )
    logger.info(f"Gold features stream başladı: {gold_query.id}")

    # 4) Gold summary (batch)
    time.sleep(20)
    write_gold_summary_batch(spark)

    # Stream'leri canlı tut — Ctrl+C ile durdurulana kadar çalışsın
    logger.info("Tüm stream'ler aktif. Ctrl+C ile durdurabilirsin.")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Durdurma sinyali alındı, kapanıyor...")
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        logger.info("Temiz kapanış tamamlandı.")


if __name__ == "__main__":
    main()