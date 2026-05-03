"""
Kafka Producer: Global Daily Climate Data
-----------------------------------------
Parquet dosyasından satırları okur, JSON formatında Kafka topic'ine gönderir.
Streaming simülasyonu için ayarlanabilir gönderim hızı destekler.
"""

import json
import time
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
from kafka import  KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "climate-events")
DATA_PATH = os.getenv("DATA_PATH", "/data/daily_weather.parquet")
MESSAGES_PER_SECOND = int(os.getenv("MESSAGES_PER_SECOND", "50"))
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0")) # 0 olunca hepsini gonder oluyor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

def create_producer():
    """producer nesnesi olsturur"""
    logger.info(f"Kafka'ya baglaniyor: {KAFKA_BOOTSTRAP_SERVERS}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        # Güvenilirlik ayarları
        acks="all",        # broker'ın yazdığını onaylamasını bekle
        retries=3,         # hata olursa 3 kez tekrar dene
        linger_ms=10,      # küçük batch'lerle gönder, throughput artar
    )
    logger.info("kafka producer olusuruldu.")
    return producer

def get_data_chunks(path: str, chunk_size=5000):
    """Parquet dosyasını RAM'i şişirmeden parça parça okur."""
    logger.info(f"Veri okunuyor: {path}")
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        yield batch.to_pandas()

def row_to_message(row: pd.Series) -> dict:
    """Bir DataFrame satırını Kafka mesajına çevirir."""

    msg = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "weather_observation",

        # ID ler
        "station_id": str(row["station_id"]),
        "city_name": str(row["city_name"]),

        # olcum tarihi
        "measurement_date": str(row["date"]),
        "season": str(row["season"]) if pd.notna(row["season"]) else None,

        "avg_temp_c": _safe_float(row["avg_temp_c"]),
        "min_temp_c": _safe_float(row["min_temp_c"]),
        "max_temp_c": _safe_float(row["max_temp_c"]),
        "precipitation_mm": _safe_float(row["precipitation_mm"]),
        "snow_depth_mm": _safe_float(row["snow_depth_mm"]),
        "avg_wind_dir_deg": _safe_float(row["avg_wind_dir_deg"]),
        "avg_wind_speed_kmh": _safe_float(row["avg_wind_speed_kmh"]),
        "peak_wind_gust_kmh": _safe_float(row["peak_wind_gust_kmh"]),
        "avg_sea_level_pres_hpa": _safe_float(row["avg_sea_level_pres_hpa"]),
        "sunshine_total_min": _safe_float(row["sunshine_total_min"]),
    }
    return msg

def _safe_float(value):
    """NaN değerleri None'a çevirir (JSON serileştirme için)."""
    if pd.isna(value):
        return None
    return float(value)

def main():
    logger.info("=" * 60)
    logger.info("Climate Data Kafka Producer başlıyor")
    logger.info(f"  Topic           : {KAFKA_TOPIC}")
    logger.info(f"  Hız             : {MESSAGES_PER_SECOND} msg/sn")
    logger.info(f"  Max mesaj       : {'sınırsız' if MAX_MESSAGES == 0 else MAX_MESSAGES}")
    logger.info("=" * 60)

    delay = 1.0 / MESSAGES_PER_SECOND
    producer = create_producer()

    sent = 0
    failed = 0
    start = time.time()

    try:
        limit_reached = False
        for df_chunk in get_data_chunks(DATA_PATH):
            if limit_reached:
                break
                
            for _, row in df_chunk.iterrows():
                if MAX_MESSAGES and sent >= MAX_MESSAGES:
                    logger.info(f"MAX_MESSAGES ({MAX_MESSAGES}) sınırına ulaşıldı.")
                    limit_reached = True
                    break

                msg = row_to_message(row)
                key = msg["station_id"]

                try:
                    producer.send(KAFKA_TOPIC, key=key, value=msg)
                    sent += 1
                except KafkaError as e:
                    logger.error(f"Gönderim Hatasi {e}")
                    failed += 1

                # her 1000 mesajda bir ozet log
                if sent % 1000 == 0 and sent > 0:
                    elapsed = time.time() - start
                    rate = sent / elapsed
                    logger.info(f"Gönderildi: {sent:,} | Hız: {rate:.1f} msg/sn | Hata: {failed}")

                time.sleep(delay)

    except KeyboardInterrupt:
        logger.info("Kullanıcı tarafından durduruldu.")

    finally:
        logger.info("Buffer'daki mesajlar gonderiliyor...")
        producer.flush()
        producer.close()

        elapsed = time.time() - start
        logger.info("=" * 60)
        logger.info(f"BİTTİ. Toplam: {sent:,} mesaj | Süre: {elapsed:.1f}sn | Hata: {failed}")
        logger.info("=" * 60)

if __name__ == "__main__":
    main()