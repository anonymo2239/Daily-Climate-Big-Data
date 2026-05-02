# Daily Climate Big Data Pipeline

Büyük Veri Analizine Giriş dersi dönem projesi.  
Uçtan uca büyük veri pipeline'ı: Docker + Kafka + Spark Structured Streaming + Delta Lake + MLflow.

## Veri Seti

[Global Daily Climate Data](https://www.kaggle.com/datasets/guillemservera/global-daily-climate-data)
- 27.6 milyon satır günlük hava ölçümü
- 14 sütun (sıcaklık, yağış, rüzgar, basınç vs.)
- 1957'den günümüze

## Mimari
[CSV/Parquet] → [Kafka Producer] → [Kafka Topic] → [Spark Streaming] → [Delta Lake (Bronze/Silver/Gold)] → [ML Models] → [Dashboard]

## Teknoloji Yığını

| Katman | Teknoloji |
|--------|-----------|
| Konteynerizasyon | Docker, Docker Compose |
| Streaming | Apache Kafka + Zookeeper |
| Veri İşleme | Apache Spark (PySpark) Structured Streaming |
| Depolama | Delta Lake |
| ML | Spark MLlib + MLflow |
| Görselleştirme | Matplotlib / Databricks Dashboard |

## Klasör Yapısı
.
├── docker-compose.yml      # Tüm servisleri ayağa kaldıran ana dosya
├── producer/
├── spark/
├── notebooks/
├── data/                   # Veri setleri (.gitignore'da, Kaggle'dan indirilmeli)
├── delta-lake/
└── screenshots/

## Kurulum

1. Bu repo'yu clone'la
2. [Kaggle'dan veri setini indir](https://www.kaggle.com/datasets/guillemservera/global-daily-climate-data) ve `data/` klasörüne koy
3. Docker Desktop'ı çalıştır
4. Ana klasörde:
```bash
   docker-compose up -d
```

## Takım

- Adem Alperen Arda (anonymo2239) — Adım 1, 2, 3 (Docker, Kafka Producer, Spark Streaming)
- Barış Cerit (ceritbariss) — Adım 4, 5 (EDA, Feature Engineering)
- Ubeydullah Gür (ubeydgur) — Adım 6, 7 (ML Modelleri, Dashboard)

## Ders

Büyük Veri Analizine Giriş — Dr. Ayşe Gül Eker — 2025-2026 Bahar