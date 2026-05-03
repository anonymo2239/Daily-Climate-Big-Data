# Adım 1: Docker Ortamının Kurulumu

## Amaç
Kafka, Zookeeper, Spark ve Producer servislerinin izole, taşınabilir ve
versiyonlanmış bir ortamda çalışmasını sağlamak.

## Mimari

┌─────────────┐     ┌──────────┐     ┌───────────────┐     ┌──────────┐
│  Producer   │ →   │  Kafka   │ →   │ Spark + Delta │ →   │  Delta   │
│  (Python)   │     │ (broker) │     │   Streaming   │     │   Lake   │
└─────────────┘     └──────────┘     └───────────────┘     └──────────┘
↑                                        │
┌──────────┐                             │
│Zookeeper │                             │
└──────────┘                             │
┌──────────┐                             │
│ Kafka UI │←────────────────────────────┘
└──────────┘

## Servisler

| Servis | İmaj | Port | Rol |
|---|---|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka koordinasyonu |
| kafka | confluentinc/cp-kafka:7.5.0 | 9092, 29092 | Mesaj kuyruğu |
| kafka-ui | provectuslabs/kafka-ui:latest | 8080 | Kafka gözlem arayüzü |
| producer | (custom) ./producer | - | Parquet → Kafka |
| spark | quay.io/jupyter/pyspark-notebook:spark-3.5.0 | 8888, 4040 | Streaming + Delta Lake |

## Çalıştırma

```bash
docker-compose up -d --build
docker ps                       # tüm servisler "Up" olmalı
```

## Erişim Noktaları

- **Jupyter Lab**: http://localhost:8888 (token: `climate`)
- **Kafka UI**: http://localhost:8080
- **Spark UI**: http://localhost:4040 (job aktifken)

## Tasarım Kararları

### 1. Kafka çift listener (9092 + 29092)
- `kafka:9092` — container içi (Spark, Producer)
- `localhost:29092` — host makineden test için
- `KAFKA_ADVERTISED_LISTENERS` ile her iki tarafa farklı adres veriliyor.

### 2. Versiyonlar sabitlendi
- Spark 3.5.0 / Scala 2.12 / Delta 3.2.0 — birbiriyle uyumlu, denenmiş kombinasyon.
- "latest" tag'leri kullanılmadı, taşınabilirlik için.

### 3. Volume bağlantıları
- `./data:/data:ro` (read-only) — Producer parquet okuyor, yazma yetkisi yok.
- `./delta-lake:...` — Spark Delta tablolarını host'tan görebilelim.
- `./notebooks:...` — Notebook'lar kalıcı.

### 4. Healthcheck
- Kafka için `nc -z localhost 9092` ile sağlık kontrolü.
- Producer `condition: service_healthy` ile Kafka tam hazır olmadan başlamıyor.

## Doğrulama

`screenshots/` klasöründe:
- `01_docker_ps.png` — tüm container'lar "Up"
- `02_kafka_ui.png` — Kafka UI broker görünüyor
- `03_kafka_messages.png` — climate-events topic'inde mesajlar