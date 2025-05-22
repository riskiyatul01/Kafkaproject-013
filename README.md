Nama : Riskiyatul Nur Oktarani

NRP : 5027231013

# Analisis Real-time Data Sensor Gudang dengan Kafka dan PySpark (Full Docker Setup)

Proyek ini merupakan implementasi sistem pemantauan kondisi gudang secara real-time menggunakan Apache Kafka untuk streaming data sensor dan Apache Spark (PySpark) untuk pemrosesan dan analisis data. Seluruh infrastruktur (Zookeeper, Kafka, Spark) diorkestrasi menggunakan Docker dan Docker Compose. Sistem ini akan mendeteksi suhu dan kelembaban tinggi, serta kondisi kritis gabungan dari kedua sensor.

## Struktur Proyek

```
.
├── consumer/
│ └── pyspark_consumer.py # Script Spark untuk memproses data
├── producer/
│ ├── kelembaban.py # Script Python untuk mengirim data kelembaban
│ └── suhu.py # Script Python untuk mengirim data suhu
├── docker-compose.yml # File konfigurasi untuk Docker Compose
└── README.md # File ini
```

## Komponen Utama Infrastruktur Docker
Semua layanan backend (Zookeeper, Kafka, Spark Master, Spark Worker) dijalankan sebagai container Docker yang dikelola oleh docker-compose.yml.
- Zookeeper: Diperlukan oleh Kafka untuk manajemen cluster.
- Kafka: Message broker yang menerima data dari producer dan menyediakannya untuk consumer. Dua topik akan dibuat:
    - sensor-suhu-gudang
    - sensor-kelembaban-gudang
- Spark Master & Worker(s): Cluster Spark untuk memproses data stream dari Kafka menggunakan PySpark.
