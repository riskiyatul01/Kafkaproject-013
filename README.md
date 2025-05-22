Nama : Riskiyatul Nur Oktarani

NRP : 5027231013

# Analisis Real-time Data Sensor Gudang dengan Kafka dan PySpark (Full Docker Setup)

Proyek ini merupakan implementasi sistem pemantauan kondisi gudang secara real-time menggunakan Apache Kafka untuk streaming data sensor dan Apache Spark (PySpark) untuk pemrosesan dan analisis data. Seluruh infrastruktur (Zookeeper, Kafka, Spark) diorkestrasi menggunakan Docker dan Docker Compose. Sistem ini akan mendeteksi suhu dan kelembaban tinggi, serta kondisi kritis gabungan dari kedua sensor.

## Latar Belakang Masalah
Sebuah perusahaan logistik mengelola beberapa gudang penyimpanan yang menyimpan barang sensitif seperti makanan, obat-obatan, dan elektronik. Untuk menjaga kualitas penyimpanan, gudang-gudang tersebut dilengkapi dengan dua jenis sensor:
*   Sensor Suhu
*   Sensor Kelembaban

Sensor akan mengirimkan data setiap detik. Perusahaan ingin memantau kondisi gudang secara real-time untuk mencegah kerusakan barang akibat suhu terlalu tinggi atau kelembaban berlebih.

## Tujuan Pembelajaran
Mahasiswa diharapkan dapat:
*   Memahami cara kerja Apache Kafka dalam pengolahan data real-time.
*   Membuat Kafka Producer dan Consumer untuk simulasi data sensor.
*   Mengimplementasikan stream filtering dengan PySpark.
*   Melakukan join multi-stream dan analisis gabungan dari berbagai sensor.
*   Mencetak hasil analitik berbasis kondisi kritis gudang ke dalam output console.

## Teknologi yang Digunakan
*   **Apache Kafka**: Platform streaming data terdistribusi.
*   **Apache Spark (PySpark)**: Mesin analitik terpadu untuk pemrosesan data skala besar, dengan dukungan streaming.
*   **Python**: Bahasa pemrograman untuk Kafka producer dan skrip PySpark.
*   **Docker & Docker Compose**: Untuk membuat, menyebarkan, dan menjalankan aplikasi dalam kontainer.


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
## Prasyarat
*   Docker Engine
*   Docker Compose

## Langkah-langkah Instalasi dan Menjalankan

### 1. Persiapan Direktori dan File
Pastikan Anda memiliki struktur direktori dan file seperti yang dijelaskan di bagian [Struktur Proyek](#struktur-proyek).
*   Buat direktori `producer` dan letakkan file `suhu.py` dan `kelembaban.py` di dalamnya.
*   Buat direktori `consumer` dan letakkan file `pyspark_consumer.py` di dalamnya.
*   Letakkan file `docker-compose.yml` di direktori root proyek.

### 2. Jalankan Layanan dengan Docker Compose
Buka terminal di direktori root proyek Anda dan jalankan perintah berikut:

```
docker-compose up -d
```

Perintah ini akan membangun (jika belum ada) dan menjalankan container Zookeeper, Kafka, Producer (Python), dan Spark.

![WhatsApp Image 2025-05-22 at 08 51 23_36a21366](https://github.com/user-attachments/assets/4c2f4e82-2226-423e-8434-211e8e5e234b)

### 3. Buat Topik Kafka
Setelah container Kafka berjalan, Anda perlu membuat dua topik Kafka. Buka terminal baru dan jalankan perintah berikut:

* Masuk ke dalam container Kafka
```
docker exec -it kafka-container bash
```

* Buat topik sensor-suhu-gudang
```
kafka-topics --create --topic sensor-suhu-gudang --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
```

* Buat topik sensor-kelembaban-gudang
```
kafka-topics --create --topic sensor-kelembaban-gudang --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
```

![WhatsApp Image 2025-05-22 at 08 54 16_b35e2cc6](https://github.com/user-attachments/assets/3a5cc376-a5b7-4443-873b-800b5252cb96)

### 4. Jalankan Producer Kafka

Producer akan mengirimkan data sensor simulasi ke topik Kafka.

a. Jalankan Producer Suhu:

Buka terminal baru dan jalankan:
```
docker exec -it producer-container bash
python suhu.py
```
![image](https://github.com/user-attachments/assets/6b2adb4c-e03b-41e5-b690-809fc647b20d)

b. Jalankan Producer Kelembaban:

Buka terminal baru dan jalankan:
```
docker exec -it producer-container bash
python kelembaban.py
```
![image](https://github.com/user-attachments/assets/c2a31046-5266-4d23-9eff-8df22c23a421)

### 5. Jalankan Consumer PySpark

Consumer PySpark akan membaca data dari kedua topik, memprosesnya, dan menampilkan peringatan.

Buka terminal baru dan jalankan:
```
docker exec -it spark-container bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark_consumer.py

```

![image](https://github.com/user-attachments/assets/ac477a92-6f34-4677-a486-0a96e99d0801)

![image](https://github.com/user-attachments/assets/27585984-486d-403c-9888-1023651eb336)

![image](https://github.com/user-attachments/assets/04b1ae67-b374-4bb6-97bb-7b546c2a3fea)

### 6. Hentikan Layanan

Setelah selesai, Anda dapat menghentikan semua container dengan perintah:
```
docker-compose down
```

![image](https://github.com/user-attachments/assets/9dcf37a1-f4c6-4fab-bf4e-7a034d2b2927)

### Cara Kerja

* Producer Data:
    * suhu.py: Mengirimkan data suhu acak (antara 75-90°C) untuk gudang G1, G2, dan G3 ke topik sensor-suhu-gudang setiap detik.
    * kelembaban.py: Mengirimkan data kelembaban acak (antara 65-80%) untuk gudang G1, G2, dan G3 ke topik sensor-kelembaban-gudang setiap detik.
* Apache Kafka:
    * Bertindak sebagai message broker, menerima data dari producer dan menyediakannya untuk consumer.
    * Dua topik (sensor-suhu-gudang dan sensor-kelembaban-gudang) digunakan untuk memisahkan aliran data.
* PySpark Consumer (pyspark_consumer.py):
    * Membaca data stream dari kedua topik Kafka.
    * Melakukan parsing data JSON.
    * Menambahkan kolom timestamp pada setiap data yang masuk.
    * Menerapkan watermarking untuk menangani data yang datang terlambat dan windowing (5 detik) untuk mengelompokkan data berdasarkan waktu.
    * Melakukan join antara stream suhu dan stream kelembaban berdasarkan gudang_id dan window waktu yang sama.
* Menerapkan filtering dan logika kondisional:
    * Jika suhu > 80°C DAN kelembaban > 70% pada gudang yang sama dalam window waktu yang sama → PERINGATAN KRITIS: Bahaya tinggi! Barang berisiko rusak.
    * Jika hanya suhu > 80°C → Peringatan Suhu Tinggi.
    * Jika hanya kelembaban > 70% → Peringatan Kelembaban Tinggi.
    * Jika kedua kondisi di bawah ambang batas → Aman.
* Mencetak hasil analisis ke konsol dengan format yang sesuai.

### Kesimpulan

Proyek ini berhasil mendemonstrasikan implementasi sistem pemantauan kondisi gudang secara real-time dengan memanfaatkan Apache Kafka dan PySpark. Melalui simulasi pengiriman data sensor suhu dan kelembaban, sistem ini mampu:
* Menerima aliran data secara berkelanjutan dari berbagai sensor (suhu dan kelembaban) melalui topik Kafka yang terpisah.
* Memproses dan menganalisis data stream menggunakan PySpark, termasuk parsing data, penambahan timestamp, dan penerapan watermarking serta windowing.
* Melakukan filtering untuk mendeteksi kondisi abnormal pada masing-masing sensor (suhu tinggi atau kelembaban tinggi) secara individual.
* Menggabungkan (join) data dari dua sensor berbeda berdasarkan gudang_id dan jendela waktu (window) untuk analisis komprehensif.
* Menghasilkan peringatan gabungan dan kritis secara dinamis ketika suhu dan kelembaban secara bersamaan melebihi ambang batas pada gudang yang sama, serta memberikan status kondisi gudang yang lebih informatif.
