from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when, window
from pyspark.sql.types import StructType, StringType, IntegerType

# 1. Buat SparkSession
spark = SparkSession.builder \
    .appName("MonitoringSensorGudang") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Skema data Kafka
schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembaban = StructType().add("gudang_id", StringType()).add("kelembaban", IntegerType())

# 3. Streaming dari Kafka
df_suhu_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

df_kelembaban_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

# 4. Parse JSON dan tambahkan timestamp
df_suhu = df_suhu_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_suhu).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

df_kelembaban = df_kelembaban_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_kelembaban).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

# 5. Tambahkan watermark dan window
df_suhu_windowed = df_suhu.withWatermark("timestamp", "10 seconds") \
    .select("gudang_id", "suhu", window("timestamp", "5 seconds").alias("win"))

df_kelembaban_windowed = df_kelembaban.withWatermark("timestamp", "10 seconds") \
    .select("gudang_id", "kelembaban", window("timestamp", "5 seconds").alias("win"))

# 6. Join berdasarkan gudang_id dan window
gabungan = df_suhu_windowed.join(df_kelembaban_windowed, on=["gudang_id", "win"])

# 7. Tambahkan kolom status
status_df = gabungan.select(
    "gudang_id", "suhu", "kelembaban",
    when((col("suhu") > 80) & (col("kelembaban") > 70),
         "Bahaya tinggi! Barang berisiko rusak").when(
         (col("suhu") > 80) & (col("kelembaban") <= 70),
         "Suhu tinggi, kelembaban normal").when(
         (col("suhu") <= 80) & (col("kelembaban") > 70),
         "Kelembaban tinggi, suhu aman").otherwise("Aman").alias("status")
)

# 8. Fungsi custom untuk mencetak output
def format_output(batch_df, epoch_id):
    rows = batch_df.collect()
    for row in rows:
        gudang = row["gudang_id"]
        suhu = row["suhu"]
        kelembaban = row["kelembaban"]
        status = row["status"]

        if "Bahaya tinggi" in status:
            print(f"\n[PERINGATAN KRITIS] Gudang {gudang}:\n- Suhu: {suhu}°C\n- Kelembaban: {kelembaban}%\n- Status: {status}")
        elif "Suhu tinggi" in status:
            print(f"\n[Peringatan Suhu Tinggi] \nGudang {gudang}:\n- Suhu: {suhu}°C\n- Kelembaban: {kelembaban}%\n- Status: {status}")
        elif "Kelembaban tinggi" in status:
            print(f"\n[Peringatan Kelembaban Tinggi] \nGudang {gudang}:\n- Suhu: {suhu}°C\n- Kelembaban: {kelembaban}%\n- Status: {status}")
        else:
            print(f"\nGudang {gudang}:\n- Suhu: {suhu}°C\n- Kelembaban: {kelembaban}%\n- Status: {status}")

# 9. Jalankan streaming
status_df.writeStream.outputMode("append") \
    .foreachBatch(format_output) \
    .start() \
    .awaitTermination()
