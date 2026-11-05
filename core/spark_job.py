"""Pemrosesan big data dengan Spark: membaca CSV bersih (HDFS atau folder lokal), menghitung SEMUA agregasi berat
secara terdistribusi, lalu menyimpan hasil kecilnya ke data/results/spark/.
Hasil itu dibaca core/ml_analysis.py untuk tahap ML (scikit-learn) dan dashboard.

Biasanya dipanggil otomatis oleh app.py. Manual (dari folder proyek yang berisi app.py):

    spark-submit core/spark_job.py
    spark-submit core/spark_job.py --input hdfs://localhost:9000/citibike/clean/*.csv
    spark-submit core/spark_job.py --input file:///D:/proyek/data/clean/*.csv

Pastikan alamat HDFS di --input sama dengan hasil:  hdfs getconf -confKey fs.defaultFS
"""
import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NEED = ["ride_id", "rideable_type", "ended_at", "start_station_name", "end_station_name",
        "start_lat", "start_lng", "member_casual", "date", "hour", "dow", "is_weekend",
        "duration_min", "distance_km"]
CLS_FEATURES = ["hour", "dow", "is_weekend", "duration_min", "distance_km", "is_electric"]
SAMPLE_ROWS = 250_000


def step(pct, msg):
    print(f"@@{pct}|{msg}", flush=True)       # dibaca app.py untuk progress bar


def save(frame, out: Path, name: str):
    # hasil agregasi kecil, aman diubah ke pandas lalu disimpan sebagai satu file CSV
    frame.toPandas().to_csv(out / f"{name}.csv", index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="hdfs://localhost:9000/citibike/clean/*.csv")
    ap.add_argument("--out", default="data/results/spark")
    a = ap.parse_args()

    spark = SparkSession.builder.appName("CitiBike-Spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    step(8, "Spark membaca CSV")
    raw = spark.read.csv(a.input, header=True, inferSchema=False)      # semua string dulu, tipe di-cast manual (lebih cepat)
    raw = raw.toDF(*[c.strip().lower() for c in raw.columns])
    miss = set(NEED) - set(raw.columns)
    if miss:
        raise SystemExit("Kolom tidak ada di data: " + ", ".join(sorted(miss)))

    df = (raw.select(
            "ride_id", "rideable_type", "start_station_name", "end_station_name", "member_casual", "date",
            F.col("start_lat").cast("double"), F.col("start_lng").cast("double"),
            F.col("hour").cast("int"), F.col("dow").cast("int"), F.col("is_weekend").cast("int"),
            F.col("duration_min").cast("double"), F.col("distance_km").cast("double"),
            # ended_at punya pecahan nanodetik; ambil 19 karakter pertama (yyyy-MM-dd HH:mm:ss)
            F.hour(F.to_timestamp(F.substring("ended_at", 1, 19))).alias("end_hour"))
          .dropDuplicates(["ride_id"])
          .withColumn("is_member", (F.col("member_casual") == "member").cast("int"))
          .withColumn("is_electric", F.coalesce(F.col("rideable_type").contains("electric"), F.lit(False)).cast("int"))
          .withColumn("day_type", F.when(F.col("dow") >= 5, "weekend").otherwise("weekday"))
          .cache())

    # ---- meta (ringkasan)
    step(15, "Spark menghitung ringkasan data")
    m = df.agg(F.count("*").alias("rows"),
               F.countDistinct("start_station_name").alias("stations"),
               F.min("date").alias("date_min"), F.max("date").alias("date_max"),
               F.countDistinct("date").alias("days"),
               F.avg("is_member").alias("member_share"), F.avg("duration_min").alias("avg_duration"),
               F.avg("is_electric").alias("electric_share"), F.avg("distance_km").alias("avg_distance")).first().asDict()
    print(f"Baris dibaca: {m['rows']:,}", flush=True)
    (out / "meta.json").write_text(json.dumps(m, default=str), encoding="utf-8")

    # ---- trip per hari (+ porsi member, durasi rata-rata)
    step(25, "Spark menghitung trip per hari")
    daily = (df.groupBy("date").agg(F.count("*").alias("trips"), F.avg("is_member").alias("member_share"),
                                    F.avg("duration_min").alias("avg_duration")).orderBy("date"))
    save(daily, out, "daily")

    # ---- pola jam x tipe pengguna x jenis hari
    step(35, "Spark menghitung pola jam dan hari")
    ndays = df.groupBy("day_type").agg(F.countDistinct("date").alias("ndays"))
    hourly = (df.groupBy("day_type", "hour", "member_casual").agg(F.count("*").alias("trips"))
                .join(ndays, "day_type").withColumn("avg_per_day", F.col("trips") / F.col("ndays"))
                .drop("ndays").orderBy("day_type", "hour", "member_casual"))
    save(hourly, out, "hourly")
    save(df.groupBy("date", "dow", "member_casual").agg(F.count("*").alias("n")), out, "per_date")

    # ---- stasiun tersibuk
    step(50, "Spark menghitung stasiun tersibuk")
    top = (df.groupBy("start_station_name")
             .agg(F.count("*").alias("trips"), F.avg("start_lat").alias("lat"), F.avg("start_lng").alias("lng"),
                  F.avg("is_member").alias("member_share"))
             .orderBy(F.desc("trips")).limit(15))
    save(top, out, "top_stations")

    # ---- arus sepeda per stasiun (hari kerja, blok pagi 06-10 dan sore 16-20)
    step(60, "Spark menghitung arus sepeda per stasiun")
    wk = df.filter(F.col("dow") < 5)
    wk_days = wk.select(F.countDistinct("date")).first()[0] or 1

    def block(col):
        return (F.when(F.col(col).between(6, 9), "pagi").when(F.col(col).between(16, 19), "sore"))

    o = (wk.withColumn("block", block("hour")).filter(F.col("block").isNotNull())
           .groupBy("block", F.col("start_station_name").alias("station")).agg(F.count("*").alias("n_out")))
    i = (wk.withColumn("block", block("end_hour")).filter(F.col("block").isNotNull())
           .groupBy("block", F.col("end_station_name").alias("station")).agg(F.count("*").alias("n_in")))
    coords = df.groupBy(F.col("start_station_name").alias("station")).agg(
        F.avg("start_lat").alias("start_lat"), F.avg("start_lng").alias("start_lng"))
    flow = (o.join(i, ["block", "station"], "outer").na.fill(0, ["n_out", "n_in"])
              .filter(F.col("n_out") + F.col("n_in") >= max(20, wk_days))
              .withColumn("net", F.col("n_out") - F.col("n_in"))
              .withColumn("net_per_day", F.col("net") / wk_days)
              .withColumn("out_per_day", F.col("n_out") / wk_days)
              .withColumn("in_per_day", F.col("n_in") / wk_days)
              .join(coords, "station", "left"))
    save(flow, out, "flow")

    # ---- sampel untuk klasifikasi member vs casual (dilatih dengan scikit-learn di tahap ML)
    step(75, "Spark mengambil sampel untuk klasifikasi")
    frac = min(1.0, (SAMPLE_ROWS * 1.1) / max(1, m["rows"]))
    smp = (df.select(*CLS_FEATURES, "is_member").sample(False, frac, seed=42).limit(SAMPLE_ROWS))
    save(smp, out, "sample")

    step(85, "Spark selesai")
    spark.stop()


if __name__ == "__main__":
    main()
