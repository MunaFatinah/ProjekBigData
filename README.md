# Citi Bike Insight

Aplikasi Big Data + Machine Learning untuk menganalisis data perjalanan sepeda berbagi **Citi Bike (New York)**.
Data masuk lewat web, dikirim NiFi ke Hadoop HDFS, lalu dianalisis (Business Intelligence + ML) dan ditampilkan
sebagai dashboard berisi insight dan rekomendasi.

```
 [1] Web INPUT            [2] NiFi + Hadoop             [3] Web OUTPUT
 upload CSV bersih     →   GetFile → PutHDFS         →   BI + ML + rekomendasi
 (cleaning manual)         folder data/outbox            (dan CSV untuk Power BI)
                          → HDFS /citibike/clean
```

## Pertanyaan yang dijawab
1. **BI**: jumlah trip hari ini dipengaruhi faktor apa dari hari-hari lampau? (autokorelasi, cuaca, pola mingguan)
2. **Prediksi**: berapa trip per hari selama 7 hari ke depan?
3. **Klasifikasi**: apakah sebuah perjalanan dilakukan member atau casual?
4. **Operasional**: stasiun mana yang cepat kosong atau menumpuk di jam sibuk, dan apa yang harus dilakukan?

## Sumber data
- **Perjalanan**: data resmi Citi Bike, https://citibikenyc.com/system-data (CSV bulanan). Ambil bulan-bulan dalam 1 tahun terakhir.
- **Cuaca**: Open-Meteo Historical Weather API (suhu rata-rata, curah hujan, angin maksimum harian New York). Diunduh otomatis saat analisis, butuh internet; kalau gagal, analisis tetap jalan tanpa faktor cuaca.

## Cara menjalankan
```bash
pip install -r requirements.txt
python app.py            # buka http://localhost:5000
```
Urutan pemakaian: **Input** (upload CSV) → **NiFi dan Hadoop** (lihat antrean, jalankan analisis) → **Dashboard**.

Tanpa Hadoop pun aplikasi berjalan: analisis otomatis memakai folder lokal `data/clean`. Dengan Hadoop dan NiFi aktif,
analisis membaca dari HDFS (variabel `HDFS_DIR`, default `/citibike/clean`). Flow NiFi (GetFile → PutHDFS) dibuat dan dijalankan **otomatis oleh aplikasi** lewat REST API NiFi (`core/nifi_client.py`): isi kredensial NiFi sekali di halaman NiFi dan Hadoop, lalu tiap upload memastikan flow berjalan. Panduan manual (cadangan): [nifi/NIFI_SETUP.md](nifi/NIFI_SETUP.md).

### Mencoba tanpa data asli
```bash
python scripts/make_sample_data.py --days 100 --out sample_data --per-month
```
Data ini **sintetis** (dibuat dengan pola yang ditanam), hanya untuk menguji pipeline. Jangan dipakai untuk laporan.

## Manajemen data
| Tahap | Lokasi | Isi |
|---|---|---|
| Sementara | `data/incoming` | file upload sebelum dicek (dihapus otomatis) |
| Bersih | `data/clean` | CSV bersih hasil upload, arsip permanen |
| Antrean | `data/outbox` | salinan untuk NiFi (dipindahkan NiFi setelah diambil) |
| Data lake | HDFS `/citibike/clean` | data bersih yang dibaca analisis |
| Hasil | `data/results` | `results.json`, CSV untuk Power BI, model terlatih |

Cleaning dilakukan manual di luar aplikasi. Aplikasi hanya memeriksa kolom wajib (`core/intake.py`) dan melengkapi kolom turunan
(date, hour, dow, month, is_weekend, duration_min, distance_km) kalau belum ada. Tidak ada baris yang dibuang.

## Metode
- **Lag dan cuaca**: autokorelasi trip harian (tren dibuang), korelasi dengan cuaca setelah efek hari dalam minggu dibuang, dan random forest dengan permutation importance pada hari uji (pemisahan berdasarkan waktu).
- **Prediksi 7 hari**: random forest dengan fitur lag 1, 2, 3, 7 hari, rata-rata 7 hari, dan kalender. Dievaluasi pada 20% hari terakhir dan dibandingkan dengan baseline "sama seperti minggu lalu". Cuaca tidak dipakai karena cuaca masa depan tidak diketahui.
- **Klasifikasi**: gradient boosting (HistGradientBoosting) pada jam, hari, durasi, jarak, dan jenis sepeda; metrik akurasi, F1, AUC, dibandingkan dengan tebakan kelas mayoritas.
- **Rekomendasi**: net flow stasiun (keluar dikurangi masuk) pada blok pagi 06.00–10.00 dan sore 16.00–20.00 hari kerja, efek hujan, porsi casual akhir pekan, dan hasil prediksi.

## Power BI
Dashboard web sudah memuat visual utama, namun file hasil bisa dibuka juga di Power BI Desktop:
**Get Data → Text/CSV**, lalu muat CSV dari tombol unduh di dashboard (`daily_trips`, `hourly_profile`, `station_flow`,
`top_stations`, `forecast`, `lag_correlation`, `feature_importance`, `recommendations`). File yang sama ada di `data/results/`.
Hubungkan tabel lewat kolom `date` atau nama stasiun.

## Batasan
- File CSV sangat besar (jutaan baris) dibaca sekaligus oleh pandas; siapkan RAM yang cukup atau upload per bulan.
- Prediksi 7 hari bergantung pada pola historis, jadi kualitasnya naik dengan data yang lebih panjang (idealnya 12 bulan). Dengan data beberapa bulan saja, model bisa kalah dari baseline; dashboard melaporkan perbandingan ini apa adanya.
- Korelasi tidak membuktikan sebab-akibat: "hujan berasosiasi dengan trip lebih sedikit", bukan bukti mutlak.

## Struktur
```
app.py                 web (Flask) dan API
core/intake.py         cek kolom CSV bersih
core/hdfs_utils.py     pembungkus `hdfs dfs` (put diverifikasi ke isi HDFS)
core/nifi_client.py    membuat dan menjalankan flow NiFi otomatis (REST API)
core/weather.py        cuaca Open-Meteo
core/ml_analysis.py    BI, prediksi, klasifikasi, rekomendasi
templates/, static/    tampilan (grafik SVG, tanpa library eksternal)
scripts/               generator data sintetis
nifi/NIFI_SETUP.md     panduan flow NiFi
```
