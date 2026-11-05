# Panduan NiFi → HDFS

> **Jalur otomatis (disarankan):** di halaman *NiFi dan Hadoop* aplikasi, isi URL, username, password NiFi dan lokasi config Hadoop, lalu klik *Hubungkan dan buat flow otomatis*. Aplikasi membuat flow di bawah ini sendiri. Panduan manual di bawah hanya cadangan.

Alur: `data/outbox/*.csv` → **GetFile** → **PutHDFS** → `/citibike/clean` di HDFS.

## 1. Pastikan Hadoop menyala
```
start-dfs            (Windows: start-dfs.cmd)
jps                  (harus tampil NameNode dan DataNode)
hdfs dfs -mkdir -p /citibike/clean
hdfs dfs -ls /
```
Catat nilai `fs.defaultFS` di `core-site.xml` (mis. `hdfs://localhost:9000`). NiFi memakai file itu untuk tahu alamat HDFS.

## 2. Buka NiFi
Buka `https://localhost:8443/nifi`. Pada NiFi 2.x, username dan password dibuat otomatis saat pertama jalan; cari di
`<folder-nifi>/logs/nifi-app.log` (cari kata "Generated").

## 3. Buat flow (dua processor)
Seret processor ke kanvas, lalu klik dua kali untuk mengisi properti.

**GetFile**
| Properti | Nilai |
|---|---|
| Input Directory | path absolut folder `data/outbox` proyek ini (ditampilkan di halaman Pipeline) |
| File Filter | `.*\.csv` |
| Keep Source File | `false` |
| Polling Interval | `5 sec` |

**PutHDFS**
| Properti | Nilai |
|---|---|
| Hadoop Configuration Resources | `<hadoop>/etc/hadoop/core-site.xml,<hadoop>/etc/hadoop/hdfs-site.xml` |
| Directory | `/citibike/clean` |
| Conflict Resolution Strategy | `replace` |

Tab **Relationships** pada PutHDFS: centang *terminate* untuk `success` dan `failure` (atau sambungkan `failure` ke
processor LogAttribute kalau mau melihat error).

Sambungkan **GetFile → PutHDFS** dengan relasi `success`, lalu klik kanan kanvas → **Start**.

## 4. Uji
1. Upload satu CSV di halaman **Input**.
2. Di halaman **Pipeline**, file muncul di *Antrean NiFi* lalu hilang dalam beberapa detik.
3. Kartu *HDFS* menampilkan file yang sama. Cek juga: `hdfs dfs -ls /citibike/clean`.

## Kalau bermasalah
| Gejala | Cek |
|---|---|
| PutHDFS merah, "Connection refused" | NameNode belum jalan, atau `fs.defaultFS` di core-site.xml tidak cocok dengan port sebenarnya |
| Error terkait `winutils` / `HADOOP_HOME` (Windows) | Set `HADOOP_HOME` dan pastikan `winutils.exe` ada di `%HADOOP_HOME%\bin` |
| File tidak diambil GetFile | Path *Input Directory* salah, atau NiFi tidak punya izin baca folder itu |
| Dashboard bilang "Tidak ada file CSV di HDFS" | Flow belum di-Start, atau pakai tombol *Sinkronkan* sebagai cadangan |
