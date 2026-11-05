"""Penerimaan CSV yang SUDAH dibersihkan secara manual.

Tidak ada cleaning di sini: tidak ada baris yang dibuang. Fungsi ini hanya
(1) memastikan kolom yang dibutuhkan analisis ada, dan
(2) melengkapi kolom turunan (date, hour, dow, dst.) kalau belum ada.
Dibaca per potongan supaya file besar tidak memenuhi RAM.
"""
import time
from pathlib import Path

import numpy as np
import pandas as pd

REQUIRED = ["ride_id", "started_at", "ended_at", "start_station_name", "end_station_name",
            "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"]
DERIVED = ["date", "hour", "dow", "month", "is_weekend", "duration_min", "distance_km"]


def _haversine_km(lat1, lng1, lat2, lng2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lng2 - lng1) / 2) ** 2
    return 2 * 6371.0088 * np.arcsin(np.sqrt(a))


def _fill(df: pd.DataFrame) -> pd.DataFrame:
    s = pd.to_datetime(df["started_at"])
    e = pd.to_datetime(df["ended_at"])
    if "rideable_type" not in df:
        df["rideable_type"] = "classic_bike"
    if "date" not in df:
        df["date"] = s.dt.strftime("%Y-%m-%d")
    if "hour" not in df:
        df["hour"] = s.dt.hour
    if "dow" not in df:
        df["dow"] = s.dt.dayofweek
    if "month" not in df:
        df["month"] = s.dt.month
    if "is_weekend" not in df:
        df["is_weekend"] = (s.dt.dayofweek >= 5).astype(int)
    if "duration_min" not in df:
        df["duration_min"] = ((e - s).dt.total_seconds() / 60).round(2)
    if "distance_km" not in df:
        df["distance_km"] = _haversine_km(df["start_lat"], df["start_lng"], df["end_lat"], df["end_lng"]).round(3)
    return df


def accept_file(src: Path, dst: Path):
    """Validasi CSV bersih di `src` lalu tulis ke `dst` (tanpa membuang baris). Return laporan."""
    t0 = time.time()
    cols = [str(c).strip().lower() for c in pd.read_csv(src, nrows=0).columns]
    missing = [c for c in REQUIRED if c not in cols]
    if missing:
        raise ValueError("Kolom wajib tidak ada: " + ", ".join(missing))
    added = [c for c in DERIVED + ["rideable_type"] if c not in cols]

    rows, dmin, dmax, first = 0, None, None, True
    for chunk in pd.read_csv(src, chunksize=500_000, low_memory=False):
        chunk.columns = [str(c).strip().lower() for c in chunk.columns]
        chunk = _fill(chunk)
        rows += len(chunk)
        lo, hi = chunk["date"].astype(str).min(), chunk["date"].astype(str).max()
        dmin = lo if dmin is None else min(dmin, lo)
        dmax = hi if dmax is None else max(dmax, hi)
        chunk.to_csv(dst, mode="w" if first else "a", header=first, index=False)
        first = False
    if rows == 0:
        raise ValueError("File kosong.")
    return {"file": Path(src).name, "stored_as": Path(dst).name, "rows": rows,
            "date_min": dmin, "date_max": dmax, "kolom_dilengkapi": added,
            "seconds": round(time.time() - t0, 1)}
