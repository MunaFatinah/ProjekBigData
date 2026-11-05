"""Buat data SINTETIS berformat Citi Bike untuk uji coba aplikasi.

PENTING: data ini hanya untuk tes pipeline. Untuk laporan, pakai data asli dari
https://citibikenyc.com/system-data (ambil bulan dalam 1 tahun terakhir).

Pemakaian:
    python scripts/make_sample_data.py --days 120 --out sample_data
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

rng = np.random.default_rng(7)

STATIONS = [  # (nama, lat, lng, tipe) tipe: kantor, wisata, hunian
    ("Grand Central / E 42 St", 40.7527, -73.9772, "kantor"), ("Penn Station / W 33 St", 40.7506, -73.9935, "kantor"),
    ("Wall St / Broad St", 40.7069, -74.0113, "kantor"), ("W 52 St / 6 Ave", 40.7611, -73.9798, "kantor"),
    ("Park Ave / E 50 St", 40.7566, -73.9739, "kantor"), ("Broadway / W 58 St", 40.7673, -73.9815, "kantor"),
    ("Central Park S / 6 Ave", 40.7658, -73.9763, "wisata"), ("Central Park W / W 72 St", 40.7761, -73.9762, "wisata"),
    ("Brooklyn Bridge Park / Pier 1", 40.7026, -73.9965, "wisata"), ("Hudson River Greenway / W 40 St", 40.7608, -73.9981, "wisata"),
    ("Washington Sq E", 40.7308, -73.9973, "wisata"), ("High Line / W 23 St", 40.7448, -74.0043, "wisata"),
    ("Lafayette St / E 8 St", 40.7305, -73.9908, "hunian"), ("E 7 St / Ave A", 40.7262, -73.9835, "hunian"),
    ("Bedford Ave / N 7 St", 40.7185, -73.9567, "hunian"), ("Lexington Ave / E 86 St", 40.7799, -73.9556, "hunian"),
    ("W 106 St / Amsterdam Ave", 40.7985, -73.9650, "hunian"), ("Jersey City / Grove St", 40.7195, -74.0431, "hunian"),
    ("Court St / Atlantic Ave", 40.6911, -73.9908, "hunian"), ("Henry St / Montague St", 40.6955, -73.9925, "hunian"),
    ("Hoboken Terminal", 40.7351, -74.0289, "kantor"), ("Union Sq W / E 16 St", 40.7360, -73.9915, "kantor"),
    ("Chelsea Market / 9 Ave", 40.7424, -74.0060, "wisata"), ("Astor Pl / Lafayette St", 40.7292, -73.9914, "hunian"),
]
TYPE_IDX = {t: [i for i, s in enumerate(STATIONS) if s[3] == t] for t in ("kantor", "wisata", "hunian")}


def weather(dates):
    doy = dates.dayofyear.values
    temp = 12 + 13 * np.sin((doy - 105) / 365 * 2 * np.pi) + rng.normal(0, 2.5, len(dates))
    rain = np.where(rng.random(len(dates)) < 0.22, rng.gamma(2, 4, len(dates)), 0.0)
    wind = np.clip(rng.normal(22, 7, len(dates)), 6, 55)
    return pd.DataFrame({"date": dates, "temp_c": temp.round(1), "precip_mm": rain.round(1), "wind_kmh": wind.round(1)})


def make(days, start):
    dates = pd.date_range(start, periods=days)
    w = weather(dates)
    rows = []
    rid = 0
    for _, wr in w.iterrows():
        d = wr["date"]
        weekend = d.dayofweek >= 5
        base = 2600 * (1.0 + 0.012 * (wr["temp_c"] - 15)) * (0.62 if wr["precip_mm"] >= 1 else 1.0)
        base *= 0.9 if weekend else 1.05
        base *= 1 - min(0.25, wr["wind_kmh"] / 300)
        n = int(rng.normal(base, base * 0.04))
        member_p = 0.50 if weekend else 0.78
        ut = np.where(rng.random(n) < member_p, "member", "casual")
        hours = np.empty(n, dtype=int)
        for u in ("member", "casual"):
            m = ut == u
            if u == "member" and not weekend:
                p = np.array([1, .5, .3, .3, .6, 2, 6, 12, 14, 7, 5, 5, 6, 5, 5, 6, 9, 14, 11, 6, 4, 3, 2, 1.5])
            elif u == "member":
                p = np.array([2, 1, .7, .5, .6, 1, 2, 4, 6, 8, 9, 10, 10, 9, 9, 8, 7, 6, 5, 4, 3, 3, 2.5, 2])
            else:
                p = np.array([1, .6, .4, .3, .3, .6, 1.5, 3, 4, 5, 6, 8, 9, 9.5, 9.5, 9, 8, 7, 5, 4, 3, 2.5, 2, 1.5])
            hours[m] = rng.choice(24, m.sum(), p=p / p.sum())
        minute = rng.integers(0, 60, n)
        sec = rng.integers(0, 60, n)
        dur = np.where(ut == "member", rng.lognormal(2.25, 0.55, n), rng.lognormal(2.85, 0.65, n))
        dur = np.clip(dur, 1.2, 180)
        elec = rng.random(n) < np.where(ut == "member", 0.38, 0.52)
        for i in range(n):
            h = hours[i]
            commute = (not weekend) and ut[i] == "member"
            if commute and 6 <= h <= 9:
                s = rng.choice(TYPE_IDX["hunian"] + TYPE_IDX["hunian"] + TYPE_IDX["kantor"], p=None)
                e = rng.choice(TYPE_IDX["kantor"])
            elif commute and 16 <= h <= 19:
                s = rng.choice(TYPE_IDX["kantor"]); e = rng.choice(TYPE_IDX["hunian"])
            elif ut[i] == "casual":
                s = rng.choice(TYPE_IDX["wisata"] + [rng.integers(0, len(STATIONS))]); e = rng.choice(TYPE_IDX["wisata"] + [rng.integers(0, len(STATIONS))])
            else:
                s, e = rng.integers(0, len(STATIONS)), rng.integers(0, len(STATIONS))
            st = d + pd.Timedelta(hours=int(h), minutes=int(minute[i]), seconds=int(sec[i]))
            S, E = STATIONS[s], STATIONS[e]
            rid += 1
            rows.append((f"R{rid:08d}", "electric_bike" if elec[i] else "classic_bike", st, st + pd.Timedelta(minutes=float(dur[i])),
                         S[0], f"S{s:03d}", E[0], f"S{e:03d}", S[1] + rng.normal(0, 4e-4), S[2] + rng.normal(0, 4e-4),
                         E[1] + rng.normal(0, 4e-4), E[2] + rng.normal(0, 4e-4), ut[i]))
    df = pd.DataFrame(rows, columns=["ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id",
                                     "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"])
    # durasi harus masuk akal terhadap jarak (kecepatan rata-rata 9-18 km/jam)
    la1, lo1, la2, lo2 = (np.radians(df[c].astype(float)) for c in ("start_lat", "start_lng", "end_lat", "end_lng"))
    a = np.sin((la2 - la1) / 2) ** 2 + np.cos(la1) * np.cos(la2) * np.sin((lo2 - lo1) / 2) ** 2
    dist = 2 * 6371.0 * np.arcsin(np.sqrt(a))
    need = dist / rng.uniform(9, 18, len(df)) * 60
    cur = (df["ended_at"] - df["started_at"]).dt.total_seconds() / 60
    df["ended_at"] = df["started_at"] + pd.to_timedelta(np.maximum(cur, need), unit="m")
    return df, w


def dirty(df):
    """Tambahkan data kotor supaya cleaning terlihat bekerja."""
    df = df.copy()
    k = len(df)
    df.loc[df.sample(int(k * 0.004), random_state=1).index, "end_station_name"] = None
    df.loc[df.sample(int(k * 0.003), random_state=2).index, "start_lat"] = None
    bad = df.sample(int(k * 0.004), random_state=3).index
    df.loc[bad, "ended_at"] = df.loc[bad, "started_at"] - pd.Timedelta(minutes=5)
    far = df.sample(int(k * 0.002), random_state=4).index
    df.loc[far, "end_lat"] = 0.0
    df.loc[far, "end_lng"] = 0.0
    return pd.concat([df, df.sample(int(k * 0.003), random_state=5)], ignore_index=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=120)
    ap.add_argument("--start", default="2026-05-01")
    ap.add_argument("--out", default="sample_data")
    ap.add_argument("--per-month", action="store_true", help="pecah jadi satu CSV per bulan")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    df, w = make(a.days, a.start)
    df = dirty(df)
    if a.per_month:
        ym = pd.to_datetime(df["started_at"]).dt.strftime("%Y%m")
        for k, g in df.groupby(ym):
            g.to_csv(out / f"{k}-citibike-sample.csv", index=False)
    else:
        df.to_csv(out / "citibike-sample.csv", index=False)
    w.to_csv(out / "weather_sample.csv", index=False)
    print(f"{len(df):,} baris ke {out}")
