"""Analisis Big Data Citi Bike: Business Intelligence + Machine Learning.

Tiga pertanyaan yang dijawab:
1. BI    : hari ini dipengaruhi faktor apa dari hari-hari lampau? (lag, cuaca, pola mingguan)
2. ML #1 : prediksi jumlah trip 7 hari ke depan (regresi, fitur lag + kalender)
3. ML #2 : klasifikasi pengguna member vs casual dari ciri perjalanannya
Lalu hasilnya diterjemahkan menjadi rekomendasi operasional (rebalancing stasiun, cuaca, promosi).
"""
import json
import math
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestRegressor
from sklearn.inspection import permutation_importance
from sklearn.metrics import accuracy_score, f1_score, mean_absolute_error, r2_score, roc_auc_score
from sklearn.model_selection import train_test_split

HARI = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]
CLS_FEATURES = ["hour", "dow", "is_weekend", "duration_min", "distance_km", "is_electric"]
CLS_LABELS = {"hour": "Jam mulai", "dow": "Hari dalam minggu", "is_weekend": "Akhir pekan",
              "duration_min": "Durasi perjalanan", "distance_km": "Jarak perjalanan", "is_electric": "Sepeda listrik"}
FACTOR_LABELS = {"temp_c": "Suhu hari ini", "precip_mm": "Curah hujan hari ini", "wind_kmh": "Angin maksimum hari ini",
                 "dow": "Hari dalam minggu", "lag1": "Trip kemarin", "lag7": "Trip 7 hari lalu",
                 "roll7": "Rata-rata trip 7 hari terakhir"}


def _py(o):
    if isinstance(o, dict):
        return {str(k): _py(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_py(v) for v in o]
    if isinstance(o, np.ndarray):
        return _py(o.tolist())
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating, float)):
        return None if (math.isnan(o) or math.isinf(o)) else float(o)
    if isinstance(o, pd.Timestamp):
        return o.strftime("%Y-%m-%d")
    return o


def load_clean(files) -> pd.DataFrame:
    frames = [pd.read_csv(f, low_memory=False) for f in files]
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset="ride_id")
    df["date"] = pd.to_datetime(df["date"])
    df["ended_at"] = pd.to_datetime(df["ended_at"])
    return df


# ---------------------------------------------------------------- pola umum (BI)
def _patterns(df):
    df["day_type"] = np.where(df["dow"] >= 5, "weekend", "weekday")
    ndays = df.groupby("day_type")["date"].nunique().to_dict()
    hp = df.groupby(["day_type", "hour", "member_casual"]).size().rename("trips").reset_index()
    hp["avg_per_day"] = hp.apply(lambda r: r["trips"] / ndays[r["day_type"]], axis=1)
    hourly = {}
    for dt in ("weekday", "weekend"):
        hourly[dt] = {}
        for u in ("member", "casual"):
            arr = [0.0] * 24
            for _, r in hp[(hp.day_type == dt) & (hp.member_casual == u)].iterrows():
                arr[int(r["hour"])] = float(r["avg_per_day"])
            hourly[dt][u] = arr

    per_date = df.groupby(["date", "dow", "member_casual"]).size().rename("n").reset_index()
    dow = per_date.groupby(["dow", "member_casual"])["n"].mean().unstack(fill_value=0).reindex(range(7), fill_value=0)
    dow_profile = {u: [float(dow[u].get(i, 0)) if u in dow else 0.0 for i in range(7)] for u in ("member", "casual")}
    return hp, hourly, dow_profile


def _station_flows(df):
    """Net flow per stasiun per blok jam (hari kerja). net > 0 = lebih banyak sepeda keluar daripada masuk."""
    wk = df[df["dow"] < 5]
    ndays = max(1, wk["date"].nunique())
    coords = df.groupby("start_station_name")[["start_lat", "start_lng"]].mean()
    blocks = {"pagi": range(6, 10), "sore": range(16, 20)}
    rows = []
    for name, hrs in blocks.items():
        out = wk[wk["hour"].isin(hrs)].groupby("start_station_name").size().rename("out")
        inn = wk[wk["end_hour"].isin(hrs)].groupby("end_station_name").size().rename("in")
        t = pd.concat([out, inn], axis=1).fillna(0)
        t = t[(t["out"] + t["in"]) >= max(20, ndays)]
        t["net"] = t["out"] - t["in"]
        t["net_per_day"] = t["net"] / ndays
        t["out_per_day"] = t["out"] / ndays
        t["in_per_day"] = t["in"] / ndays
        t["block"] = name
        t = t.join(coords, how="left").reset_index().rename(columns={"index": "station"})
        t = t.rename(columns={t.columns[0]: "station"})
        rows.append(t)
    flow = pd.concat(rows, ignore_index=True)
    return flow


# ---------------------------------------------------------------- model harian
def _daily_frame(df, weather):
    d = df.groupby("date").agg(trips=("ride_id", "size"), member_share=("is_member", "mean"),
                               avg_duration=("duration_min", "mean")).reset_index()
    full = pd.date_range(d["date"].min(), d["date"].max())
    gaps = full.difference(d["date"])
    note = None
    if len(gaps):                       # pakai segmen kontinu terbaru saja
        d = d[d["date"] > gaps.max()].reset_index(drop=True)
        note = f"Data punya {len(gaps)} hari kosong; model harian memakai rentang kontinu terbaru ({len(d)} hari)."
    if weather is not None:
        d = d.merge(weather, on="date", how="left")
    return d.sort_values("date").reset_index(drop=True), note


def _lag_features(trips: pd.Series):
    f = pd.DataFrame({"lag1": trips.shift(1), "lag2": trips.shift(2), "lag3": trips.shift(3), "lag7": trips.shift(7)})
    f["roll7"] = trips.shift(1).rolling(7).mean()
    return f


def _daily_models(d, progress):
    res = {"available": False, "note": None}
    n = len(d)
    if n < 14:
        res["note"] = "Butuh minimal 14 hari data harian untuk analisis lag dan prediksi. Upload data lebih banyak."
        return res
    d = d.copy()
    d["dow"] = d["date"].dt.dayofweek
    d["is_weekend"] = (d["dow"] >= 5).astype(int)
    d["ratio"] = d["trips"] / d.groupby("dow")["trips"].transform("mean")   # buang efek hari dalam minggu
    has_w = "temp_c" in d.columns and d["temp_c"].notna().sum() > 10

    # --- autokorelasi: tren dibuang supaya yang tersisa pola mingguan
    trend = d["trips"].rolling(7, center=True, min_periods=4).mean()
    detr = d["trips"] / trend
    ac = [{"lag": k, "corr": detr.corr(detr.shift(k))} for k in range(1, 15) if n > k + 8]

    # --- korelasi cuaca (efek hari dalam minggu sudah dibuang)
    wcorr, weffects = [], {}
    if has_w:
        for col, lab in [("temp_c", "Suhu"), ("precip_mm", "Hujan"), ("wind_kmh", "Angin")]:
            for k in range(0, 4):
                wcorr.append({"factor": lab, "lag": k, "corr": d["ratio"].corr(d[col].shift(k))})
        rainy = d["precip_mm"] >= 1
        if rainy.sum() >= 4 and (~rainy).sum() >= 4:
            weffects["rain_pct"] = (d.loc[rainy, "ratio"].mean() / d.loc[~rainy, "ratio"].mean() - 1) * 100
            weffects["rain_days"] = int(rainy.sum())
        ok = d[["temp_c", "ratio"]].dropna()
        if len(ok) > 10 and ok["temp_c"].std() > 0:
            weffects["temp_pct_per_c"] = np.polyfit(ok["temp_c"], ok["ratio"], 1)[0] * 100
            weffects["temp_range"] = [float(ok["temp_c"].min()), float(ok["temp_c"].max())]

    res.update({"available": True, "autocorr": ac, "weather_corr": wcorr, "weather_effects": weffects,
                "has_weather": bool(has_w)})

    # --- model faktor (BI): apa yang menjelaskan jumlah trip hari ini?
    progress(55, "Melatih model faktor penyebab (BI)")
    lf = _lag_features(d["trips"])
    X = pd.concat([d[["dow"]], lf[["lag1", "lag7", "roll7"]]], axis=1)
    if has_w:
        X = pd.concat([d[["temp_c", "precip_mm", "wind_kmh"]], X], axis=1)
    ok = X.notna().all(axis=1)
    Xv, yv = X[ok], d.loc[ok, "trips"]
    if len(Xv) >= 21:
        ntest = max(7, int(len(Xv) * 0.2))
        Xtr, Xte, ytr, yte = Xv.iloc[:-ntest], Xv.iloc[-ntest:], yv.iloc[:-ntest], yv.iloc[-ntest:]
        rf = RandomForestRegressor(n_estimators=300, min_samples_leaf=2, random_state=42, n_jobs=-1).fit(Xtr, ytr)
        pred = rf.predict(Xte)
        pi = permutation_importance(rf, Xte, yte, n_repeats=30, random_state=42, scoring="neg_mean_absolute_error")
        imp = np.clip(pi.importances_mean, 0, None)
        imp = imp / imp.sum() if imp.sum() > 0 else imp
        res["factors"] = {
            "importance": sorted([{"factor": FACTOR_LABELS[c], "key": c, "share": float(v)} for c, v in zip(Xv.columns, imp)],
                                 key=lambda r: -r["share"]),
            "r2": float(r2_score(yte, pred)), "mae": float(mean_absolute_error(yte, pred)), "test_days": int(len(yte)),
        }

    # --- model prediksi 7 hari (tanpa cuaca karena cuaca masa depan belum diketahui)
    progress(65, "Melatih model prediksi 7 hari")
    if n >= 35:
        F = lf.copy()
        F["dow"], F["is_weekend"] = d["dow"], d["is_weekend"]
        ok = F.notna().all(axis=1)
        Fv, yv = F[ok], d.loc[ok, "trips"]
        ntest = max(7, int(len(Fv) * 0.2))
        Ftr, Fte, ytr, yte = Fv.iloc[:-ntest], Fv.iloc[-ntest:], yv.iloc[:-ntest], yv.iloc[-ntest:]
        m = RandomForestRegressor(n_estimators=400, min_samples_leaf=2, random_state=42, n_jobs=-1).fit(Ftr, ytr)
        pred = m.predict(Fte)
        base = Fte["lag7"]                          # baseline naif: sama dengan minggu lalu
        mae, mae_b = mean_absolute_error(yte, pred), mean_absolute_error(yte, base)
        mape = float(np.mean(np.abs(yte - pred) / yte) * 100)
        sd = float(np.std(yte.values - pred))
        m = RandomForestRegressor(n_estimators=400, min_samples_leaf=2, random_state=42, n_jobs=-1).fit(Fv, yv)
        hist, dates, fc = list(d["trips"].astype(float)), list(d["date"]), []
        for _ in range(7):
            nd = dates[-1] + pd.Timedelta(days=1)
            row = pd.DataFrame([{"lag1": hist[-1], "lag2": hist[-2], "lag3": hist[-3], "lag7": hist[-7],
                                 "roll7": float(np.mean(hist[-7:])), "dow": nd.dayofweek, "is_weekend": int(nd.dayofweek >= 5)}])
            p = float(m.predict(row[Fv.columns])[0])
            hist.append(p); dates.append(nd)
            fc.append({"date": nd, "dow": HARI[nd.dayofweek], "pred": p, "lower": max(0.0, p - 1.28 * sd), "upper": p + 1.28 * sd})
        res["forecast"] = {"mae": float(mae), "mae_baseline": float(mae_b), "mape": mape,
                           "improvement_pct": float((1 - mae / mae_b) * 100) if mae_b > 0 else None,
                           "test_days": int(len(yte)), "predictions": fc,
                           "history": [{"date": r.date, "trips": r.trips} for r in d.tail(28).itertuples()]}
    else:
        res["note"] = "Prediksi 7 hari butuh minimal 35 hari data kontinu."
    return res


# ---------------------------------------------------------------- klasifikasi member vs casual
def _classification(df, out_dir: Path, progress):
    if df["is_member"].nunique() < 2:
        return {"available": False, "note": "Data hanya berisi satu tipe pengguna, klasifikasi dilewati."}
    progress(75, "Melatih model klasifikasi member vs casual")
    s = df.sample(min(len(df), 250_000), random_state=42)
    X, y = s[CLS_FEATURES], s["is_member"]
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    clf = HistGradientBoostingClassifier(max_iter=150, learning_rate=0.1, random_state=42).fit(Xtr, ytr)
    proba = clf.predict_proba(Xte)[:, 1]
    pred = (proba >= 0.5).astype(int)
    sub = Xte.sample(min(8000, len(Xte)), random_state=1)
    pi = permutation_importance(clf, sub, yte.loc[sub.index], n_repeats=3, random_state=42, scoring="roc_auc")
    imp = np.clip(pi.importances_mean, 0, None)
    imp = imp / imp.sum() if imp.sum() > 0 else imp
    joblib.dump({"model": clf, "features": CLS_FEATURES}, out_dir / "classifier.joblib")
    return {"available": True, "rows_used": int(len(s)),
            "accuracy": float(accuracy_score(yte, pred)), "f1_macro": float(f1_score(yte, pred, average="macro")),
            "roc_auc": float(roc_auc_score(yte, proba)), "baseline_accuracy": float(max(y.mean(), 1 - y.mean())),
            "importance": sorted([{"factor": CLS_LABELS[c], "share": float(v)} for c, v in zip(CLS_FEATURES, imp)],
                                 key=lambda r: -r["share"])}


def predict_user(out_dir: Path, hour, dow, duration_min, distance_km, is_electric):
    pkg = joblib.load(out_dir / "classifier.joblib")
    row = pd.DataFrame([{"hour": int(hour), "dow": int(dow), "is_weekend": int(int(dow) >= 5),
                         "duration_min": float(duration_min), "distance_km": float(distance_km),
                         "is_electric": int(is_electric)}])[pkg["features"]]
    p = float(pkg["model"].predict_proba(row)[0, 1])
    return {"member": p, "casual": 1 - p, "label": "member" if p >= 0.5 else "casual"}


# ---------------------------------------------------------------- insight & rekomendasi
def _fmt(n, d=0):
    return f"{n:,.{d}f}".replace(",", "#").replace(".", ",").replace("#", ".")


def _build_texts(meta, hourly, dow_profile, flow, daily_res, cls):
    ins, rec = [], []
    wk = hourly["weekday"]
    pm, pc = int(np.argmax(wk["member"])), int(np.argmax(wk["casual"]))
    ins.append(f"Pada hari kerja, pengguna member memuncak pukul {pm:02d}.00 sedangkan casual pukul {pc:02d}.00. Pola member mengikuti jam komuter.")
    tot_dow = np.array(dow_profile["member"]) + np.array(dow_profile["casual"])
    ins.append(f"Hari tersibuk adalah {HARI[int(tot_dow.argmax())]} (rata-rata {_fmt(tot_dow.max())} trip) dan tersepi {HARI[int(tot_dow.argmin())]} ({_fmt(tot_dow.min())} trip).")
    wkd_casual = np.mean(dow_profile["casual"][5:]) / max(1e-9, tot_dow[5:].mean())
    wk_casual = np.mean(dow_profile["casual"][:5]) / max(1e-9, tot_dow[:5].mean())
    ins.append(f"Porsi casual naik dari {wk_casual*100:.0f}% di hari kerja menjadi {wkd_casual*100:.0f}% di akhir pekan.")

    if daily_res.get("available"):
        ac = [a for a in daily_res["autocorr"] if a["corr"] is not None and not math.isnan(a["corr"])]
        if ac:
            best = max(ac, key=lambda a: a["corr"])
            kuat = "kuat" if best["corr"] >= 0.5 else "sedang" if best["corr"] >= 0.3 else "lemah"
            mingguan = " Jedanya kelipatan 7 hari, jadi ada siklus mingguan." if best["lag"] % 7 == 0 else ""
            ins.append(f"Jumlah trip hari ini paling mirip dengan {best['lag']} hari sebelumnya (korelasi {_fmt(best['corr'], 2)}, tergolong {kuat}, tren sudah dibuang).{mingguan}")
        we = daily_res.get("weather_effects", {})
        if "rain_pct" in we:
            ins.append(f"Pada {we['rain_days']} hari hujan (≥1 mm), jumlah trip {abs(we['rain_pct']):.0f}% {'lebih rendah' if we['rain_pct'] < 0 else 'lebih tinggi'} dibanding hari kering di hari yang sama dalam minggu.")
        if "temp_pct_per_c" in we:
            ins.append(f"Setiap kenaikan 1°C berasosiasi dengan perubahan trip sekitar {'+' if we['temp_pct_per_c'] > 0 else ''}{_fmt(we['temp_pct_per_c'], 1)}% pada rentang {we['temp_range'][0]:.0f}–{we['temp_range'][1]:.0f}°C.")
        fac = daily_res.get("factors")
        if fac:
            top = fac["importance"][0]
            ins.append(f"Faktor terpenting untuk menjelaskan trip harian: {top['factor']} ({top['share']*100:.0f}% kontribusi model).")
    if cls.get("available"):
        ins.append(f"Member vs casual dapat dibedakan dengan akurasi {_fmt(cls['accuracy']*100, 1)}% (AUC {_fmt(cls['roc_auc'], 2)}); petunjuk terkuatnya adalah {cls['importance'][0]['factor'].lower()}.")

    # --- rekomendasi
    for blk, title in (("pagi", "pagi (06.00–10.00)"), ("sore", "sore (16.00–20.00)")):
        f = flow[flow["block"] == blk]
        out = f.sort_values("net_per_day", ascending=False).head(3)
        inn = f.sort_values("net_per_day").head(3)
        if len(out) and out.iloc[0]["net_per_day"] > 0:
            names = ", ".join(out["station"])
            rec.append({"judul": f"Isi ulang stok sepeda sebelum jam sibuk {blk}",
                        "isi": f"Stasiun {names} kehilangan sepeda paling cepat pada blok {title}. Jadwalkan truk rebalancing sebelum blok ini dimulai.",
                        "bukti": f"Net keluar tertinggi: {out.iloc[0]['station']} ({out.iloc[0]['net_per_day']:.1f} sepeda/hari lebih banyak keluar daripada masuk)."})
        if len(inn) and inn.iloc[0]["net_per_day"] < 0:
            names = ", ".join(inn["station"])
            rec.append({"judul": f"Kosongkan dock yang menumpuk pada blok {blk}",
                        "isi": f"Stasiun {names} menerima lebih banyak sepeda daripada yang keluar pada blok {title}. Angkut sepeda berlebih ke stasiun yang kekurangan agar dock tidak penuh.",
                        "bukti": f"Net masuk tertinggi: {inn.iloc[0]['station']} ({abs(inn.iloc[0]['net_per_day']):.1f} sepeda/hari)."})
    we = daily_res.get("weather_effects", {}) if daily_res.get("available") else {}
    if we.get("rain_pct", 0) < -5:
        rec.append({"judul": "Siapkan skenario operasional hari hujan",
                    "isi": "Kurangi jadwal rebalancing dan ajukan perawatan armada pada hari yang diprakirakan hujan, karena permintaan turun jelas.",
                    "bukti": f"Trip turun rata-rata {abs(we['rain_pct']):.0f}% saat hujan ≥1 mm ({we['rain_days']} hari hujan pada data)."})
    if wkd_casual - wk_casual > 0.05:
        rec.append({"judul": "Tawarkan pass akhir pekan untuk konversi casual ke member",
                    "isi": "Pengguna casual terkonsentrasi di akhir pekan dan siang hari. Promosi pass akhir pekan atau paket 3 hari di stasiun wisata dapat mengonversi mereka.",
                    "bukti": f"Casual {wkd_casual*100:.0f}% dari trip akhir pekan vs {wk_casual*100:.0f}% di hari kerja."})
    F = daily_res.get("forecast") or {}
    fc = F.get("predictions")
    if fc:
        top = max(fc, key=lambda r: r["pred"])
        low = min(fc, key=lambda r: r["pred"])
        days = f"Hari tersibuk berikutnya {top['dow']} {top['date'].strftime('%d/%m')} (sekitar {_fmt(top['pred'])} trip) dan tersepi {low['dow']} {low['date'].strftime('%d/%m')} (sekitar {_fmt(low['pred'])} trip)."
        better = F.get("improvement_pct") is not None and F["improvement_pct"] >= 0
        if better:
            rec.append({"judul": "Atur jadwal armada dan staf dengan prediksi 7 hari",
                        "isi": days + " Tempatkan lebih banyak teknisi dan truk di hari tersibuk, jadwalkan perawatan di hari tersepi.",
                        "bukti": f"Galat model {F['mape']:.1f}% (MAPE) pada {F['test_days']} hari uji, {F['improvement_pct']:.0f}% lebih akurat dari tebakan minggu lalu."})
        else:
            rec.append({"judul": "Pakai prediksi 7 hari sebagai kisaran, bukan angka pasti",
                        "isi": days + " Model belum mengalahkan tebakan sederhana minggu lalu, karena cuaca (faktor terbesar) belum diketahui saat memprediksi. Gabungkan dengan prakiraan cuaca sebelum menetapkan jadwal armada.",
                        "bukti": f"Galat model {F['mape']:.1f}% (MAPE) pada {F['test_days']} hari uji; galat tebakan minggu lalu {F['mae_baseline']:.0f} trip vs model {F['mae']:.0f} trip."})
    return ins, rec


# ---------------------------------------------------------------- orkestrasi
def run_analysis(df: pd.DataFrame, weather, out_dir: Path, source: str, progress=lambda p, m: None):
    t0 = time.time()
    out_dir.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["ended_at"] = pd.to_datetime(df["ended_at"])
    df["end_hour"] = df["ended_at"].dt.hour
    df["is_member"] = (df["member_casual"] == "member").astype(int)
    df["is_electric"] = df["rideable_type"].astype(str).str.contains("electric").astype(int)

    progress(10, "Menghitung pola jam dan hari (BI)")
    hp, hourly, dow_profile = _patterns(df)
    progress(25, "Menghitung arus sepeda per stasiun")
    flow = _station_flows(df)
    top = (df.groupby("start_station_name").agg(trips=("ride_id", "size"), lat=("start_lat", "mean"), lng=("start_lng", "mean"),
                                                member_share=("is_member", "mean")).sort_values("trips", ascending=False).head(15).reset_index())
    progress(40, "Menyusun data harian dan menggabungkan cuaca")
    daily, gap_note = _daily_frame(df, weather)
    daily_res = _daily_models(daily, progress)
    if gap_note:
        daily_res["note"] = gap_note
    cls = _classification(df, out_dir, progress)

    progress(90, "Menyusun insight dan rekomendasi")
    meta = {"rows": len(df), "stations": int(df["start_station_name"].nunique()),
            "date_min": df["date"].min(), "date_max": df["date"].max(), "days": int(df["date"].nunique()),
            "member_share": float(df["is_member"].mean()), "avg_duration": float(df["duration_min"].mean()),
            "electric_share": float(df["is_electric"].mean()), "avg_distance": float(df["distance_km"].mean()),
            "source": source, "weather": bool(daily_res.get("has_weather"))}
    insights, recs = _build_texts(meta, hourly, dow_profile, flow, daily_res, cls)

    # ---- CSV untuk Power BI
    d = daily.copy()
    d.to_csv(out_dir / "daily_trips.csv", index=False)
    hp[["day_type", "hour", "member_casual", "trips", "avg_per_day"]].to_csv(out_dir / "hourly_profile.csv", index=False)
    flow.round(3).to_csv(out_dir / "station_flow.csv", index=False)
    top.round(5).to_csv(out_dir / "top_stations.csv", index=False)
    if daily_res.get("forecast"):
        pd.DataFrame(daily_res["forecast"]["predictions"]).to_csv(out_dir / "forecast.csv", index=False)
    lag_rows = [{"jenis": "autokorelasi_trip", "faktor": "Trip", **a} for a in daily_res.get("autocorr", [])]
    lag_rows += [{"jenis": "korelasi_cuaca", "faktor": w["factor"], "lag": w["lag"], "corr": w["corr"]} for w in daily_res.get("weather_corr", [])]
    pd.DataFrame(lag_rows, columns=["jenis", "faktor", "lag", "corr"]).to_csv(out_dir / "lag_correlation.csv", index=False)
    imp_rows = [{"model": "faktor_trip_harian", **r} for r in (daily_res.get("factors") or {}).get("importance", [])]
    imp_rows += [{"model": "klasifikasi_member_casual", **r} for r in cls.get("importance", [])]
    pd.DataFrame(imp_rows, columns=["model", "factor", "key", "share"]).to_csv(out_dir / "feature_importance.csv", index=False)
    pd.DataFrame(recs).to_csv(out_dir / "recommendations.csv", index=False)

    def block_top(b, asc):
        f = flow[flow["block"] == b].sort_values("net_per_day", ascending=asc).head(8)
        return f[["station", "out_per_day", "in_per_day", "net_per_day", "start_lat", "start_lng"]].to_dict("records")

    results = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"), "seconds": round(time.time() - t0, 1), "meta": meta,
        "hourly": hourly, "dow": dow_profile, "top_stations": top.to_dict("records"),
        "daily": {"dates": [x.strftime("%Y-%m-%d") for x in daily["date"]], "trips": daily["trips"].tolist(),
                  "temp": daily["temp_c"].tolist() if "temp_c" in daily else None,
                  "precip": daily["precip_mm"].tolist() if "precip_mm" in daily else None},
        "models": daily_res, "classification": cls,
        "flows": {"pagi_keluar": block_top("pagi", False), "pagi_masuk": block_top("pagi", True),
                  "sore_keluar": block_top("sore", False), "sore_masuk": block_top("sore", True)},
        "insights": insights, "recommendations": recs,
    }
    (out_dir / "results.json").write_text(json.dumps(_py(results), ensure_ascii=False), encoding="utf-8")
    progress(100, "Selesai")
    return results
