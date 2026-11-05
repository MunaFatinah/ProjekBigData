"""Citi Bike Big Data - web app (input, pipeline Hadoop/NiFi, dashboard output).

Jalankan:  python app.py   lalu buka http://localhost:5000
"""
import json
import os
import re
import shutil
import threading
import time
from pathlib import Path

from flask import Flask, abort, jsonify, render_template, request, send_from_directory

from core import hdfs_utils, nifi_client
from core.intake import accept_file
from core.ml_analysis import load_clean, predict_user, run_analysis
from core.weather import get_weather

BASE = Path(__file__).parent
DATA = BASE / "data"
INCOMING, OUTBOX, CLEAN, RESULTS = (DATA / n for n in ("incoming", "outbox", "clean", "results"))
HDFS_CACHE = DATA / "hdfs_cache"
WEATHER_CACHE = DATA / "weather.csv"
NIFI_CFG = DATA / "nifi_config.json"
HDFS_DIR = os.environ.get("HDFS_DIR", "/citibike/clean")
for p in (INCOMING, OUTBOX, CLEAN, RESULTS):
    p.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 ** 3   # 2 GB

JOB = {"running": False, "pct": 0, "msg": "", "error": None, "finished": None}
EXPORTS = {"daily_trips", "hourly_profile", "station_flow", "top_stations", "forecast",
           "lag_correlation", "feature_importance", "recommendations"}


def _safe(name: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(name).name)
    return name or "upload.csv"


def _files(folder: Path):
    out = []
    for f in sorted(folder.glob("*.csv")):
        st = f.stat()
        out.append({"name": f.name, "size": st.st_size, "modified": time.strftime("%Y-%m-%d %H:%M", time.localtime(st.st_mtime))})
    return out


def _to_outbox(src: Path):
    """Salin ke outbox secara atomik: tulis dulu sebagai .part (tidak cocok filter .csv), baru ganti nama.
    Tanpa ini GetFile bisa mengambil file yang baru separuh tersalin."""
    tmp = OUTBOX / (src.name + ".part")
    shutil.copy2(src, tmp)
    os.replace(tmp, OUTBOX / src.name)


def _nifi_ensure(cfg=None):
    """Pastikan flow NiFi ada dan jalan. Return (ok, pesan)."""
    cfg = cfg or nifi_client.load_config(NIFI_CFG)
    if not nifi_client.is_configured(cfg):
        return False, "NiFi belum dihubungkan (isi kredensial di halaman NiFi dan Hadoop)."
    try:
        n = nifi_client.NiFi(cfg["url"], cfg["username"], cfg["password"])
        r = n.ensure_flow(str(OUTBOX.resolve()), HDFS_DIR, cfg["hadoop_conf"])
        return True, "Flow NiFi dibuat dan dijalankan." if r["created"] else "Flow NiFi berjalan."
    except nifi_client.NiFiError as e:
        return False, str(e)


# ------------------------------------------------------------------ halaman
@app.get("/")
def page_input():
    return render_template("input.html", page="input")


@app.get("/pipeline")
def page_pipeline():
    return render_template("pipeline.html", page="pipeline", hdfs_dir=HDFS_DIR,
                           outbox_path=str(OUTBOX.resolve()), clean_path=str(CLEAN.resolve()))


@app.get("/dashboard")
def page_dashboard():
    return render_template("dashboard.html", page="dashboard")


# ------------------------------------------------------------------ 1. input
@app.post("/api/upload")
def api_upload():
    files = request.files.getlist("files")
    if not files:
        return jsonify(error="Pilih minimal satu file CSV."), 400
    direct = request.form.get("hdfs_direct") == "1"
    reports = []
    for f in files:
        name = _safe(f.filename or "")
        if not name.lower().endswith(".csv"):
            reports.append({"file": name, "error": "Bukan file .csv"})
            continue
        raw = INCOMING / name
        f.save(raw)                                       # data sudah bersih (cleaning manual di luar aplikasi)
        try:
            dst = CLEAN / name
            rep = accept_file(raw, dst)                   # hanya validasi kolom, tanpa membuang baris
            raw.unlink(missing_ok=True)
            _to_outbox(dst)                               # NiFi GetFile memantau folder outbox
            rep["outbox"] = True
            ok, msg = _nifi_ensure()                      # flow dipastikan jalan otomatis, tanpa kerja manual
            rep["nifi"] = {"ok": ok, "msg": msg}
            if direct:
                try:
                    hdfs_utils.put(dst, HDFS_DIR)
                    rep["hdfs_direct"] = "ok"
                except Exception as e:                    # noqa: BLE001
                    rep["hdfs_direct"] = f"gagal: {e}"
            reports.append(rep)
        except Exception as e:                            # noqa: BLE001
            raw.unlink(missing_ok=True)
            (CLEAN / name).unlink(missing_ok=True)
            reports.append({"file": name, "error": str(e)})
    return jsonify(reports=reports)


# ------------------------------------------------------------------ 2. pipeline
@app.get("/api/pipeline/status")
def api_status():
    hd, outbox, clean = hdfs_utils.status(HDFS_DIR), _files(OUTBOX), _files(CLEAN)
    in_hdfs = {f["name"]: f["size"] for f in hd["files"]}
    waiting = {f["name"] for f in outbox}
    delivery = []
    for f in clean:                                   # posisi tiap file, berdasarkan bukti (bukan asumsi)
        if in_hdfs.get(f["name"]) is not None:
            st = "hdfs"
        elif f["name"] in waiting:
            st = "antre"
        else:
            st = "hilang"                             # sudah tidak di outbox tapi belum di HDFS: NiFi gagal/belum selesai
        delivery.append({"name": f["name"], "state": st})
    return jsonify(hdfs=hd, hdfs_dir=HDFS_DIR, outbox=outbox, clean=clean, delivery=delivery,
                   analysis={**JOB, "has_results": (RESULTS / "results.json").exists()})


@app.get("/api/nifi/config")
def api_nifi_config():
    c = nifi_client.load_config(NIFI_CFG)
    return jsonify(url=c["url"], username=c["username"], hadoop_conf=c["hadoop_conf"], has_password=bool(c["password"]))


@app.post("/api/nifi/connect")
def api_nifi_connect():
    b = request.get_json(silent=True) or {}
    cfg = nifi_client.load_config(NIFI_CFG)
    for k in ("url", "username", "password", "hadoop_conf"):
        if b.get(k):
            cfg[k] = b[k].strip()
    ok, msg = _nifi_ensure(cfg)
    if not ok:
        return jsonify(error=msg), 400
    nifi_client.save_config(NIFI_CFG, cfg)             # disimpan hanya kalau login dan pembuatan flow berhasil
    return jsonify(ok=True, msg=msg)


@app.get("/api/nifi/status")
def api_nifi_status():
    cfg = nifi_client.load_config(NIFI_CFG)
    if not nifi_client.is_configured(cfg):
        return jsonify(connected=False, reason="Belum dihubungkan.")
    try:
        return jsonify(nifi_client.NiFi(cfg["url"], cfg["username"], cfg["password"]).status())
    except nifi_client.NiFiError as e:
        return jsonify(connected=False, reason=str(e))


@app.post("/api/hdfs/sync")
def api_sync():
    if not hdfs_utils.available():
        return jsonify(error="Perintah `hdfs` tidak ditemukan di PATH."), 400
    sent, errs = [], []
    for f in sorted(CLEAN.glob("*.csv")):
        try:
            hdfs_utils.put(f, HDFS_DIR)
            sent.append(f.name)
        except Exception as e:                            # noqa: BLE001
            errs.append(f"{f.name}: {e}")
    return jsonify(sent=sent, errors=errs)


def _job(source):
    def prog(p, m):
        JOB.update(pct=p, msg=m)
    try:
        used = "local"
        files = []
        if source in ("auto", "hdfs") and hdfs_utils.available():
            prog(2, "Mengambil data bersih dari HDFS")
            files = hdfs_utils.fetch_dir(HDFS_DIR, HDFS_CACHE)
            if files:
                used = "hdfs"
        if not files:
            if source == "hdfs":
                raise RuntimeError("Tidak ada file CSV di HDFS. Cek NiFi atau tekan Sinkronkan.")
            files = sorted(CLEAN.glob("*.csv"))
        if not files:
            raise RuntimeError("Belum ada data bersih. Upload CSV di halaman Input dulu.")
        prog(5, f"Membaca {len(files)} file ({'HDFS' if used == 'hdfs' else 'lokal'})")
        df = load_clean(files)
        w = get_weather(df["date"].min().strftime("%Y-%m-%d"), df["date"].max().strftime("%Y-%m-%d"), WEATHER_CACHE)
        run_analysis(df, w, RESULTS, used, prog)
        JOB.update(error=None, finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception as e:                                # noqa: BLE001
        JOB.update(error=str(e), pct=0, msg="Gagal")
    finally:
        JOB["running"] = False


@app.post("/api/run-analysis")
def api_run():
    if JOB["running"]:
        return jsonify(error="Analisis sedang berjalan."), 409
    source = (request.get_json(silent=True) or {}).get("source", "auto")
    JOB.update(running=True, pct=1, msg="Memulai", error=None)
    threading.Thread(target=_job, args=(source,), daemon=True).start()
    return jsonify(started=True)


# ------------------------------------------------------------------ 3. output
@app.get("/api/results")
def api_results():
    p = RESULTS / "results.json"
    if not p.exists():
        return jsonify(error="Belum ada hasil analisis."), 404
    return app.response_class(p.read_text(encoding="utf-8"), mimetype="application/json")


@app.get("/api/export/<name>.csv")
def api_export(name):
    if name not in EXPORTS or not (RESULTS / f"{name}.csv").exists():
        abort(404)
    return send_from_directory(RESULTS, f"{name}.csv", as_attachment=True)


@app.post("/api/predict")
def api_predict():
    if not (RESULTS / "classifier.joblib").exists():
        return jsonify(error="Model belum dilatih. Jalankan analisis dulu."), 404
    b = request.get_json(force=True)
    try:
        r = predict_user(RESULTS, b["hour"], b["dow"], b["duration_min"], b["distance_km"], b.get("is_electric", 0))
    except (KeyError, ValueError) as e:
        return jsonify(error=f"Input tidak valid: {e}"), 400
    return jsonify(r)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
