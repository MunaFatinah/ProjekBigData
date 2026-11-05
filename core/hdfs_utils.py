"""Pembungkus tipis untuk perintah `hdfs dfs` (Hadoop harus sudah terpasang & jalan)."""
import shutil
import subprocess
from pathlib import Path


def _bin():
    return shutil.which("hdfs")


def available() -> bool:
    return _bin() is not None


def _run(args, timeout=60):
    p = subprocess.run([_bin(), "dfs", *args], capture_output=True, text=True, timeout=timeout)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


_FS = {}


def default_fs() -> str:
    """Nilai fs.defaultFS yang dipakai perintah `hdfs` (file:/// berarti data akan masuk disk lokal, bukan HDFS)."""
    if "v" not in _FS:
        try:
            p = subprocess.run([_bin(), "getconf", "-confKey", "fs.defaultFS"], capture_output=True, text=True, timeout=40)
            _FS["v"] = (p.stdout or "").strip().splitlines()[-1] if p.returncode == 0 and p.stdout.strip() else ""
        except Exception:                                  # noqa: BLE001
            _FS["v"] = ""
    return _FS["v"]


def status(hdfs_dir: str) -> dict:
    """Cek apakah HDFS bisa dihubungi dan daftar file di folder target."""
    if not available():
        return {"ok": False, "files": [],
                "reason": "Perintah `hdfs` tidak ditemukan di PATH. Pastikan %HADOOP_HOME%\\bin sudah masuk PATH."}
    try:
        code, out = _run(["-ls", hdfs_dir], timeout=25)
    except subprocess.TimeoutExpired:
        return {"ok": False, "files": [], "reason": "HDFS tidak merespons. Cek NameNode dengan `jps`."}
    if code != 0:
        if "No such file" in out:
            return {"ok": True, "files": [], "reason": f"Folder {hdfs_dir} belum ada. NiFi atau tombol Sinkronkan akan membuatnya."}
        lines = out.strip().splitlines()
        return {"ok": False, "files": [], "reason": lines[-1] if lines else "HDFS error"}
    files = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 8 and not line.startswith("Found"):
            files.append({"name": Path(parts[-1]).name, "size": int(parts[4]), "modified": f"{parts[5]} {parts[6]}"})
    fs = default_fs()
    if fs.startswith("file:"):
        return {"ok": False, "files": files, "default_fs": fs,
                "reason": "fs.defaultFS = file:/// : perintah hdfs menulis ke disk lokal, bukan HDFS. "
                          "Cek core-site.xml (fs.defaultFS harus hdfs://localhost:9000) dan HADOOP_CONF_DIR."}
    return {"ok": True, "files": files, "default_fs": fs, "reason": ""}


def put(local_file: Path, hdfs_dir: str):
    if not available():
        raise RuntimeError("perintah `hdfs` tidak ditemukan di PATH")
    _run(["-mkdir", "-p", hdfs_dir])
    code, out = _run(["-put", "-f", str(local_file), hdfs_dir], timeout=600)
    if code != 0:
        raise RuntimeError(out.strip())
    # Jangan percaya kode keluar saja: pastikan file benar-benar ada di HDFS dengan ukuran sama.
    names = {f["name"]: f["size"] for f in status(hdfs_dir)["files"]}
    if names.get(Path(local_file).name) != Path(local_file).stat().st_size:
        raise RuntimeError("perintah put selesai tetapi file tidak terbaca di HDFS dengan ukuran yang sama "
                           f"(fs.defaultFS = {default_fs() or '?'})")


def fetch_dir(hdfs_dir: str, local_dir: Path) -> list:
    """Salin semua CSV dari HDFS ke folder cache lokal supaya bisa dibaca pandas."""
    local_dir.mkdir(parents=True, exist_ok=True)
    got = []
    for f in status(hdfs_dir)["files"]:
        if f["name"].lower().endswith(".csv"):
            dst = local_dir / f["name"]
            if dst.exists():
                dst.unlink()
            code, _ = _run(["-get", f"{hdfs_dir.rstrip('/')}/{f['name']}", str(dst)], timeout=600)
            if code == 0:
                got.append(dst)
    return got
