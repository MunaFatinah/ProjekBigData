"""Klien REST NiFi: membuat dan menjalankan flow GetFile -> PutHDFS secara otomatis.

Dengan ini pengguna tidak perlu menyeret processor di UI NiFi. Aplikasi:
  1. login ke NiFi (token JWT),
  2. membuat process group "CitiBike" berisi GetFile -> PutHDFS (kalau belum ada),
  3. mengisi properti (folder outbox, config Hadoop, folder HDFS),
  4. menjalankannya, lalu melaporkan status tiap langkah apa adanya.

Semua pemanggilan idempoten: aman dipanggil berulang (tiap upload).
"""
import json
import os
import shutil
import time
from pathlib import Path

import requests

try:                                              # NiFi memakai sertifikat self-signed
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:                                 # noqa: BLE001
    pass

GROUP_NAME = "CitiBike"
CLIENT_ID = "citibike-app"
GETFILE = "org.apache.nifi.processors.standard.GetFile"
PUTHDFS = "org.apache.nifi.processors.hadoop.PutHDFS"


class NiFiError(RuntimeError):
    pass


# ------------------------------------------------------------------ konfigurasi
def detect_hadoop_conf() -> str:
    """Cari core-site.xml dan hdfs-site.xml dari HADOOP_CONF_DIR, HADOOP_HOME, atau lokasi perintah `hdfs`."""
    cands = []
    if os.environ.get("HADOOP_CONF_DIR"):
        cands.append(Path(os.environ["HADOOP_CONF_DIR"]))
    if os.environ.get("HADOOP_HOME"):
        cands.append(Path(os.environ["HADOOP_HOME"]) / "etc" / "hadoop")
    exe = shutil.which("hdfs")
    if exe:
        cands.append(Path(exe).resolve().parent.parent / "etc" / "hadoop")
    for d in cands:
        files = [d / n for n in ("core-site.xml", "hdfs-site.xml") if (d / n).exists()]
        if files:
            return ",".join(str(f) for f in files)
    return ""


def load_config(path: Path) -> dict:
    cfg = {"url": os.environ.get("NIFI_URL", "https://localhost:8443"),
           "username": os.environ.get("NIFI_USER", ""), "password": os.environ.get("NIFI_PASS", ""),
           "hadoop_conf": ""}
    if path.exists():
        try:
            cfg.update({k: v for k, v in json.loads(path.read_text(encoding="utf-8")).items() if v not in (None, "")})
        except Exception:                         # noqa: BLE001
            pass
    if not cfg["hadoop_conf"]:
        cfg["hadoop_conf"] = detect_hadoop_conf()
    return cfg


def save_config(path: Path, cfg: dict):
    path.write_text(json.dumps({k: cfg.get(k, "") for k in ("url", "username", "password", "hadoop_conf")}, indent=2),
                    encoding="utf-8")


def is_configured(cfg: dict) -> bool:
    return bool(cfg.get("username") and cfg.get("password"))


# ------------------------------------------------------------------ klien
class NiFi:
    def __init__(self, url, username, password, timeout=20):
        self.base = url.rstrip("/") + "/nifi-api"
        self.user, self.pw, self.timeout = username, password, timeout
        self.s = requests.Session()
        self.s.verify = False
        self.token = None

    def _login(self):
        try:
            r = self.s.post(self.base + "/access/token", data={"username": self.user, "password": self.pw},
                            timeout=self.timeout)
        except requests.exceptions.SSLError as e:
            raise NiFiError(f"Gagal HTTPS ke NiFi: {e}. Pastikan URL memakai https:// dan port yang benar.")
        except requests.exceptions.ConnectionError:
            raise NiFiError(f"NiFi tidak terjangkau di {self.base}. Pastikan NiFi sudah menyala "
                            "(setelah start, tunggu 1-2 menit sampai UI bisa dibuka di browser).")
        except requests.exceptions.Timeout:
            raise NiFiError("NiFi tidak merespons (timeout).")
        if r.status_code in (400, 401):
            raise NiFiError("Username atau password NiFi salah. Di NiFi 2.x kredensial awal dibuat otomatis; "
                            "cari kata 'Generated' di folder-nifi/logs/nifi-app.log.")
        if r.status_code != 201 and r.status_code != 200:
            raise NiFiError(f"Login NiFi gagal (HTTP {r.status_code}): {r.text[:200]}")
        self.token = r.text.strip()

    def _req(self, method, path, **kw):
        if not self.token:
            self._login()
        for attempt in (1, 2):
            h = {"Authorization": f"Bearer {self.token}"}
            if "json" in kw:
                h["Content-Type"] = "application/json"
            try:
                r = self.s.request(method, self.base + path, headers=h, timeout=self.timeout, **kw)
            except requests.exceptions.RequestException as e:
                raise NiFiError(f"Permintaan ke NiFi gagal: {e}")
            if r.status_code == 401 and attempt == 1:     # token kedaluwarsa
                self._login()
                continue
            break
        if r.status_code >= 300:
            try:
                msg = r.json().get("message") if r.headers.get("content-type", "").startswith("application/json") else r.text
            except Exception:                              # noqa: BLE001
                msg = r.text
            raise NiFiError(f"NiFi menolak {method} {path} (HTTP {r.status_code}): {str(msg)[:300]}")
        return r.json() if r.content else {}

    # ---------------------------------------------------------- bagian flow
    def root_id(self):
        return self._req("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]

    def processor_types(self):
        return {t["type"]: t.get("bundle") for t in self._req("GET", "/flow/processor-types")["processorTypes"]}

    def find_group(self, root):
        for g in self._req("GET", f"/process-groups/{root}/process-groups").get("processGroups", []):
            if g["component"]["name"] == GROUP_NAME:
                return g["id"]
        return None

    def group_processors(self, gid):
        return self._req("GET", f"/process-groups/{gid}/processors").get("processors", [])

    def group_connections(self, gid):
        return self._req("GET", f"/process-groups/{gid}/connections").get("connections", [])

    def _create_processor(self, gid, ptype, bundle, name, x, y):
        comp = {"type": ptype, "name": name, "position": {"x": x, "y": y}}
        if bundle:
            comp["bundle"] = bundle
        return self._req("POST", f"/process-groups/{gid}/processors",
                         json={"revision": {"clientId": CLIENT_ID, "version": 0}, "component": comp})

    def _configure(self, proc, config):
        body = {"revision": proc["revision"], "component": {"id": proc["id"], "config": config}}
        return self._req("PUT", f"/processors/{proc['id']}", json=body)

    def _set_group_state(self, gid, state):
        self._req("PUT", f"/flow/process-groups/{gid}", json={"id": gid, "state": state})

    def _wait_stopped(self, gid, seconds=15):
        end = time.time() + seconds
        while time.time() < end:
            if all(p["component"]["state"] != "RUNNING" and p["status"]["aggregateSnapshot"].get("activeThreadCount", 0) == 0
                   for p in self.group_processors(gid)):
                return
            time.sleep(0.7)

    # ---------------------------------------------------------- operasi utama
    def ensure_flow(self, outbox: str, hdfs_dir: str, hadoop_conf: str) -> dict:
        """Buat (kalau belum ada) lalu jalankan flow. Return ringkasan langkah."""
        if not hadoop_conf:
            raise NiFiError("Lokasi core-site.xml dan hdfs-site.xml belum diketahui. Isi kolom 'Config Hadoop' "
                            "(biasanya <folder-hadoop>\\etc\\hadoop\\core-site.xml,<folder-hadoop>\\etc\\hadoop\\hdfs-site.xml).")
        types = self.processor_types()
        for t in (GETFILE, PUTHDFS):
            if t not in types:
                raise NiFiError(f"NiFi ini tidak punya processor {t.split('.')[-1]}. Untuk PutHDFS, pasang "
                                "nifi-hadoop-nar di folder lib NiFi lalu restart NiFi.")
        root = self.root_id()
        gid = self.find_group(root)
        created = False
        if not gid:
            g = self._req("POST", f"/process-groups/{root}/process-groups",
                          json={"revision": {"clientId": CLIENT_ID, "version": 0},
                                "component": {"name": GROUP_NAME, "position": {"x": 200, "y": 150}}})
            gid, created = g["id"], True

        want_get = {"properties": {"Input Directory": outbox, "File Filter": r".*\.csv", "Keep Source File": "false",
                                   "Minimum File Age": "3 sec", "Polling Interval": "5 sec"},
                    "schedulingPeriod": "5 sec", "autoTerminatedRelationships": []}
        want_put = {"properties": {"Hadoop Configuration Resources": hadoop_conf, "Directory": hdfs_dir,
                                   "Conflict Resolution Strategy": "replace"},
                    "autoTerminatedRelationships": ["success", "failure"]}

        procs = {p["component"]["type"]: p for p in self.group_processors(gid)}
        if GETFILE not in procs:
            procs[GETFILE] = self._create_processor(gid, GETFILE, types[GETFILE], "GetFile (outbox)", 100, 100)
        if PUTHDFS not in procs:
            procs[PUTHDFS] = self._create_processor(gid, PUTHDFS, types[PUTHDFS], "PutHDFS", 100, 400)

        def differs(p, want):
            have = p["component"]["config"].get("properties", {})
            if any(have.get(k) != v for k, v in want["properties"].items()):
                return True
            return sorted(p["component"]["config"].get("autoTerminatedRelationships") or []) != sorted(want["autoTerminatedRelationships"])

        if differs(procs[GETFILE], want_get) or differs(procs[PUTHDFS], want_put):
            self._set_group_state(gid, "STOPPED")
            self._wait_stopped(gid)
            procs = {p["component"]["type"]: p for p in self.group_processors(gid)}
            if differs(procs[GETFILE], want_get):
                self._configure(procs[GETFILE], want_get)
            if differs(procs[PUTHDFS], want_put):
                self._configure(procs[PUTHDFS], want_put)

        if not self.group_connections(gid):
            a, b = procs[GETFILE], procs[PUTHDFS]
            self._req("POST", f"/process-groups/{gid}/connections", json={
                "revision": {"clientId": CLIENT_ID, "version": 0},
                "component": {"source": {"id": a["id"], "groupId": gid, "type": "PROCESSOR"},
                              "destination": {"id": b["id"], "groupId": gid, "type": "PROCESSOR"},
                              "selectedRelationships": ["success"]}})

        invalid = []
        for _ in range(8):                      # NiFi memvalidasi secara asinkron; beri waktu beberapa detik
            invalid = []
            for p in self.group_processors(gid):
                if p["component"].get("validationStatus") in ("INVALID", "VALIDATING"):
                    invalid.append(f"{p['component']['name']}: " + "; ".join(p["component"].get("validationErrors", [])))
            if not invalid:
                break
            time.sleep(1)
        if invalid:
            raise NiFiError("Flow dibuat tetapi belum valid: " + " | ".join(invalid))

        self._set_group_state(gid, "RUNNING")
        return {"group_id": gid, "created": created}

    def status(self) -> dict:
        """Status flow untuk ditampilkan di halaman pipeline."""
        root = self.root_id()
        gid = self.find_group(root)
        if not gid:
            return {"connected": True, "flow": False, "processors": [], "queued": 0, "errors": []}
        procs, errors = [], []
        for p in self.group_processors(gid):
            c = p["component"]
            procs.append({"name": c["name"], "state": c["state"], "valid": c.get("validationStatus"),
                          "problems": c.get("validationErrors", [])})
            for b in p.get("bulletins", []):
                bb = b.get("bulletin", {})
                if bb.get("level") in ("ERROR", "WARN"):
                    errors.append(f"{c['name']}: {bb.get('message', '')[:300]}")
        snap = self._req("GET", f"/flow/process-groups/{gid}/status")["processGroupStatus"]["aggregateSnapshot"]
        return {"connected": True, "flow": True, "group_id": gid, "processors": procs,
                "queued": snap.get("flowFilesQueued", 0), "errors": errors[-3:]}
