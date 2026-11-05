"""Data cuaca harian New York dari Open-Meteo (gratis, tanpa API key)."""
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

API = "https://archive-api.open-meteo.com/v1/archive"
NYC = (40.7128, -74.0060)


def get_weather(start: str, end: str, cache: Path):
    """Return DataFrame [date, temp_c, precip_mm, wind_kmh] atau None kalau tidak tersedia."""
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    if cache.exists():
        c = pd.read_csv(cache, parse_dates=["date"])
        if len(c) and c["date"].min() <= s and c["date"].max() >= e - pd.Timedelta(days=7):
            return c
    e_req = min(e, pd.Timestamp(date.today() - timedelta(days=6)))   # arsip tertunda beberapa hari
    try:
        r = requests.get(API, params={
            "latitude": NYC[0], "longitude": NYC[1],
            "start_date": s.strftime("%Y-%m-%d"), "end_date": e_req.strftime("%Y-%m-%d"),
            "daily": "temperature_2m_mean,precipitation_sum,wind_speed_10m_max",
            "timezone": "America/New_York"}, timeout=25)
        r.raise_for_status()
        d = r.json()["daily"]
        w = pd.DataFrame({"date": pd.to_datetime(d["time"]), "temp_c": d["temperature_2m_mean"],
                          "precip_mm": d["precipitation_sum"], "wind_kmh": d["wind_speed_10m_max"]})
        cache.parent.mkdir(parents=True, exist_ok=True)
        w.to_csv(cache, index=False)
        return w
    except Exception:
        if cache.exists():
            return pd.read_csv(cache, parse_dates=["date"])
        return None
