"""
API data-source adapters for the streaming producer.

Each adapter calls a free, key-less public API and turns the response into
records that follow the project schema:

    record_id, timestamp (ISO-8601 UTC), value (float), metric_type, sensor_id,
    source, unit, location, latitude, longitude, source_timestamp

Adding a new API = subclass `DataSource`, implement `fetch()`, register it in
`SOURCES`.  Nothing else in the pipeline has to change.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import requests

import config

log = logging.getLogger(__name__)

Record = Dict[str, Any]
Getter = Callable[..., Any]


class APIError(RuntimeError):
    """Raised when an upstream API cannot be reached or returns bad data."""


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def epoch_to_iso(seconds: float) -> str:
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def http_get_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: float = 10,
    retries: int = 3,
    backoff: float = 2.0,
) -> Any:
    """GET + JSON decode with retries. Raises APIError after the last attempt."""
    hdrs = {"User-Agent": "bigdata-streaming-project/1.0", **(headers or {})}
    last: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=hdrs, timeout=timeout)
            if resp.status_code == 429:
                raise APIError("rate limited (HTTP 429)")
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError, APIError) as exc:
            last = exc
            log.warning("GET %s failed (attempt %d/%d): %s", url, attempt, retries, exc)
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise APIError(f"GET {url} failed after {retries} attempts: {last}")


def make_record(
    source: str,
    sensor_id: str,
    metric_type: str,
    value: Any,
    unit: str,
    timestamp: str,
    location: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    source_timestamp: Optional[str] = None,
) -> Optional[Record]:
    """Build one schema-conformant record, or None if `value` is not numeric."""
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    if val != val:  # NaN
        return None
    key = f"{source}|{sensor_id}|{metric_type}|{timestamp}"
    return {
        "record_id": hashlib.sha1(key.encode()).hexdigest()[:24],  # idempotent upserts
        "timestamp": timestamp,
        "value": round(val, 6),
        "metric_type": metric_type,
        "sensor_id": sensor_id,
        "source": source,
        "unit": unit,
        "location": location,
        "latitude": latitude,
        "longitude": longitude,
        "source_timestamp": source_timestamp,
    }


class DataSource:
    """Base class for an API adapter."""

    name = "base"
    description = ""

    def __init__(self, getter: Getter = http_get_json):
        self._get = getter  # injectable for tests

    def fetch(self) -> List[Record]:  # pragma: no cover - interface
        raise NotImplementedError


# --------------------------------------------------------------------------- #
# 1. Crypto market prices  -- CoinGecko
# --------------------------------------------------------------------------- #
class CryptoSource(DataSource):
    """https://docs.coingecko.com/reference/simple-price  (no key required)."""

    name = "crypto"
    description = "Cryptocurrency prices (USD/PHP) and 24h change - CoinGecko"
    URL = "https://api.coingecko.com/api/v3/simple/price"
    COINS = ["bitcoin", "ethereum", "solana", "dogecoin", "ripple"]

    def fetch(self) -> List[Record]:
        headers = {}
        if config.COINGECKO_API_KEY:
            headers["x-cg-demo-api-key"] = config.COINGECKO_API_KEY
        data = self._get(
            self.URL,
            params={
                "ids": ",".join(self.COINS),
                "vs_currencies": "usd,php",
                "include_24hr_change": "true",
                "include_last_updated_at": "true",
            },
            headers=headers,
        )
        if not isinstance(data, dict):
            raise APIError(f"unexpected CoinGecko payload: {type(data).__name__}")
        now = utc_now_iso()
        out: List[Record] = []
        for coin in self.COINS:
            item = data.get(coin)
            if not item:
                continue
            src_ts = epoch_to_iso(item["last_updated_at"]) if item.get("last_updated_at") else None
            for metric, key, unit in (
                ("price_usd", "usd", "USD"),
                ("price_php", "php", "PHP"),
                ("change_24h_pct", "usd_24h_change", "%"),
            ):
                rec = make_record(self.name, coin, metric, item.get(key), unit, now,
                                  location="global market", source_timestamp=src_ts)
                if rec:
                    out.append(rec)
        return out


# --------------------------------------------------------------------------- #
# 2. International Space Station position -- wheretheiss.at
# --------------------------------------------------------------------------- #
class ISSSource(DataSource):
    """https://wheretheiss.at/w/developer  (no key required, ~1 req/s limit)."""

    name = "iss"
    description = "Live ISS position, altitude and velocity - wheretheiss.at"
    URL = "https://api.wheretheiss.at/v1/satellites/25544"

    def fetch(self) -> List[Record]:
        d = self._get(self.URL, params={"units": "kilometers"})
        if not isinstance(d, dict) or "latitude" not in d:
            raise APIError("unexpected ISS payload")
        now = utc_now_iso()
        src_ts = epoch_to_iso(d["timestamp"]) if d.get("timestamp") else None
        lat, lon = float(d["latitude"]), float(d["longitude"])
        out = []
        for metric, key, unit in (("altitude_km", "altitude", "km"),
                                  ("velocity_kmh", "velocity", "km/h")):
            rec = make_record(self.name, "iss", metric, d.get(key), unit, now,
                              location=d.get("visibility", "orbit"),
                              latitude=lat, longitude=lon, source_timestamp=src_ts)
            if rec:
                out.append(rec)
        return out


# --------------------------------------------------------------------------- #
# 3. Air quality in Philippine cities -- Open-Meteo
# --------------------------------------------------------------------------- #
class AirQualitySource(DataSource):
    """https://open-meteo.com/en/docs/air-quality-api  (no key required)."""

    name = "airquality"
    description = "Air quality (PM2.5, PM10, US AQI ...) for PH cities - Open-Meteo"
    URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
    CITIES = {
        "manila": (14.5995, 120.9842),
        "quezon_city": (14.6760, 121.0437),
        "cebu_city": (10.3157, 123.8854),
        "davao_city": (7.1907, 125.4553),
        "baguio": (16.4023, 120.5960),
    }
    VARS = ["pm2_5", "pm10", "us_aqi", "ozone", "nitrogen_dioxide", "carbon_monoxide"]

    def fetch(self) -> List[Record]:
        names = list(self.CITIES)
        data = self._get(
            self.URL,
            params={
                "latitude": ",".join(str(self.CITIES[n][0]) for n in names),
                "longitude": ",".join(str(self.CITIES[n][1]) for n in names),
                "current": ",".join(self.VARS),
                "timezone": "UTC",
            },
        )
        entries = data if isinstance(data, list) else [data]  # 1 location -> dict
        if len(entries) != len(names):
            raise APIError(f"expected {len(names)} locations, got {len(entries)}")
        now = utc_now_iso()
        out: List[Record] = []
        for city, entry in zip(names, entries):
            cur = entry.get("current") or {}
            units = entry.get("current_units") or {}
            src_ts = (cur.get("time") + ":00Z") if cur.get("time") else None
            for var in self.VARS:
                rec = make_record(self.name, city, var, cur.get(var), units.get(var, ""), now,
                                  location=city.replace("_", " ").title(),
                                  latitude=self.CITIES[city][0], longitude=self.CITIES[city][1],
                                  source_timestamp=src_ts)
                if rec:
                    out.append(rec)
        return out


# --------------------------------------------------------------------------- #
# 4. Earthquakes around the Philippines -- USGS
# --------------------------------------------------------------------------- #
class EarthquakeSource(DataSource):
    """https://earthquake.usgs.gov/fdsnws/event/1/  (no key required).

    Events are sporadic, so each event is emitted once (de-duplicated by id).
    The first poll back-fills the last 24 hours so the dashboard has history.
    """

    name = "earthquake"
    description = "Earthquakes in/around the Philippines (magnitude, depth) - USGS"
    URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    BBOX = {"minlatitude": 4, "maxlatitude": 21.5, "minlongitude": 116, "maxlongitude": 127}

    def __init__(self, getter: Getter = http_get_json):
        super().__init__(getter)
        self._seen: set = set()

    def fetch(self) -> List[Record]:
        start = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
        data = self._get(
            self.URL,
            params={"format": "geojson", "orderby": "time", "limit": 100,
                    "minmagnitude": 2.0, "starttime": start, **self.BBOX},
        )
        if not isinstance(data, dict) or "features" not in data:
            raise APIError("unexpected USGS payload")
        out: List[Record] = []
        for feat in data["features"]:
            eid = feat.get("id")
            props = feat.get("properties") or {}
            coords = (feat.get("geometry") or {}).get("coordinates") or [None, None, None]
            if not eid or eid in self._seen or props.get("mag") is None or not props.get("time"):
                continue
            self._seen.add(eid)
            ts = epoch_to_iso(props["time"] / 1000)
            lon, lat, depth = coords[0], coords[1], coords[2]
            place = props.get("place") or "Philippines region"
            for metric, val, unit in (("magnitude", props["mag"], "Mw"), ("depth_km", depth, "km")):
                rec = make_record(self.name, "usgs_ph", metric, val, unit, ts,
                                  location=place, latitude=lat, longitude=lon, source_timestamp=ts)
                if rec:
                    rec["record_id"] = hashlib.sha1(f"{eid}|{metric}".encode()).hexdigest()[:24]
                    out.append(rec)
        return out


SOURCES = {cls.name: cls for cls in (CryptoSource, ISSSource, AirQualitySource, EarthquakeSource)}


def build_sources(names: str) -> List[DataSource]:
    """'crypto,iss' -> [CryptoSource(), ISSSource()]"""
    built = []
    for name in (n.strip().lower() for n in names.split(",") if n.strip()):
        if name not in SOURCES:
            raise ValueError(f"Unknown data source '{name}'. Choose from: {', '.join(SOURCES)}")
        built.append(SOURCES[name]())
    if not built:
        raise ValueError("No data source selected")
    return built
