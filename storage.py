"""MongoDB storage layer shared by the Spark job and the Streamlit dashboard."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from pymongo import ASCENDING, DESCENDING, MongoClient, ReplaceOne
from pymongo.errors import PyMongoError

import config

log = logging.getLogger(__name__)

READING_COLUMNS = ["timestamp", "value", "metric_type", "sensor_id", "source",
                   "unit", "location", "latitude", "longitude"]
AGG_COLUMNS = ["window_start", "window_end", "source", "sensor_id", "metric_type", "unit",
               "avg_value", "min_value", "max_value", "stddev_value", "count"]


def parse_ts(value: Any) -> Optional[datetime]:
    """ISO-8601 string (with or without 'Z') or datetime -> aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class MongoStore:
    def __init__(self, uri: str = config.MONGO_URI, db: str = config.MONGO_DB,
                 readings: str = config.MONGO_READINGS, aggregates: str = config.MONGO_AGGREGATES,
                 client: Optional[MongoClient] = None):
        self.client = client or MongoClient(uri, tz_aware=True, serverSelectionTimeoutMS=2500)
        self.db = self.client[db]
        self.readings = self.db[readings]
        self.aggregates = self.db[aggregates]

    # -- health -------------------------------------------------------------
    def ping(self) -> bool:
        try:
            self.client.admin.command("ping")
            return True
        except PyMongoError:
            return False

    def ensure_indexes(self) -> None:
        self.readings.create_index([("timestamp", DESCENDING)])
        self.readings.create_index([("metric_type", ASCENDING), ("sensor_id", ASCENDING),
                                    ("timestamp", DESCENDING)])
        self.aggregates.create_index([("window_start", DESCENDING)])

    # -- writes (used by Spark job) -----------------------------------------
    def upsert_readings(self, rows: Iterable[Dict[str, Any]]) -> int:
        ops = []
        for r in rows:
            ts = parse_ts(r.get("timestamp"))
            rid = r.get("record_id")
            if ts is None or not rid or r.get("value") is None:
                continue
            doc = {k: r.get(k) for k in READING_COLUMNS if k != "timestamp"}
            doc["timestamp"] = ts
            doc["ingested_at"] = datetime.now(timezone.utc)
            ops.append(ReplaceOne({"_id": rid}, doc, upsert=True))
        if ops:
            self.readings.bulk_write(ops, ordered=False)
        return len(ops)

    def upsert_aggregates(self, rows: Iterable[Dict[str, Any]]) -> int:
        ops = []
        for r in rows:
            ws, we = parse_ts(r.get("window_start")), parse_ts(r.get("window_end"))
            if ws is None:
                continue
            doc = {k: r.get(k) for k in AGG_COLUMNS if k not in ("window_start", "window_end")}
            doc.update(window_start=ws, window_end=we)
            _id = f"{r.get('source')}|{r.get('sensor_id')}|{r.get('metric_type')}|{ws.isoformat()}"
            ops.append(ReplaceOne({"_id": _id}, doc, upsert=True))
        if ops:
            self.aggregates.bulk_write(ops, ordered=False)
        return len(ops)

    # -- reads (used by dashboard) ------------------------------------------
    @staticmethod
    def _filter(start, end, metrics, sensors, sources, time_field="timestamp") -> Dict[str, Any]:
        q: Dict[str, Any] = {}
        rng = {}
        if start is not None:
            rng["$gte"] = start
        if end is not None:
            rng["$lte"] = end
        if rng:
            q[time_field] = rng
        if metrics:
            q["metric_type"] = {"$in": list(metrics)}
        if sensors:
            q["sensor_id"] = {"$in": list(sensors)}
        if sources:
            q["source"] = {"$in": list(sources)}
        return q

    def query_readings(self, start: Optional[datetime] = None, end: Optional[datetime] = None,
                       metrics: Optional[List[str]] = None, sensors: Optional[List[str]] = None,
                       sources: Optional[List[str]] = None, limit: int = 100_000) -> pd.DataFrame:
        """Newest `limit` matching readings, returned oldest -> newest."""
        q = self._filter(start, end, metrics, sensors, sources)
        cur = (self.readings.find(q, {c: 1 for c in READING_COLUMNS} | {"_id": 0})
               .sort("timestamp", DESCENDING).limit(int(limit)))
        df = pd.DataFrame(list(cur), columns=READING_COLUMNS)
        if df.empty:
            return df
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df.sort_values("timestamp").reset_index(drop=True)

    def query_aggregates(self, start=None, end=None, metrics=None, sensors=None, sources=None,
                         limit: int = 50_000) -> pd.DataFrame:
        q = self._filter(start, end, metrics, sensors, sources, time_field="window_start")
        cur = (self.aggregates.find(q, {c: 1 for c in AGG_COLUMNS} | {"_id": 0})
               .sort("window_start", DESCENDING).limit(int(limit)))
        df = pd.DataFrame(list(cur), columns=AGG_COLUMNS)
        if df.empty:
            return df
        for c in ("window_start", "window_end"):
            df[c] = pd.to_datetime(df[c], utc=True)
        return df.sort_values("window_start").reset_index(drop=True)

    def distinct(self, field: str) -> List[str]:
        try:
            return sorted(v for v in self.readings.distinct(field) if v)
        except PyMongoError:
            return []

    def stats(self) -> Dict[str, Any]:
        """Total document count and time span of the raw collection."""
        total = self.readings.estimated_document_count()
        first = next(iter(self.readings.find({}, {"timestamp": 1}).sort("timestamp", ASCENDING).limit(1)), None)
        last = next(iter(self.readings.find({}, {"timestamp": 1}).sort("timestamp", DESCENDING).limit(1)), None)
        return {"total": total,
                "first": first["timestamp"] if first else None,
                "last": last["timestamp"] if last else None}
