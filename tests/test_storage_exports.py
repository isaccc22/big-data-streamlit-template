from datetime import datetime, timedelta, timezone
import io

import mongomock
import pandas as pd
import pytest

import exporters
from storage import MongoStore


@pytest.fixture
def store():
    return MongoStore(client=mongomock.MongoClient(tz_aware=True), db="t")


def rows(n=30):
    base = datetime(2026, 10, 20, 3, 0, tzinfo=timezone.utc)
    out = []
    for i in range(n):
        for s in ("bitcoin", "ethereum"):
            ts = (base + timedelta(seconds=15 * i)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            out.append({"record_id": f"{s}{i}", "timestamp": ts, "value": 100 + i, "metric_type": "price_usd",
                        "sensor_id": s, "source": "crypto", "unit": "USD", "location": "x",
                        "latitude": None, "longitude": None})
    return out


def test_upsert_idempotent_and_query(store):
    assert store.upsert_readings(rows()) == 60
    store.upsert_readings(rows())  # replay
    assert store.readings.count_documents({}) == 60
    df = store.query_readings(sensors=["bitcoin"], limit=10)
    assert len(df) == 10 and df["timestamp"].is_monotonic_increasing
    assert str(df["timestamp"].dt.tz) == "UTC"
    assert store.distinct("sensor_id") == ["bitcoin", "ethereum"]
    st = store.stats()
    assert st["total"] == 60 and st["first"] < st["last"]


def test_invalid_rows_skipped(store):
    assert store.upsert_readings([{"record_id": "a", "timestamp": "garbage", "value": 1}]) == 0


def test_aggregates(store):
    store.upsert_aggregates([{"window_start": "2026-10-20T03:00:00Z", "window_end": "2026-10-20T03:01:00Z",
                              "source": "crypto", "sensor_id": "bitcoin", "metric_type": "price_usd",
                              "unit": "USD", "avg_value": 1.0, "min_value": 0.5, "max_value": 2.0,
                              "stddev_value": 0.1, "count": 4}] * 2)
    assert store.aggregates.count_documents({}) == 1
    assert len(store.query_aggregates()) == 1


def test_exports(store):
    store.upsert_readings(rows())
    df = store.query_readings()
    df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Manila")
    assert exporters.csv_bytes(df).startswith(b"timestamp")
    x = exporters.xlsx_bytes(df, "T")
    sheets = pd.ExcelFile(io.BytesIO(x)).sheet_names
    assert sheets == ["Readings", "Summary", "Chart"]
    assert exporters.pdf_bytes(df, "T", ["Range: x"]).startswith(b"%PDF")
    assert exporters.image_bytes(df, "T", "png")[:8] == b"\x89PNG\r\n\x1a\n"
    assert exporters.image_bytes(df, "T", "jpg")[:2] == b"\xff\xd8"
    empty = df.iloc[0:0]
    for fn in (lambda: exporters.xlsx_bytes(empty), lambda: exporters.pdf_bytes(empty), lambda: exporters.image_bytes(empty, "e")):
        assert fn()
