"""Renders the dashboard headlessly with no Kafka/MongoDB running (demo-data fallback)."""
import os

import pytest

pytest.importorskip("streamlit")
import streamlit as st
from streamlit.testing.v1 import AppTest


@pytest.fixture(autouse=True)
def _clear_streamlit_caches():
    st.cache_data.clear()
    st.cache_resource.clear()


def test_app_renders_in_demo_mode(monkeypatch):
    monkeypatch.setenv("MONGO_URI", "mongodb://127.0.0.1:1")      # nothing listens here
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "127.0.0.1:1")
    at = AppTest.from_file(os.path.join(os.path.dirname(__file__), "..", "app.py"), default_timeout=90).run()
    assert not at.exception, at.exception
    assert [t.label for t in at.tabs] == ["🔴 Live", "🗂 History", "📐 Aggregation", "ℹ️ About"]


def test_app_with_mongo_data(monkeypatch):
    """History / Aggregation / Live-fallback render from MongoDB-stored data."""
    from datetime import datetime, timedelta, timezone

    import mongomock

    import storage

    client = mongomock.MongoClient(tz_aware=True)
    monkeypatch.setattr(storage, "MongoClient", lambda *a, **k: client)
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "127.0.0.1:1")
    store = storage.MongoStore()
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(60):
        ts = (now - timedelta(seconds=15 * (60 - i))).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        for s, base in (("bitcoin", 68000), ("ethereum", 2600)):
            rows.append({"record_id": f"{s}{i}", "timestamp": ts, "value": base + i, "metric_type": "price_usd",
                         "sensor_id": s, "source": "crypto", "unit": "USD", "location": "m"})
    store.upsert_readings(rows)
    store.upsert_aggregates([{"window_start": (now - timedelta(minutes=m)).strftime("%Y-%m-%dT%H:%M:00Z"),
                              "window_end": now.strftime("%Y-%m-%dT%H:%M:00Z"), "source": "crypto",
                              "sensor_id": "bitcoin", "metric_type": "price_usd", "unit": "USD",
                              "avg_value": 68000.0 + m, "min_value": 67990.0, "max_value": 68010.0,
                              "stddev_value": 3.0, "count": 4} for m in range(10)])

    at = AppTest.from_file(os.path.join(os.path.dirname(__file__), "..", "app.py"), default_timeout=90).run()
    assert not at.exception, at.exception
    text = " ".join(m.value for m in at.markdown) + " ".join(i.value for i in at.info)
    assert "MongoDB fallback" in text
    assert any(m.label == "Records (filtered)" and m.value == "120" for m in at.metric)
    # Spark-window view
    at.radio[0].set_value("⚡ Spark windows (from MongoDB)").run()
    assert not at.exception, at.exception
    assert any("Computed continuously by Spark" in c.value for c in at.caption)
