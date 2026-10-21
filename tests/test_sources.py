import pytest

from sources import (AirQualitySource, APIError, CryptoSource, EarthquakeSource, ISSSource,
                     build_sources)

REQUIRED = {"record_id", "timestamp", "value", "metric_type", "sensor_id", "source", "unit"}


def check(records):
    assert records
    for r in records:
        assert REQUIRED <= r.keys()
        assert isinstance(r["value"], float)
        assert r["timestamp"].endswith("Z")


def test_crypto():
    payload = {"bitcoin": {"usd": 68000.5, "php": 3900000, "usd_24h_change": -1.2, "last_updated_at": 1760932800},
               "ethereum": {"usd": 2600, "php": None, "usd_24h_change": 0.5, "last_updated_at": 1760932800}}
    recs = CryptoSource(getter=lambda *a, **k: payload).fetch()
    check(recs)
    assert {r["sensor_id"] for r in recs} == {"bitcoin", "ethereum"}
    assert len(recs) == 5  # ethereum php is None -> skipped


def test_crypto_bad_payload():
    with pytest.raises(APIError):
        CryptoSource(getter=lambda *a, **k: []).fetch()


def test_iss():
    d = {"latitude": 10.5, "longitude": -20.1, "altitude": 420.2, "velocity": 27600.0,
         "visibility": "daylight", "timestamp": 1760932800}
    recs = ISSSource(getter=lambda *a, **k: d).fetch()
    check(recs)
    assert {r["metric_type"] for r in recs} == {"altitude_km", "velocity_kmh"}
    assert recs[0]["latitude"] == 10.5


def test_airquality_multi_location():
    entry = {"current": {"time": "2026-10-20T03:00", "pm2_5": 12.3, "pm10": 20.0, "us_aqi": 50,
                         "ozone": None, "nitrogen_dioxide": 5, "carbon_monoxide": 200},
             "current_units": {"pm2_5": "μg/m³", "us_aqi": "USAQI"}}
    recs = AirQualitySource(getter=lambda *a, **k: [entry] * 5).fetch()
    check(recs)
    assert {r["sensor_id"] for r in recs} == set(AirQualitySource.CITIES)
    assert all(r["metric_type"] != "ozone" for r in recs)  # None skipped


def test_airquality_count_mismatch():
    with pytest.raises(APIError):
        AirQualitySource(getter=lambda *a, **k: {"current": {}}).fetch()


def test_earthquake_dedup():
    feat = {"id": "us1", "properties": {"mag": 4.5, "time": 1760932800000, "place": "10 km E of X"},
            "geometry": {"coordinates": [121.0, 14.5, 35.0]}}
    src = EarthquakeSource(getter=lambda *a, **k: {"features": [feat, {"id": "us2", "properties": {"mag": None, "time": 1}}]})
    first = src.fetch()
    check(first)
    assert {r["metric_type"] for r in first} == {"magnitude", "depth_km"}
    assert src.fetch() == []  # already seen


def test_build_sources():
    assert [s.name for s in build_sources("crypto, iss")] == ["crypto", "iss"]
    with pytest.raises(ValueError):
        build_sources("nope")
