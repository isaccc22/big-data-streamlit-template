import json

import pytest

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.master("local[1]").appName("t")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def msg(i, ts, value, sensor="bitcoin", **kw):
    d = {"record_id": f"r{i}", "timestamp": ts, "value": value, "metric_type": "price_usd",
         "sensor_id": sensor, "source": "crypto", "unit": "USD"}
    d.update(kw)
    return (json.dumps(d),)


def test_parse_and_aggregate(spark):
    from spark_job import aggregate_windows, parse_messages

    data = [msg(1, "2026-10-20T03:00:05.100Z", 10.0), msg(2, "2026-10-20T03:00:20.000Z", 20.0),
            msg(3, "2026-10-20T03:00:50.000Z", 30.0), msg(4, "2026-10-20T03:01:05.000Z", 40.0),
            ("not json",), msg(5, "bad-ts", 1.0), msg(6, "2026-10-20T03:00:10Z", None)]
    raw = spark.createDataFrame(data, ["value"])
    parsed = parse_messages(raw)
    assert parsed.count() == 4  # invalid json / bad ts / null value filtered out

    rows = {r["window_start"]: r for r in aggregate_windows(parsed, "1 minute", "2 minutes").collect()}
    w1 = rows["2026-10-20T03:00:00Z"]
    assert w1["count"] == 3 and w1["avg_value"] == 20.0 and w1["min_value"] == 10.0 and w1["max_value"] == 30.0
    assert rows["2026-10-20T03:01:00Z"]["count"] == 1
