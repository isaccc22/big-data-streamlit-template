import json

import producer as prod
from sources import APIError, DataSource, make_record


class FakeFuture:
    def get(self, timeout=None):
        return True


class FakeKafka:
    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, json.loads(value)))
        return FakeFuture()

    def flush(self, timeout=None):
        pass

    def close(self):
        pass


class Good(DataSource):
    name = "good"

    def fetch(self):
        return [make_record("good", "s1", "m", 1.5, "u", "2026-10-20T03:00:00.000Z")]


class Bad(DataSource):
    name = "bad"

    def fetch(self):
        raise APIError("boom")


def test_collect_survives_failing_api_and_send_keys_by_sensor():
    p = prod.StreamingDataProducer.__new__(prod.StreamingDataProducer)
    p.topic, p.sources, p.producer = "t", [Bad(), Good()], FakeKafka()
    recs = p.collect()
    assert len(recs) == 1
    assert p.send_records(recs) == 1
    topic, key, body = p.producer.sent[0]
    assert (topic, key) == ("t", b"s1")
    assert {"timestamp", "value", "metric_type", "sensor_id"} <= body.keys()
