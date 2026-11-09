import pytest
from mqtt_broker import InMemoryMQTTBroker


class FakeWebSocket:
    def __init__(self):
        self.sent = []

    async def send_json(self, data):
        self.sent.append(data)


def test_publish_without_subscribers_returns_zero():
    broker = InMemoryMQTTBroker()
    assert broker.get_stats()["total_messages_published"] == 0


async def test_publish_broadcasts_to_subscribers():
    broker = InMemoryMQTTBroker()
    sub = FakeWebSocket()
    broker.subscribe("escuela/temperatura", sub)

    receivers = await broker.publish("escuela/temperatura", "25.4")

    assert receivers == 1
    assert broker.total_published_count == 1
    assert sub.sent[0]["type"] == "mqtt_message"
    assert sub.sent[0]["payload"] == "25.4"


async def test_publish_no_subscriber_topic_returns_zero():
    broker = InMemoryMQTTBroker()
    receivers = await broker.publish("escuela/sin_suscriptores", "x")
    assert receivers == 0
    assert broker.total_published_count == 1


def test_unsubscribe_from_topic_cleans_empty_topic():
    broker = InMemoryMQTTBroker()
    sub = FakeWebSocket()
    broker.subscribe("escuela/humedad", sub)

    broker.unsubscribe(sub, "escuela/humedad")

    assert broker.subscriptions == {}


def test_unsubscribe_everywhere_removes_from_all_topics():
    broker = InMemoryMQTTBroker()
    sub = FakeWebSocket()
    other = FakeWebSocket()
    broker.subscribe("escuela/humedad", sub)
    broker.subscribe("escuela/luces", sub)
    broker.subscribe("escuela/luces", other)

    broker.unsubscribe(sub)

    assert broker.subscriptions == {"escuela/luces": {other}}
    assert broker.get_stats()["total_topics"] == 1
