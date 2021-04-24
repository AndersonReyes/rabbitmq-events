from rabbitmq_events.rabbitmqclient import RabbitMQEventsClient

import pytest
import uuid


@pytest.fixture
def client():
    with RabbitMQEventsClient("pytest") as client:
        yield client


@pytest.fixture
def local_cache():
    yield set()


def drain_client(clientobj, event_name):
    for method, properties, body in clientobj.channel.consume(
        event_name, inactivity_timeout=0.0001
    ):
        if not all((method, properties, body)):
            break


def test_basic(client):
    event_name = "pytest"
    client.channel.queue_delete(event_name)

    @client.task(event_name)
    def task(body):
        assert body == {"num": 5}

    client.send_event(event_name, {"num": 5})

    drain_client(client, event_name)

    client.channel.cancel()
    client.channel.queue_delete(event_name)


def test_task_with_errors(client):
    event_name = "pytest"
    client.channel.queue_delete(event_name)

    @client.task(event_name)
    def task(body):
        assert body == {"num": 5}

    with pytest.raises(AssertionError, match="{'num': 6} != {'num': 5}"):
        client.send_event(event_name, {"num": 6})

        drain_client(client, event_name)

    client.channel.cancel()
    client.channel.queue_delete(event_name)


def test_multiple_consumers_with_single_client(client, local_cache):
    event_name = "pytest"

    client.default_prefect_count = 1
    client.channel.queue_delete(event_name)

    @client.task(event_name)
    def task(body):
        assert body == {"num": 5}
        local_cache.add("task")

    @client.task(event_name)
    def another_consumer(body):
        global b_called
        assert body == {"num": 5}
        local_cache.add("another_consumer")

    for _ in range(10):
        client.send_event(event_name, {"num": 5})

    drain_client(client, event_name)

    client.channel.cancel()
    client.channel.queue_delete(event_name)

    assert "task" in local_cache
    assert "another_consumer" in local_cache


def test_multiple_consumers_with_multiple_client(local_cache):
    event_name = "pytest"

    clientA = RabbitMQEventsClient("ConsumerA")
    clientB = RabbitMQEventsClient("ConsumerB")

    clientA.channel.queue_delete(event_name)
    clientB.channel.queue_delete(event_name)

    @clientA.task(event_name)
    def consumerA(body):
        assert body == {"num": 5}
        local_cache.add("A")

    @clientB.task(event_name)
    def consumerB(body):
        assert body == {"num": 5}
        local_cache.add("B")

    for _ in range(10):
        clientA.send_event(event_name, {"num": 5})

    drain_client(clientA, event_name)
    drain_client(clientB, event_name)

    clientA.channel.cancel()
    clientA.channel.queue_delete(event_name)

    assert "A" in local_cache
    assert "B" in local_cache
