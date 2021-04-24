import os
import json
import logging

import pika
from pika.exchange_type import ExchangeType

logging.basicConfig(level=logging.INFO)
HOST = os.getenv("RABBITMQ_HOST", "localhost")


class RabbitMQEventsClient:
    def __init__(self, consumer, default_prefect_count=1):
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(HOST))
        self.channel = self.conn.channel()
        self.consumer = consumer
        self.default_prefect_count = default_prefect_count

    def __enter__(self):
        if self.conn is None:
            self.conn = pika.BlockingConnection(pika.ConnectionParameters(HOST))

        if self.channel is None:
            self.channel = self.conn.channel()

        return self

    def __exit__(self, type, value, traceback):
        self.conn.close()
        self.conn = None
        self.channel = None

    def make_tag(self, event, handler):
        return "{}.{}.{}".format(event, self.consumer, handler.__name__,)

    def task(self, event_name, prefetch_count=None):
        prefetch_count = prefetch_count or self.default_prefect_count
        self.channel.exchange_declare(
            exchange=event_name, exchange_type=ExchangeType.direct
        )
        self.channel.queue_declare(queue=event_name, durable=True)
        self.channel.queue_bind(exchange=event_name, queue=event_name)

        def wrapper(func):
            tag = self.make_tag(event_name, func)

            def callback(ch, method, properties, body):

                logger = logging.getLogger(tag)

                try:
                    logger.info("event received")
                    func(body=json.loads(body))
                    logger.info("event processed")
                except Exception:
                    logger.exception("error handling event")
                    raise
                finally:
                    self.channel.basic_ack(delivery_tag=method.delivery_tag)

            self.channel.basic_qos(prefetch_count=prefetch_count)
            self.channel.basic_consume(
                queue=event_name, on_message_callback=callback, consumer_tag=tag
            )

            return callback

        return wrapper

    def start(self):
        self.channel.start_consuming()

    def send_event(self, event, body, **kwargs):
        self.channel.basic_publish(
            exchange=event,
            routing_key=event,
            body=json.dumps(body),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
            **kwargs
        )
