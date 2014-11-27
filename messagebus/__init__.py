import pika
import json
import os
import socket
import inspect
import datetime
try:
    from consumer import Consumer
except ImportError:
    from messagebus.consumer import Consumer


class MessageBus:
    RABBITMQ_DEFAULT_EXCHANGE = 'tcr'

    def __init__(self, broker_url='amqp://localhost', queue_prefix=None):
        self.broker_url = broker_url
        self.consumer = Consumer(self.broker_url, queue_prefix)

    def publish(self, message, payload={}):
        body = json.dumps(self._prepare_payload(payload), ensure_ascii=False)
        connection = pika.BlockingConnection(pika.URLParameters(self.broker_url))
        channel = connection.channel()
        channel.basic_publish(exchange=self.RABBITMQ_DEFAULT_EXCHANGE, routing_key=message, body=body)
        connection.close()

    def _prepare_payload(self, payload):
        def serialize(value):
            if isinstance(value, datetime.datetime):
                return value.isoformat()
            return value
        proc_payload = { k: serialize(v) for k, v in payload.items() }
        if not proc_payload.has_key('timestamp'):
            proc_payload['timestamp'] = datetime.datetime.utcnow().isoformat()
        return proc_payload

    def subscribe(self, message, callback):
        self.consumer.subscribe(message, callback)

    def start(self):
        self.consumer.start()

