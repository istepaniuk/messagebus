import pika
import json
import os
import socket
import inspect
import datetime
from threading import Thread, Event
import time
import uuid

try:
    from consumer import Consumer
except ImportError:
    from messagebus.consumer import Consumer

class MessageBus:
    RABBITMQ_DEFAULT_EXCHANGE = 'the_exchange_name'

    def __init__(self, broker_url='amqp://localhost', queue_prefix=None):
        self.broker_url = broker_url
        self.consumer = Consumer(self.broker_url, queue_prefix)
        self._queue_prefix = queue_prefix

    def publish(self, message, payload={}):
        self._publish(message, payload)

    def _publish(self, message, payload, correlation_id=None):
        body = json.dumps(self._prepare_payload(payload), ensure_ascii=False)
        connection = pika.BlockingConnection(pika.URLParameters(self.broker_url))
        channel = connection.channel()

        properties = None
        if correlation_id:
            properties = pika.BasicProperties(correlation_id=correlation_id)

        channel.basic_publish(
            exchange=self.RABBITMQ_DEFAULT_EXCHANGE,
            routing_key=message,
            body=body,
            properties=properties)
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

    def subscribe_and_publish_response(self, message, callback):
        def subscribe_callback(request_payload, **kwargs):
            correlation_id = kwargs['properties'].correlation_id
            response = callback(request_payload)
            self._publish(message + '.answered', response, correlation_id)
        self.consumer.subscribe(message, subscribe_callback, transient_queue=True)

    def publish_and_get_response(self, message, payload, timeout_secs=5):
        sent_correlation = str(uuid.uuid1())
        consumer_ready = Event()
        def on_consumer_ready():
            consumer_ready.set()

        consumer = Consumer(self.broker_url, self._queue_prefix)
        consumer.on_connection_setup_finished = on_consumer_ready
        response = {}
        response_received = Event()

        def response_callback(response_payload, **kwargs):
            if not sent_correlation == kwargs['properties'].correlation_id:
                return
            response['payload'] = response_payload
            response_received.set()

        def wait_for_response():
            consumer.subscribe(message + '.answered', response_callback, transient_queue=True)
            consumer.start()

        thread = Thread(target = wait_for_response)
        thread.daemon = True
        thread.start()

        consumer_ready.wait(2)
        self._publish(message, payload, correlation_id=sent_correlation)
        timed_out = not response_received.wait(timeout_secs)
        if timed_out:
            raise MessageBusTimeoutError()
        consumer.stop()
        return response.get('payload')

    def start(self):
        self.consumer.start()


class MessageBusTimeoutError(Exception):
    pass
