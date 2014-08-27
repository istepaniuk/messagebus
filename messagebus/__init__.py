import pika
import json
import os
import socket
import inspect
try:
    from consumer import Consumer
except ImportError:
    from messagebus.consumer import Consumer


class MessageBus:
    RABBITMQ_DEFAULT_EXCHANGE = 'tcr'

    def __init__(self, broker_url='amqp://localhost'):
        self.broker_url = broker_url
        self.consumer = Consumer(self.broker_url)

    def publish(self, message, payload=''):
        body = ''
        if type(payload) is dict:
            body = json.dumps(payload, ensure_ascii=False)
        if type(payload) is str:
            body = payload
        connection = pika.BlockingConnection(pika.URLParameters(self.broker_url))
        channel = connection.channel()
        channel.basic_publish(exchange=self.RABBITMQ_DEFAULT_EXCHANGE, routing_key=message, body=body)
        connection.close()

    def subscribe(self, message, callback):
        self.consumer.subscribe(message, callback)

    def start(self):
        self.consumer.start()

