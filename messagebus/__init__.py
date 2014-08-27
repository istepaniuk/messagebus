from django.conf import settings

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
    @classmethod
    def publish(cls, message, payload=""):
        body = ''
        if type(payload) is dict:
            body = json.dumps(payload, ensure_ascii=False)
        if type(payload) is str:
            body = payload
        connection = pika.BlockingConnection(pika.URLParameters(settings.RABBITMQ_BROKER_URL))
        channel = connection.channel()
        channel.basic_publish(exchange=settings.RABBITMQ_DEFAULT_EXCHANGE, routing_key=message, body=body)
        connection.close()

    @classmethod
    def subscribe(cls, message, callback):
        Consumer(settings.RABBITMQ_BROKER_URL).subscribe(message, callback)
