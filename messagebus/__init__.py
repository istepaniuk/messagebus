import pika
import json
import os
import socket
import inspect
from messagebus.consumer import Consumer
import settings

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
        channel.basic_publish(exchange=settings.RABBIT_DEAFULT_EXCHANGE, routing_key=message, body=body)
        connection.close()

    @classmethod
    def subscribe(cls, message, callback):
        Consumer(settings.RABBITMQ_BROKER_URL).subscribe(message, callback)
