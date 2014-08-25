import pika
import json
import os
import socket
import inspect
from messagebus.consumer import Consumer

class MessageBus:
    DEFAULT_HOST = 'localhost'

    @classmethod
    def publish(cls, message, payload=""):
        body = ''
        if type(payload) is dict:
            body = json.dumps(payload)
        if type(payload) is str:
            body = payload
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=cls.DEFAULT_HOST))
        channel = connection.channel()
        channel.basic_publish(exchange='tcr',routing_key=message, body=body)
        connection.close()

    @classmethod
    def subscribe(cls, message, callback):
        Consumer(cls.DEFAULT_HOST).subscribe(message, callback)
