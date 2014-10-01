import pika
import json
import os
import socket

class Consumer:
    def __init__(self, broker_url):
        self.exchange = 'tcr'
        self.broker_url = broker_url
        self.subscriptions = {}
        self.patterns = {}
        self.bound_count = 0

    def subscribe(self, message, callback):
        self.subscriptions[message] = callback
        queue_name = self._get_queue_name(message)
        self.patterns[queue_name] = message

    def start(self):
        if len(self.subscriptions) == 0:
            pass
        params = pika.URLParameters(self.broker_url)
        self.connection = pika.SelectConnection(params, self._on_connected)
        self.connection.add_on_close_callback(self._on_connection_closed)
        self.connection.ioloop.start()

    def _on_connection_closed(self, a, b, c):
        raise Exception("Connection lost")

    def _on_channel_closed(self, channel, reply_code, reply_text):
        raise IOError("Channel closed (%s) %s" % (reply_code, reply_text))

    def _on_connected(self, connection):
        connection.channel(self._on_channel_opened)

    def _on_channel_opened(self, new_channel):
        self.channel = new_channel
        self.channel.basic_qos(prefetch_size=0, prefetch_count=0)
        self.channel.add_on_close_callback(self._on_channel_closed)
        self.channel.exchange_declare(self._on_exchange_declared, self.exchange, 'topic', durable=True)

    def _on_exchange_declared(self, unused_frame):
        for pattern in self.subscriptions.keys():
            queue_name = self._get_queue_name(pattern)
            self.channel.queue_declare(queue=queue_name, durable=True,
                exclusive=False, auto_delete=False, callback=self._on_queue_declared)

    def _get_queue_name(self, subscription_pattern):
        #TODO: queues should be exclusive once dead lettering is in place
        return "%s-%s" % (socket.gethostname(), subscription_pattern)

    def _on_queue_declared(self, frame):
        queue_name = frame.method.queue
        self.channel.queue_bind(self._on_bind_ok, queue_name,
            self.exchange, self.patterns[queue_name])

    def _on_bind_ok(self, frame):
        self.bound_count = self.bound_count + 1
        if self.bound_count == len(self.subscriptions):
            for queue_name in self.patterns.keys():
                self.channel.basic_consume(self._handle_delivery,
                    queue=queue_name, no_ack=False)

    def _handle_delivery(self, channel, method, header, body):
        routing_key = method.routing_key
        callback = self.subscriptions[routing_key]
        try:
            payload = json.loads(body, encoding="utf8")
        except ValueError:
            payload = body
        try:
            callback(payload)
            channel.basic_ack(method.delivery_tag)
        except Exception as e:
            if method.redelivered:
                channel.basic_nack(method.delivery_tag, requeue=False)
                print "Warning: an error ocurred while processing the message for a second time."
            else:
                channel.basic_nack(delivery_tag=method.delivery_tag,requeue=True)
            self.connection.close()
            raise
