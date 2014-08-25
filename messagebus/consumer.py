import pika
import json
import os
import socket
import inspect

class Consumer:
        EXCHANGE = 'tcr'

        def __init__(self, host):
            self.host = host

        def subscribe(self, message, callback):
            self.callback = callback
            self._subscription_pattern = message
            self._queue_name = self._get_queue_name()
            params = pika.ConnectionParameters(host=self.host)
            connection = pika.SelectConnection(params, self._on_connected)
            connection.ioloop.start()

        def _get_queue_name(self):
            #TODO: queues should be exclusive once dead lettering is in place
            return "%s-%s" % (socket.gethostname(), self._subscription_pattern)

        def _on_connected(self, connection):
            connection.channel(self._on_channel_opened)

        def _on_channel_opened(self, new_channel):
            self.channel = new_channel
            self.channel.exchange_declare(self._on_exchange_declared, self.EXCHANGE, 'topic')

        def _on_exchange_declared(self, unused_frame):
            self.channel.queue_declare(queue=self._queue_name, durable=True,
                exclusive=False, auto_delete=False, callback=self._on_queue_declared)

        def _on_queue_declared(self, frame):
            self.channel.queue_bind(self._on_bind_ok, self._queue_name,
                self.EXCHANGE, self._subscription_pattern)

        def _on_bind_ok(self, unused_frame):
            self.channel.basic_consume(self._handle_delivery,
                queue=self._queue_name, no_ack=True)

        def _handle_delivery(self, channel, method, header, body):
            if len(inspect.getargspec(self.callback).args) == 0:
                self.callback()
                return
            try:
                payload = json.loads(body)
            except ValueError:
                payload = body
            self.callback(payload)
