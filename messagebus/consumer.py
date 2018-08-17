import pika
import json
import os
import socket
import inspect
import uuid
import logging

class Consumer:
    def __init__(self, broker_url, queue_prefix=None, exchange=''):
        self._logger = logging.getLogger(__name__)
        self.queue_prefix = queue_prefix
        self.exchange = exchange
        self.broker_url = broker_url
        self._subscriptions = []
        self._bound_count = 0
        self._closing = False
        self.on_connection_setup_finished = lambda: None
        self.dead_letter_exchange = "%s-dlx" % exchange

    def subscribe(self, pattern, callback, transient_queue=False):
        queue_name = self._get_queue_name(pattern)
        if transient_queue and queue_name.endswith('.answered'):
            queue_name += str(uuid.uuid1())
        self._subscriptions.append({
            "callback": callback,
            "queue_name": queue_name,
            "pattern": pattern,
            "transient_queue": transient_queue,
        })

    def start(self):
        if len(self._subscriptions) == 0:
            pass
        params = pika.URLParameters(self.broker_url)

        self.connection = pika.SelectConnection(params, self._on_connected)
        self._logger.debug('Connection created')
        self.connection.add_on_close_callback(self._on_connection_closed)
        try:
            self._logger.debug('IO Loop starting')
            self.connection.ioloop.start()
        except KeyboardInterrupt:
            self._logger.info("KeyboardInterrupt. Adios!")

    def stop(self):
        self._closing = True
        self._logger.debug('Consumer stopping')
        self.connection.close()

    def _on_connection_closed(self, a, b, c):
        self._logger.debug('Connection closed')
        if not self._closing:
            raise Exception("Connection lost")

    def _on_channel_closed(self, channel, reply_code, reply_text):
        self._logger.debug('Channel closed')
        if not self._closing:
            raise IOError("Channel closed (%s) %s" % (reply_code, reply_text))
        self.connection.close()

    def _on_connected(self, connection):
        connection.channel(self._on_channel_opened)

    def _on_channel_opened(self, new_channel):
        self.channel = new_channel
        new_channel.basic_qos(prefetch_size=0, prefetch_count=1)
        new_channel.add_on_close_callback(self._on_channel_closed)
        new_channel.exchange_declare(self._on_dlx_declared, self.dead_letter_exchange, 'fanout', durable=True)
        new_channel.exchange_declare(self._on_exchange_declared, self.exchange, 'topic', durable=True)

    def _on_dlx_declared(self, unused_frame):
        self.channel.queue_declare(queue='dead-letter', durable=True,
            exclusive = False, auto_delete = False, callback=self._on_dlq_declared)

    def _on_dlq_declared(self, frame):
        self.channel.queue_bind(lambda x: x, queue = 'dead-letter',
            exchange = self.dead_letter_exchange  ,routing_key = '')

    def _on_exchange_declared(self, unused_frame):
        for subscription in self._subscriptions:
            arguments = { 'x-dead-letter-exchange' : self.dead_letter_exchange }
            self.channel.queue_declare(queue=subscription["queue_name"],
                durable=False if subscription['transient_queue'] else True,
                arguments = arguments,
                exclusive=False,
                auto_delete=True if subscription['transient_queue'] else False,
                callback=self._on_queue_declared)

    def _get_queue_name(self, subscription_pattern):
        if self.queue_prefix is None:
            self.queue_prefix = socket.gethostname()
        return "%s-%s" % (self.queue_prefix, subscription_pattern)

    def _on_queue_declared(self, frame):
        queue_name = frame.method.queue
        subscription = self._get_subscription_by_queue_name(queue_name)
        self.channel.queue_bind(self._on_bind_ok, queue_name,
            self.exchange, subscription['pattern'])

    def _get_subscription_by_queue_name(self, queue_name):
        for subscription in self._subscriptions:
            if subscription['queue_name'] == queue_name:
                return subscription
        return None

    def _on_bind_ok(self, frame):
        self._bound_count = self._bound_count + 1
        if self._bound_count == len(self._subscriptions):
            for subscription in self._subscriptions:
                self.channel.basic_consume(self._get_handle_delivery_callback(subscription),
                    queue = subscription['queue_name'],
                    no_ack= True if subscription['transient_queue'] else False)
            self.on_connection_setup_finished()

    def _get_handle_delivery_callback(self, subscription):
        def handle_delivery(channel, method, properties, body):
            try:
                payload = json.loads(body.decode('utf-8'), encoding='utf-8')
                self._invoke_callback(subscription['callback'],
                                      payload,
                                      method.routing_key,
                                      properties)

                if not subscription['transient_queue']:
                    channel.basic_ack(method.delivery_tag)
            except Exception:
                should_requeue = not method.redelivered
                if not subscription['transient_queue']:
                    channel.basic_nack(delivery_tag = method.delivery_tag, requeue = should_requeue)
                    channel.close()
                if not should_requeue:
                    self._logger.exception(
                        "Unhandled exception in message subscription for '%s' that will not be requeued",
                        method.routing_key,
                        extra=dict(subscription=subscription, body=body))
                raise
        return handle_delivery

    def _invoke_callback(self, callback, payload, routing_key, properties):
        if str(type(callback)) == "<type 'classobj'>":
            callback(payload)
            return
        callback_spec = inspect.getargspec(callback)

        if callback_spec.keywords is not None:
            kwargs = dict(
                routing_key=routing_key,
                properties=properties
            )
            callback(payload, **kwargs)
        else:
            callback(payload)
