# -*- coding: utf-8 -*-

from expects import (
    be_false,
    equal,
    expect,
    have_key,
)
from mamba import before, description, it
from messagebus import MessageBus
from threading import Thread, Event
import uuid
import logging
import sys

MSG_TIMEOUT = 3
SUBSCRIBER_SETUP_GRACE_TIME = 3

logging.disable(sys.maxsize)

with description('MessageBus') as self:
    with before.each:
        self.bus = MessageBus(queue_prefix='testing')

    with it('can publish and subscribe to a message'):
        received_event = Event()
        received = {'routing_key': None}

        def callback(message, **kwargs):
            received['routing_key'] = kwargs['routing_key']
            received.update(message)
            received_event.set()

        self._subscribe_in_the_background('test.message4', callback)

        self.bus.publish('test.message4', {'id': 4, 'name': u'John Döe'})

        received_event.wait(MSG_TIMEOUT)
        expect(received['routing_key']).to(equal('test.message4'))
        expect(received).to(have_key('id', 4))
        expect(received).to(have_key('name', u'John Döe'))

    with it('does not receive a message if not subscribed to it'):
        received_event = Event()
        self._subscribe_in_the_background('test.some_test_message',
                                          lambda: received_event.set())

        self.bus.publish('test.some_other_different_test_message')

        has_received = received_event.wait(0.05)
        expect(has_received).to(be_false)

    with it('retries to process a message twice if an exception is thrown'):
        instance = str(uuid.uuid1())
        received = {'count': 0}
        received_event = Event()

        def callback(message):
            received['count'] += 1
            if received['count'] >= 2:
                received_event.set()
            raise Exception('test!')

        self._subscribe_in_the_background("test.message3.%s" % instance,
                                          callback)

        self.bus.publish("test.message3.%s" % instance, {})

        has_received = received_event.wait(MSG_TIMEOUT)
        expect(received['count']).to(equal(2))

    with it('can subscribe to wildcard a pattern'):
        received = {}
        received_event = Event()

        def callback(message):
            received.update(message)
            if len(received) > 1:
                received_event.set()

        self._subscribe_in_the_background('test_wildcards.*', callback)

        self.bus.publish('test_wildcards.abc', {'abc': 15})
        self.bus.publish('test_wildcards.xyz', {'xyz': 28})

        received_event.wait(MSG_TIMEOUT)
        expect(received).to(have_key('abc'))
        expect(received).to(have_key('xyz'))

    with it('can publish a message and wait for a response'):
        instance = str(uuid.uuid1())
        message_type = "test.response_requested.%s" % instance

        def echoing_callback(message):
            return message

        self._subscribe_in_the_background(message_type, echoing_callback,
                                          responds=True)

        bus = MessageBus(queue_prefix='testing2')
        received = bus.publish_and_get_response(message_type,
                                                {'proof': instance})

        expect(received['proof']).to(equal(instance))

    def _subscribe_in_the_background(self, message, callback, responds=False):
        """Creates a new MessageBus in a daemon thread and sets up a
        subscription. This function will only return once the subscription is
        ready to process messages so it's safe to publish immediately
        afterwards.
        """
        started = Event()
        queue_prefix = "testing-%s" % str(uuid.uuid1())

        def start_bus():
            while True:
                bus = MessageBus(queue_prefix=queue_prefix)
                bus.consumer.on_connection_setup_finished = \
                    lambda: started.set()
                if responds:
                    bus.subscribe_and_publish_response(message, callback)
                else:
                    bus.subscribe(message, callback)

                try:
                    bus.start()
                except Exception as e:
                    if str(e) != 'test!':
                        raise

        thread = Thread(target=start_bus)
        thread.daemon = True
        thread.start()

        started_ok = started.wait(SUBSCRIBER_SETUP_GRACE_TIME)

        if not started_ok:
            raise Exception('Consumer took too long to set up')
