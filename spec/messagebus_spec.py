# -*- coding: utf-8 -*-

from messagebus import MessageBus
from expects import *
from threading import Thread, Event
from time import sleep
import uuid

MSG_TIMEOUT = 3

with description('messagebus'):
    with before.each:
        self.bus = MessageBus(queue_prefix = 'testing')
        self.bus2 = MessageBus(queue_prefix = 'testing_publish')

    with it('can send and receive a message'):
        received_event = Event()
        def callback(x): received_event.set()
        self.bus.subscribe('test.message', callback)
        self._start_bus_in_background(self.bus)

        self.bus2.publish('test.message',{})

        has_received = received_event.wait(MSG_TIMEOUT)
        expect(has_received).to(be_true)

    with it('can passes the routing key to the callback'):
        received_event = Event()
        status = {"routing_key": None }
        def callback(message, **kwargs):
            status["routing_key"] = kwargs['routing_key']
            received_event.set()
        self.bus.subscribe('test.message4', callback)
        self._start_bus_in_background(self.bus)

        self.bus2.publish('test.message4')

        received_event.wait(MSG_TIMEOUT)
        expect(status["routing_key"]).to(equal('test.message4'))

    with it('does not receive a message if not subscribed to it'):
        received_event = Event()
        def callback(x): received_event.set()
        self.bus.subscribe('test.some_test_message', callback)
        self._start_bus_in_background(self.bus)

        self.bus2.publish('test.some_other_different_test_message')

        has_received = received_event.wait(0.5)
        expect(has_received).to(be_false)

    with it('transmits the payload along the message'):
        received = {}
        received_event = Event()
        def callback(message):
            received.update(message)
            received_event.set()
        self.bus.subscribe('test.message_with_payload', callback)
        self._start_bus_in_background(self.bus)

        self.bus2.publish('test.message_with_payload', {'id': 4, 'name': u'John Döe'})

        has_received = received_event.wait(MSG_TIMEOUT)
        expect(has_received).to(be_true)
        expect(received).to(have_key('id', 4))
        expect(received).to(have_key('name', u'John Döe'))

    with it('can subscribe to two message types'):
        received1 = {}
        received2 = {}
        received_event1 = Event()
        received_event2 = Event()
        def callback1(message):
            received1.update(message)
            received_event1.set()
        def callback2(message):
            received2.update(message)
            received_event2.set()
        self.bus.subscribe('test.message1', callback1)
        self.bus.subscribe('test.message2', callback2)
        self._start_bus_in_background(self.bus)

        self.bus2.publish('test.message1', {'id': 5})
        self.bus2.publish('test.message2', {'id': 8})

        received_event1.wait(MSG_TIMEOUT)
        received_event2.wait(MSG_TIMEOUT)
        expect(received1).to(have_key('id', 5))
        expect(received2).to(have_key('id', 8))

    with it('retries to process a message twice if an exception is thrown'):
        instance = str(uuid.uuid1())
        received_event = Event()
        received = { "count": 0 }
        def callback(message):
            if instance != message["instance"]:
                return
            received["count"] = received["count"] + 1
            if received["count"] > 1:
                received_event.set()
            raise Exception('test_exception')
        def start():
            while(True):
                try:
                    self.bus = MessageBus()
                    self.bus.subscribe('test.message3', callback)
                    self.bus.start()
                except Exception as e:
                    if e.message != 'test_exception':
                        raise
        thread = Thread(target = start)
        thread.daemon = True
        thread.start()

        MessageBus().publish('test.message3', {"instance": instance})

        received_event.wait(MSG_TIMEOUT)
        expect(received["count"]).to(be(2))

    with it('can subscribe to wildcard a pattern'):
        received1 = {}
        received2 = {}
        received_event1 = Event()
        received_event2 = Event()
        def callback1(message):
            received1.update(message)
            received_event1.set()
        def callback2(message):
            received2.update(message)
            received_event2.set()
        self.bus.subscribe('test1.*', callback1)
        self.bus.subscribe('test2.*', callback2)
        self._start_bus_in_background(self.bus)

        self.bus2.publish('test1.message1', {'id': 15})
        self.bus2.publish('test2.message2', {'id': 28})

        received_event1.wait(MSG_TIMEOUT)
        received_event2.wait(MSG_TIMEOUT)
        expect(received1).to(have_key('id', 15))
        expect(received2).to(have_key('id', 28))

    with it('can publish a message and wait for a response'):
        msgtype = str(uuid.uuid1())
        def echoing_callback(message):
            return message
        self.bus.subscribe_and_publish_response(
            'test.response_requested.'+msgtype, echoing_callback)
        self._start_bus_in_background(self.bus)
        origin_uuid = str(uuid.uuid1())

        received = self.bus2.publish_and_get_response(
            'test.response_requested.'+msgtype, {'proof': origin_uuid })

        expect(received['proof']).to(equal(origin_uuid))

    def _start_bus_in_background(self, bus):
        started = Event()
        bus.consumer.on_connection_setup_finished = lambda: started.set()
        def callback():
            bus.start()
        thread = Thread(target = callback)
        thread.daemon = True
        thread.start()
        started.wait(2)
