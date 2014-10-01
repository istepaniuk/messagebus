#!/usr/bin/env python
# -*- coding: utf-8 -*-

from messagebus import MessageBus
from expects import *
from threading import Thread
from time import sleep
import uuid

MSG_TIMEOUT = 0.040

with description('messagebus'):
    with before.each:
        self.bus = MessageBus()

    with it('can send and receive a message'):
        status = {"received": False }
        def callback(x): status["received"] = True
        self.bus.subscribe('test.message', callback)
        def start(): self.bus.start()
        thread = Thread(target = start)
        thread.daemon = True
        thread.start()

        self.bus.publish('test.message')

        sleep(MSG_TIMEOUT)
        expect(status["received"]).to(be_true)

    with it('does not receive a message if not subscribed to it'):
        status = {"received": False }
        def callback(x): status["received"] = True
        self.bus.subscribe('test.some_test_message', callback)
        def start(): self.bus.start()
        thread = Thread(target = start)
        thread.daemon = True
        thread.start()

        self.bus.publish('test.some_other_different_test_message')

        sleep(MSG_TIMEOUT)
        expect(status["received"]).to(be_false)

    with it('transmits the payload along the message'):
        received = {}
        def callback(message): received.update(message)
        self.bus.subscribe('test.message_with_payload', callback)
        def start(): self.bus.start()
        thread = Thread(target = start)
        thread.daemon = True
        thread.start()

        self.bus.publish('test.message_with_payload', {'id': 4, 'name': u'John Döe'})

        sleep(MSG_TIMEOUT)
        expect(received).to(have_key('id', 4))
        expect(received).to(have_key('name', u'John Döe'))

    with it('can subscribe to two message types'):
        received1 = {}
        received2 = {}
        def callback1(message): received1.update(message)
        def callback2(message): received2.update(message)
        self.bus.subscribe('test.message1', callback1)
        self.bus.subscribe('test.message2', callback2)
        def start(): self.bus.start()
        thread = Thread(target = start)
        thread.daemon = True
        thread.start()

        self.bus.publish('test.message1', {'id': 5})
        self.bus.publish('test.message2', {'id': 8})

        sleep(MSG_TIMEOUT)
        expect(received1).to(have_key('id', 5))
        expect(received2).to(have_key('id', 8))

    with it('retries to process a message if an exception is thrown'):
        instance = str(uuid.uuid1())
        received = {"count": 0}
        def callback(message):
            if instance != message["instance"]:
                return
            tried = received["count"] = received["count"] + 1
            if tried == 1:
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

        sleep(MSG_TIMEOUT)
        expect(received["count"]).to(be(2))
