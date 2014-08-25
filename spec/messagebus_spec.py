#!/usr/bin/env python
# -*- coding: utf-8 -*-

from messagebus import MessageBus
from expects import *
from threading import Thread
from time import sleep

MSG_TIMEOUT = 0.050

with description('messagebus'):
    with it('can send and receive a message'):
        status = {"received": False }
        def callback(): status["received"] = True
        def subscribe(): MessageBus.subscribe('test.message', callback)
        thread = Thread(target = subscribe)
        thread.daemon = True
        thread.start()

        MessageBus.publish('test.message')

        sleep(MSG_TIMEOUT)
        expect(status["received"]).to(be_true)

    with it('does not receive a message if not subscribed to it'):
        status = {"received": False }
        def callback(): status["received"] = True
        def subscribe(): MessageBus.subscribe('test.some_test_message', callback)
        thread = Thread(target = subscribe)
        thread.daemon = True
        thread.start()

        MessageBus.publish('test.some_other_different_test_message')

        sleep(MSG_TIMEOUT)
        expect(status["received"]).to(be_false)

    with it('transmits the payload along the message'):
        received = {}
        def callback(message): received.update(message)
        def subscribe(): MessageBus.subscribe('test.message_with_payload', callback)
        thread = Thread(target = subscribe)
        thread.daemon = True
        thread.start()

        MessageBus.publish('test.message_with_payload', {'id': 4, 'name': u'John Döe'})

        sleep(MSG_TIMEOUT)
        expect(received).to(have_key('id', 4))
        expect(received).to(have_key('name', u'John Döe'))
