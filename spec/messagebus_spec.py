from messagebus import MessageBus
from expects import *
from threading import Thread
from time import sleep

MSG_TIMEOUT = 0.25

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
