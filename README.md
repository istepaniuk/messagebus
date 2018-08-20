messagebus
==========
Wrapper arround Pika to publish and subscribe to messages on RabbitMQ

## Build status
[![Build status](https://api.travis-ci.org/istepaniuk/messagebus.svg?branch=master)](https://www.travis-ci.org/istepaniuk/messagebus)

## Why?
Pika is a good AMQP library, but it exposes AMQP for any purpose.
This library uses AMQP to implement this very simple interface, based on many assumptions about the way we use AMQP to implement the [Message Bus pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageBus.html), [Publish-Subscribe Channel](https://www.enterpriseintegrationpatterns.com/patterns/messaging/PublishSubscribeChannel.html) and [Competing Consumers](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CompetingConsumers.html)

### How does it work
* Publishers and subscribers are bound through one single exchange.
* Queues are named after the message type by default.
* Queues are persistent, subscribers will use the same ones on restart.
* Two subscriber instances will bind to the same queue if no queue prefix is specified (load sharing).
* When your handler returns, messages are ACKed, if it throws an error, messages are rejected and requeued once.
* If a message fails to be handled for a second time, it will be rejected and sent to a dead-letter queue for you to deal with that.
* RabbitMQ requires no previous configuration, the queues are created and bound every time a consumer is started. These operations are idempotent so if your queue was already there, it does nothing.
* Any exception in the handler will stop the consumer, close all connections and bubble up. Exiting (restarting) your long running process at this stage is the recomended pattern.

## Usage

Instantiating `messagebus.MessageBus` is pretty much everything you need.

### Publishing a message:
```python
bus = MessageBus()
bus.publish('some.message', {'example_payload': 4})
```

### Subscribing to messages:
```python

def the_callback(message):
    print('some.message received!')

bus = MessageBus()
bus.subscribe('some.message', the_callback)
bus.start() # (will block forever)
```

Additionally, there are two methods to implement synchronous/RPC-like calls over the bus.
```python
    def subscribe_and_publish_response(self, message, callback)
    def publish_and_get_response(self, message, payload, timeout_secs=5)
```
   
