#!/usr/bin/env python

import pyrabbit
import urlparse
import settings
import json
import argparse
import sys
import uuid
import redirect

parser = argparse.ArgumentParser(
    description="Publish messages read from stdin, one per line, to the queue "
                "specified in the message x-death properties")
args = parser.parse_args()
parsed_url = urlparse.urlparse(settings.RABBITMQ_BROKER_URL)

client = pyrabbit.Client(
    parsed_url.hostname + ":" + str(parsed_url.port),
    parsed_url.username,
    parsed_url.password)

messages = sys.stdin.readlines()
messages = [json.loads(message) for message in messages]

vhost = '/'
for message in messages:
    q_name = message['properties']['headers']['x-death'][0]['queue']
    tmp_x = "temporary-mgmt-exchange-%s" % uuid.uuid1()
    with redirect.RedirectStdStreamsToDevNull():
        client.create_exchange(vhost, tmp_x, 'topic', auto_delete=True,
                               durable=False)
        client.create_binding(vhost, tmp_x, q_name, '#')
        rt_key = str(message['routing_key'])
        payload = json.dumps(message['payload'], ensure_ascii=False)
        client.publish(vhost, tmp_x, rt_key, payload)
        client.delete_binding(vhost, tmp_x, q_name, '%23')
    print("Replayed message %s." % message['message_count'])
