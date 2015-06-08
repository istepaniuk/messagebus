#!/usr/bin/env python

import pyrabbit
import urlparse
import settings
import json
import argparse
import sys
import uuid

# necessary because pyrabbit prints crap to the output
# pull-requested a change:
# https://github.com/istepaniuk/pyrabbit/commit/41c9a11245475729f114ecc3dcfa20a4f5409545
import redirect

parser = argparse.ArgumentParser(description='Publish messages from stdin, one by line, to the tcr exchange.')
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
    payload = json.dumps(message['payload'], ensure_ascii=False)
    with redirect.RedirectStdStreamsToDevNull():
        client.publish(vhost, 'tcr', message['routing_key'], payload)

