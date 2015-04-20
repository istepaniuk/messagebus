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

parser = argparse.ArgumentParser(description='Publish messages from stdin, one by line, to the specified queue.')
parser.add_argument('queue_name', metavar='QUEUE_NAME', type=str,
                   help='the name of the queue to publish the messages to')
args = parser.parse_args()
parsed_url = urlparse.urlparse(settings.RABBITMQ_BROKER_URL)

client = pyrabbit.Client(
    parsed_url.hostname + ":" + str(parsed_url.port),
    parsed_url.username,
    parsed_url.password)

messages = sys.stdin.readlines()
for message in messages:
    json.loads(message)

with redirect.RedirectStdStreamsToDevNull():
    vhost = '/'
    tmp_x = "tmp-mgmt-x-%s" % uuid.uuid1()
    client.create_exchange(vhost, tmp_x, 'topic', auto_delete = True, durable = False)
    client.create_binding(vhost, tmp_x, args.queue_name, '#')
    rt_key = str(args.queue_name).rsplit("-", 1)[-1]
    for message in messages:
        messages = client.publish(vhost, tmp_x, rt_key, message)
    client.delete_binding(vhost, tmp_x, args.queue_name, '%23')

