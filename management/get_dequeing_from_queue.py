#!/usr/bin/env python

import pyrabbit
import urlparse
import settings
import json
import argparse
import os
import sys

# necessary because pyrabbit prints crap to the output
# pull-requested a change:
# https://github.com/istepaniuk/pyrabbit/commit/41c9a11245475729f114ecc3dcfa20a4f5409545
import redirect

parser = argparse.ArgumentParser(description='Gets messages from a queue. Does not requeue!')
parser.add_argument('queue_name', metavar='ORIGINAL_QUEUE_NAME', type=str,
                   help='the name the queue to get messages from')
args = parser.parse_args()
parsed_url = urlparse.urlparse(settings.RABBITMQ_BROKER_URL)


client = pyrabbit.Client(
    parsed_url.hostname + ":" + str(parsed_url.port),
    parsed_url.username,
    parsed_url.password)

with redirect.RedirectStdStreamsToDevNull():
    messages = client.get_messages('/', args.queue_name, 1000, requeue = False)

def set_output_encoding(encoding='utf-8'):
    import sys
    import codecs
    '''When piping to the terminal, python knows the encoding needed, and
       sets it automatically. But when piping to another program (for example,
       | less), python can not check the output encoding. In that case, it
       is None. What I am doing here is to catch this situation for both
       stdout and stderr and force the encoding'''
    current = sys.stdout.encoding
    if current is None :
        sys.stdout = codecs.getwriter(encoding)(sys.stdout)
    current = sys.stderr.encoding
    if current is None :
        sys.stderr = codecs.getwriter(encoding)(sys.stderr)

set_output_encoding()

for message in messages:
    message['payload'] = json.loads(message['payload'])
    print json.dumps(message, ensure_ascii=False)

