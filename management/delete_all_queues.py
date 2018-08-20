#!/usr/bin/env python

import pyrabbit
from urllib.parse import urlparse
import settings
import argparse

parser = argparse.ArgumentParser(
    description='WARNING: DELETES every single queue')
parser.add_argument(
    'yes', metavar='YES', type=str,
    help='Param to make sure this does not get run by accident')
args = parser.parse_args()

parsed_url = urlparse(settings.RABBITMQ_BROKER_URL)

client = pyrabbit.Client(
    parsed_url.hostname + ":" + str(parsed_url.port),
    parsed_url.username,
    parsed_url.password)

vhost = parsed_url.path or '/'

queues = client.get_queues()

for queue in queues:
    client.delete_queue(vhost, queue['name'])
