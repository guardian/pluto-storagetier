#!/usr/bin/env python3

from gnmvidispine.vs_search import *
from argparse import ArgumentParser
import pika
import logging
import json
import uuid
import sys

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
logger.level = logging.DEBUG


def send_message(conn, vsid:str):
    msg = json.dumps({
        "field": [
            {
                "key": "itemId",
                "value": vsid
            }
        ]
    })
    msg_id = uuid.uuid4()
    props = pika.BasicProperties(content_type="application/json",content_encoding="utf8",message_id=msg_id.hex)
    conn.basic_publish("vidispine-events", "vidispine.itemneedsbackup", msg, props)


##START MAIN
parser = ArgumentParser(description="Searches Vidispine for items that have no gnm_vidispine_id and requests updates for them")
parser.add_argument("--vshost", default="localhost", help="Vidispine host")
parser.add_argument("--vsport", help="Port number for vidispine")
parser.add_argument("--https", action="store_true", help="use https communication")
parser.add_argument("--rmq", default="localhost", help="RabbitMQ host")
parser.add_argument("--rmquser", help="RabbitMQ user")
parser.add_argument("--rmqpasswd",  help="RabbitMQ password")
parser.add_argument("--vhost", default="pluto-ng", help="RabbitMQ VHhost")
parser.add_argument("--limit", help="Only process up to this many items")
args = parser.parse_args()

if not args.rmquser or not args.rmqpasswd:
    parser.print_usage()
    sys.exit(1)

logger.info("Connecting to rabbitmq...")
credentials = pika.PlainCredentials(args.rmquser, args.rmqpasswd)
rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=args.rmq, port=args.port, virtual_host=args.vhost, credentials=credentials))
rmq_chan = rmq_conn.channel()
logger.info("Done.")

search = VSSearch(host=args.vshost, port=args.vsport, https=args.https)
search.addCriterion({"gnm_nearline_id": ""})
action = search.execute()

logger.info("Found {0} items without gnm_nearline_id", action.totalItems)
ctr = 0
for item in action.results(shouldPopulate=False):
    ctr += 1
    logger.info("Sending event {0}/{1} for {2}....".format(ctr, action.totalItems, item.name))
    send_message(rmq_chan, item.name)

logger.info("All done.")
