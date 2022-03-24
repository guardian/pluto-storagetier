#!/usr/bin/env python3

from gnmvidispine.vs_search import *
from argparse import ArgumentParser
import pika
import logging
import json
import uuid
import sys
from os.path import join
from urllib.parse import urlparse

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


def load_vs_credentials(basepath:str) -> (str, str):
    """
    loads in vidispine credentials from a mounted secret
    :param basepath: base path where the credentials are mounted
    :return: a tuple of (username, password). Raises an exception on error
    """
    logger.info("Reading vidispine credentials from {0}".format(basepath))
    try:
        with open(join(basepath, "vsuser")) as f:
            username = f.read()
        with open(join(basepath, "vspass")) as f:
            pwd = f.read()

        return username, pwd
    except FileNotFoundError:
        with open(join(basepath, "vidispine_admin_user")) as f:
            username = f.read()
        with open(join(basepath, "vidispine_admin_password")) as f:
            pwd = f.read()

        return username, pwd


class RMQCredentials(object):
    def __init__(self, host:str, port:int, vhost:str, user:str, passwd:str):
        self.host = host
        self.port = port
        self.vhost = vhost
        self.user = user
        self.passwd = passwd

    def validate(self):
        if self.host=="":
            raise ValueError("RabbitMQ host is not set")
        if self.port==0:
            raise ValueError("RabbitMQ port is not set")
        if self.vhost=="":
            raise ValueError("RabbitMQ vhost is not set")
        if self.user=="":
            raise ValueError("RabbitMQ user is not set")
        if self.passwd=="":
            raise ValueError("RabbitMQ password is not set")

    @staticmethod
    def load_from_file(basepath:str) -> (str, str):
        """
        loads in rabbitmq credentials from a mounted secret
        :param basepath:
        :return:
        """
        logger.info("Reading RabbitMQ credentials from {0}".format(basepath))
        with open(join(basepath, "rabbitmq_client_uri")) as f:
            urlcontent = f.read()
            parsed = urlparse(urlcontent)
            return RMQCredentials(
                host=parsed.hostname,
                port=parsed.port if parsed.port is not None else 5672,
                vhost=parsed.path.lstrip("/") if parsed.path is not None and parsed.path != "/" else "default",
                user=parsed.username,
                passwd=parsed.password
            )


##START MAIN
parser = ArgumentParser(description="Searches Vidispine for items that have no gnm_vidispine_id and requests updates for them")
parser.add_argument("--vshost", default="localhost", help="Vidispine host")
parser.add_argument("--vsport", default=8080, help="Port number for vidispine")
parser.add_argument("--vscreds", default=None, help="Path to retrieve Vidispine credentials")
parser.add_argument("--https", default=False, action="store_true", help="use https communication")
parser.add_argument("--rmq", default="localhost", help="RabbitMQ host")
parser.add_argument("--rmqcreds", default=None, help="Path to retrieve RabbitMQ credentials. Expects text files called `rabbitmq_user` and `rabbitmq_passwd`")
parser.add_argument("--rmquser", default="admin", help="RabbitMQ user, as an alternative to using rmq-credentials")
parser.add_argument("--rmqpasswd", default="admin", help="RabbitMQ password, as an alternative to using rmq-credentials")
parser.add_argument("--vhost", default="pluto-ng", help="RabbitMQ VHhost")
parser.add_argument("--limit", default=None, help="Only process up to this many items")
parser.add_argument("--count", default=False, action="store_true", help="Don't add anything to a queue, just count the number of items")
args = parser.parse_args()

if not args.rmquser or not args.rmqpasswd:
    parser.print_usage()
    sys.exit(1)

logger.info("Connecting to rabbitmq...")
if args.rmqcreds:
    logger.info("Loading credentials from file...")
    rmqcreds = RMQCredentials.load_from_file(args.rmqcreds)
else:
    logger.info("Using commandline credentials")
    rmqcreds = RMQCredentials(host=args.rmq, port=5672, vhost=args.vhost, user=args.rmquser, passwd=args.rmqpasswd)

credentials = pika.PlainCredentials(rmqcreds.user, rmqcreds.passwd)
rmq_conn = pika.BlockingConnection(
    pika.ConnectionParameters(host=rmqcreds.host,
                              port=rmqcreds.port,
                              virtual_host=rmqcreds.vhost,
                              credentials=credentials)
)

rmq_chan = rmq_conn.channel()
logger.info("Done.")

vscreds = load_vs_credentials(args.vscreds)

search = VSSearch(host=args.vshost, port=args.vsport, https=args.https, user=vscreds[0], passwd=vscreds[1])
search.addCriterion({"gnm_nearline_id": ""})
action = search.execute()

logger.info("Found {0} items without gnm_nearline_id".format(action.totalItems))
if args.count:
    sys.exit(0)

ctr = 0
for item in action.results(shouldPopulate=False):
    ctr += 1
    logger.info("Sending event {0}/{1} for {2}....".format(ctr, action.totalItems, item.name))
    send_message(rmq_chan, item.name)
    if args.limit is not None and ctr>=int(args.limit):
        logger.info("Hit limit of {0} items, completing".format(args.limit))
        break

logger.info("All done.")
