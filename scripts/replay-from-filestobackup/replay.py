#!/usr/bin/env python3

from argparse import ArgumentParser
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.response.hit import Hit
import dateutil.parser as dateutil
import mimetypes
import os
import pika
import logging
import uuid
from pprint import pprint
import json
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

## The purpose of this script is to read un-backed-up files from Elasticsearch and replay them onto an exchange as
## "new" files.


def should_drop(filepath: str) -> bool:
    """
    returns a boolean True if the file should be ignored for backup
    :param filepath:
    :return:
    """
    return ".Trashes" in filepath \
           or "Adobe Premiere Preview Files" in filepath \
           or filepath.endswith(".pkf") \
           or not filepath.startswith("/srv/Multimedia2/") \
           or filepath.startswith("/srv/Multimedia2/Scratch")


def convert_to_assetsweeper_record(h: Hit):
    """
    converts the data from the given search hit into a (partial) asset sweeper record for storagetier
    :param h: search hit to convert
    :return:
    """
    if isinstance(h.wholepath, tuple):
        wholepath = "".join(list(h.wholepath))
    else:
        wholepath = h.wholepath

    parsed_time = dateutil.parse(h.timestamp)
    epoch_time = int(parsed_time.timestamp())
    file_dir = os.path.dirname(wholepath)
    file_name = os.path.basename(wholepath)
    return ({
        "imported_id": None,
        "mime_type": mimetypes.types_map.get("."+h.extension, "application/octet-stream"),
        "mtime": epoch_time,
        "ctime": epoch_time,
        "atime": epoch_time,
        "size": h.size,
        "ignore": False,
        "owner": 0,
        "group": 0,
        "parent_dir": file_dir,
        "filename": file_name
    }, wholepath)


def scan_for_files(client: Elasticsearch, index_name: str, filter_type: str):
    """
    generator that scans for files from the index and yields them
    :param client: elasticsearch client object
    :param index_name: name of the index to scan
    :param filter_type: either 'nobackup' or 'resync'
    :return: a generator which yields the records as dictionaries
    """
    if filter_type=="nobackup":
        filter_str = "Entry had no matches"
    elif filter_type=="resync":
        filter_str = "Matched none out of 1 potential entries"
    else:
        logger.error("Invalid filter option. You must specify 'nobackup' or 'resync'")
        raise ValueError("Invalid filter parameter")

    s = Search(using=client, index=index_name)\
        .query("term", **{"notes.keyword": filter_str})
    logger.info("Got a total of {0} records to upload".format(s.count()))

    return s.scan()


def send_to_rabbit(conn, exchg: str, routing_key: str, rec: dict):
    """
    Sends the given message to a rabbitmq exchange
    :param conn: active RabbitMQ Channel object
    :param exchg: exchange name to send to (string)
    :param routing_key: routing key to send with it (string)
    :param rec: the record to send, as a dictionary
    :return: None
    """
    content = json.dumps(rec, separators=(',', ':'))
    msg_id = uuid.uuid4()
    props = pika.BasicProperties(content_type="application/json",content_encoding="utf8",message_id=msg_id.hex)
    conn.basic_publish(exchg, routing_key, content, props)


# START MAIN
parser = ArgumentParser(description="Read un-backed-up files from elasticsearch and replay them for backup")
parser.add_argument("--es",dest='esurl',default='localhost',help="URL(s) to access elasticsearch on")
parser.add_argument("--index",dest='indexname',default='files-to-back-up',help="Index name to scan")
parser.add_argument("--rabbitmq",dest="rmq",default="localhost", help="rabbitmq instance to send results to")
parser.add_argument("--port",dest="port",default="5672", help="port on which rabbitmq is running")
parser.add_argument("--user", dest="user", default="rabbit", help="rabbitmq username")
parser.add_argument("--passwd", dest="passwd", default="rabbit", help="rabbitmq password")
parser.add_argument("--vhost", dest="vhost", default="/", help="virtualhost on the rabbitmq broker")
parser.add_argument("--exchange", dest="exchange", help="rabbitmq exchange to send to")
parser.add_argument("--routing-key", dest="routingkey", default="test.message", help="routing key for sent messages")
parser.add_argument("--limit", dest="limit", help="limit the returned results to this amount")
parser.add_argument("--filter-type", dest="filtertype", default="nobackup", help="defaults to 'nobackup' which means only select files logged as having no backup at all. You can also specify 'resync', which means to only select files which have a backup that does not match.")
args = parser.parse_args()

mimetypes.init()

credentials = pika.PlainCredentials(args.user, args.passwd)
rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=args.rmq, port=args.port, virtual_host=args.vhost, credentials=credentials))
rmq_chan = rmq_conn.channel()

esclient = Elasticsearch(args.esurl.split(","), verify_certs=True)
i = 0
limit = None

if args.limit is not None:
    limit = int(args.limit)

for f in scan_for_files(esclient, args.indexname, args.filtertype):
    rec, wholepath = convert_to_assetsweeper_record(f)
    if should_drop(wholepath):
        logger.debug("dropping {0}".format(wholepath))
        continue

    pprint(rec)
    send_to_rabbit(rmq_chan, args.exchange, args.routingkey, rec)
    i+=1
    if limit and i>=limit:
        logger.info("Exiting due to user-supplied limit")
        break

logger.info("Selected {0} records".format(i))
