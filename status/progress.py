
from bottle import route, run
from pymongo import MongoClient
from bson.timestamp import Timestamp
import logging
import json
import os


logger = logging.getLogger(__name__)

host = '0.0.0.0'
port = 8080
SERVICE_VERSION = '0.1'
SERVICE_NAME = 'mc-status'
BASE_RESPONSE = {'service': SERVICE_NAME, 'version': SERVICE_VERSION}
PROGRESS_DIR = '/var/run/mongo-connector'
OPLOG_PATTERN = '%s/%s.oplog'

accounts = {
    'base': {
        'conf': '/etc/mongo-connector/base.json',
        'oplog': '/var/log/mongo-connector/oplog.timestamp'
    }
}


def bson_ts_to_long(timestamp):
    """Convert BSON timestamp into integer.

    Conversion rule is based from the specs
    (http://bsonspec.org/#/specification).
    """
    return ((timestamp.time << 32) + timestamp.inc)


def long_to_bson_ts(val):
    """Convert integer into BSON timestamp.
    """
    seconds = val >> 32
    increment = val & 0xffffffff

    return Timestamp(seconds, increment)


def read_oplog_file(oplog_file):
    progress = json.load(open(oplog_file))
    return progress[1]


def read_conf(account_name):
    if account_name in accounts:
        conf = json.load(open(accounts[account_name]['conf']))
    else:
        path = os.path.join(PROGRESS_DIR, '%s.json' % account_name)

        # specific account conf file exists
        if os.path.isfile(path):
            accounts[account_name] = {}
            accounts[account_name]['conf'] = path
            accounts[account_name]['oplog'] = OPLOG_PATTERN % (PROGRESS_DIR, account_name)
            return read_conf(account_name)

        # no specific conf file, using base conf
        else:
            logger.info('No account specific, using base conf.')
            return read_conf('base')

    accounts[account_name]['mainAddress'] = conf['mainAddress']
    accounts[account_name]['namespaces'] = conf['namespaces']
    return account_name


def get_oplog_cursor(oplog, oplog_ns_set=None, timestamp=None):
    """Get a cursor to the oplog after the given timestamp, filtering
    entries not in the namespace set.
    If no timestamp is specified, returns a cursor to the entire oplog.
    """
    query = {}
    if oplog_ns_set:
        query['ns'] = {'$in': oplog_ns_set}

    if timestamp is None:
        cursor = oplog.find(query, tailable=True, await_data=True)
    else:
        query['ts'] = {'$gte': timestamp}
        cursor = oplog.find(query, tailable=True, await_data=True)
        # Applying 8 as the mask to the cursor enables OplogReplay
        cursor.add_option(8)
    return cursor


def get_client(mongo_address):
    client = MongoClient(mongo_address)
    is_master = client.admin.command("isMaster")
    client.disconnect()
    return MongoClient(mongo_address, replicaSet=is_master['setName'])


@route('/')
def home():
    return BASE_RESPONSE


@route('/status/<account_name>')
def oplog_progress(account_name):
    response = {'account_name': account_name}
    response.update(BASE_RESPONSE)
    account_name = read_conf(account_name)
    account_conf = accounts[account_name]
    response.update(account_conf)

    oplog_timestamp = None
    if 'oplog' in account_conf:
        oplog_timestamp = long_to_bson_ts(read_oplog_file(account_conf['oplog']))
        response.update({
            'progress': oplog_timestamp.time
        })

    global client
    local_client = client
    if client.host is not account_conf['mainAddress'].split(':')[0]:
        read_conf(account_name)
        local_client = get_client(accounts[account_name]['mainAddress'])
    oplog_cursor = get_oplog_cursor(
        local_client.local.oplog.rs,
        accounts[account_name]['namespaces']['include'],
        oplog_timestamp
    )
    response.update({'oplog_count': oplog_cursor.count()})
    return response


read_conf('base')
mongo_address = accounts['base']['mainAddress']
client = get_client(mongo_address)
oplog = client.local.oplog.rs


if __name__ == '__main__':
    run(server=SERVICE_NAME, host=host, port=port)
