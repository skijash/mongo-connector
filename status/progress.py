
from pymongo import MongoClient
import logging
import json
import os

from mongo_connector import errors
from settings import PROGRESS_DIR, OPLOG_PATTERN, accounts

logger = logging.getLogger(__name__)


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
    if 'setName' not in is_master:
        raise errors.ReplicaSetNotPresent('Status service startup unsuccessful')
    return MongoClient(mongo_address, replicaSet=is_master['setName'])
