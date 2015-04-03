
from bottle import route, run
import logging
import json
import os

logger = logging.getLogger(__name__)

host = '0.0.0.0'
port = 8080
SERVICE_VERSION = '0.1'
BASE_RESPONSE = {'service': 'mc status', 'version': SERVICE_VERSION}
PROGRESS_DIR = '/var/run/mongo-connector'

conf_locations = {
    'base': '/etc/mongo-connector/base.json'
}


def read_oplog_file(oplog_file):
    progress = json.load(open(oplog_file))
    return progress[1]


def read_conf(account_name):
    if account_name in conf_locations:
        conf = json.load(open(conf_locations[account_name]))
    else:
        path = os.path.join(PROGRESS_DIR, '%s.json' % account_name)

        # specific account conf file exists
        if os.path.isfile(path):
            conf_locations[account_name] = path
            return read_conf(account_name)

        # no specific conf file, using base conf
        else:
            logger.info('No account specific configuration, using base (%s)' %
                        conf_locations['base'])
            return read_conf('base')

    oplog_file = '%s/%s.oplog' % (PROGRESS_DIR, account_name)

    return {
        'mainAddress': conf['mainAddress'],
        'oplogFile': oplog_file,
        'progress': read_oplog_file(oplog_file)
    }


@route('/')
def home():
    return BASE_RESPONSE


@route('/status/<account_name>')
def oplog_progress(account_name):
    response = {'account_name': account_name}
    response.update(BASE_RESPONSE)
    response.update(read_conf(account_name))
    return response


run(host=host, port=port)
