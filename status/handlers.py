
from bottle import route, run, request

from mongo_connector.util import long_to_bson_ts
from progress import read_conf, get_client, read_oplog_file, get_oplog_cursor
from settings import BASE_RESPONSE, HOST, PORT
from settings import accounts


@route('/')
def home():
    return BASE_RESPONSE


@route('/status/<account_name>/<collection_name>')
def oplog_progress_collection(account_name, collection_name):
    response = {'account_name': account_name}
    response.update(BASE_RESPONSE)
    account_name = read_conf(account_name)
    account_conf = accounts[account_name]
    print 'acnf', account_conf
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
        local_client = get_client(account_conf['mainAddress'])

    if collection_name:
        namespaces = ['%s.%s' % (account_name, collection_name)]
    else:
        try:
            namespaces = account_conf['namespaces']['include']
        except KeyError:
            namespaces = None

    oplog_cursor = get_oplog_cursor(
        local_client.local.oplog.rs,
        namespaces,
        oplog_timestamp
    )
    response.update({'oplog_count': oplog_cursor.count()})
    return response


@route('/status/<account_name>')
def oplog_progress(account_name):
    return oplog_progress_collection(account_name, None)


@route('/control/<account_name>/<command>')
def control(account_name, command):
    print account_name, command
    print request.json


def main():
    run(host=HOST, port=PORT)


read_conf('base')
mongo_address = accounts['base']['mainAddress']
client = get_client(mongo_address)
oplog = client.local.oplog.rs


if __name__ == '__main__':
    main()
