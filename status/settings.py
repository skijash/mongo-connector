
HOST = '0.0.0.0'
PORT = 8080
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
