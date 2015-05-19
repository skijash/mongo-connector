
import consul


class Statii(object):
    OPLOG_EVENT = 'oplog'
    OPLOG_KEY = 'service/connector/oplog/%s'

    def __init__(self):
        self.master = consul.Consul()

    def fire_oplog(self, payload):
        self.master.event.fire(self.OPLOG_EVENT, payload)

    def write_oplog_kv(self, name, payload):
        self.master.kv.put(self.OPLOG_KEY % name, payload)
