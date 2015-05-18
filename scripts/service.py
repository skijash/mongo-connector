#!/usr/bin/env python

import os
import sys
import json
import base64
import getopt
import consul
import subprocess


CONFIG_DIR = '/etc/mongo-connector/config'


def get_running():
    return subprocess.check_output(
        'sudo service mongo-connector status', shell=True)


def check_running(account_name):
    out = subprocess.check_output(
        'sudo service mongo-connector status ' + account_name, shell=True)
    print account_name, out
    if 'is not running' in out:
        return False
    return True


def write_conf(account_name, conf):
    """Write configuration to the config dir"""
    filename = '%s.json' % account_name
    print 'writing conf to %s...' % filename
    open(os.path.join(CONFIG_DIR, filename), 'w').write(conf)
    print 'conf written'


def start_service(account_name):
    """Just start the service with the existing oplog state and conf"""
    print 'starting %s connector...' % account_name
    subprocess.call(
        'sudo service mongo-connector start ' + account_name, shell=True)
    print 'started'


def stop_service(account_name):
    """Stop the service and update oplog state"""
    print 'stopping connector %s...' % account_name
    subprocess.call(
        'sudo service mongo-connector stop ' + account_name, shell=True)
    update_oplog_state(account_name)
    print 'stopped'


def update_oplog_state(account_name):
    """Update oplog state in kv store"""
    master = consul.Consul()
    oplog_file = '/var/run/mongo-connector/%s.oplog' % account_name
    master.kv.put('service/connector/oplog/%s' % account_name, open(oplog_file).read())


def cleanup(account_name):
    """Delete the files related to the account_name - oplog state and conf"""
    print 'Cleaning up...'
    subprocess.call(
        'sudo rm /run/mongo-connector/%s.*' % account_name)
    print 'Account conf and oplog state removed.'


def check_and_start(account_name, running, conf):
    if account_name not in running:
        print '%s connector is not running' % account_name
        write_conf(account_name, conf)
        start_service(account_name)
    else:
        running.remove(account_name)


def stop_and_cleanup(account_name):
    stop_service(account_name)
    update_oplog_state(account_name)
    cleanup(account_name)


def start_stop(payload):
    for line in payload:
        conf = json.loads(line)
        running = get_running().split(' ')

        for c in conf:
            # check if the account connector is running
            # and if not, start it
            account_name = c['Key'].rsplit('/', 1)[1]
            conf = base64.b64decode(c['Value'])
            check_and_start(account_name, running, conf)

        if running:
            # if something is left here, it means that a key has been deleted
            # and that connector needs to be stopped
            for account_name in running:
                stop_service(account_name)


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


RUN = {
    'start': [start_service],
    'stop': [stop_service, update_oplog_state],
    'stop-and-cleanup': [stop_service, update_oplog_state, cleanup],
    'cleanup': [cleanup],
    'update-oplog': [update_oplog_state]
}


def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "h", ["help"])

            try:
                assert len(args) <= 1
                for f in RUN[args[0]]:
                    f(sys.stdin)
            except (KeyError, AssertionError):
                raise Usage('Argument should be one of %s' % RUN.keys())

        except getopt.error as msg:
            raise Usage(msg)

    except Usage as err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2


if __name__ == "__main__":
    sys.exit(main())
