#!/usr/bin/env python

import os
import sys
import json
import base64
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
    pass


def cleanup(account_name):
    """Delete the files related to the account_name - oplog state and conf"""
    print 'Cleaning up...'
    subprocess.call(
        'sudo rm /run/mongo-connector/%s.*' % account_name)
    print 'Account conf and oplog state removed.'


def check_and_start(running, conf):
    account_name = conf['Key'].rsplit('/', 1)[1]
    conf = base64.b64decode(conf['Value'])

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


def main():
    for line in sys.stdin:
        conf = json.loads(line)
        running = get_running().split(' ')

        for c in conf:
            # check if the account connector is running
            # and if not, start it
            check_and_start(running, c)

        if running:
            # if something is left here, it means that a key has been deleted
            # and that connector needs to be stopped
            for account_name in running:
                stop_service(account_name)


if __name__ == '__main__':
    main()
