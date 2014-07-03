#!/usr/bin/env python

__author__ = "Chris Powell"

import getopt
import logging
import os
import sys
import traceback

from Dispatch.lockfile import LockFile
from Dispatch.version import VERSION
from Dispatch.util import read_config, setup_logging
from Dispatch.transfer_manager import TransferManager

def usage():
    print "Usage:"
    print "%s [-c configfile | --config=configfile] [-h | --help] --daemon --debug --version" % sys.argv[0]
    print
    print
    print " --config:       The name of the config file to parse"
    print " --daemon:       Run FES in daemon mode"
    print " --debug:        Enables debug logging"
    print " --version:      Prints the version and exits"
    print " --help:         Print the usage"

if __name__ == '__main__':                                                                                                                                                                                                                                                           
    config_file = '/opt/dispatch/dispatch.conf'
    daemon = 0
    enable_debug = 0
    args = sys.argv[1:]
    try:
        (opts, getopts) = getopt.getopt(args, 'c:h?',
                ['config=', 'version', 'daemon', 'debug', 'help'])

    except:
        print '\nInvalid command line option detected.'
        usage()
        sys.exit(1)

    for opt, arg in opts:
        if opt in ('-h', '-?', '--help'):
            usage()
            sys.exit(0)
        if opt in ('-c', '--config'):
            config_file = arg
        if opt == '--daemon':
            daemon = 1
        if opt == '--debug':
            enable_debug = 1
        if opt == '--version':
            print "TransferManager version:", VERSION
            sys.exit(0)

    settings = read_config(config_file)

    keys_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'keys')
    settings['SSH_KEYS'] = keys_dir
    if not os.path.exists(keys_dir):
        os.mkdir(keys_dir)

    setup_logging(settings, enable_debug, daemon)

    lock_file = LockFile(settings['LOCK_FILE'])

    lock_file.create()

    try:
        tm = TransferManager(settings, lock_file, daemon)
    except Exception, e:
        traceback.print_exc(file=sys.stdout)
        print "\nTransferManager exited abnormally"

    # Remove lock file on exit
    lock_file.remove()


