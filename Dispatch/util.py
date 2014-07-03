import logging
import os
import smtplib
import sys
import threading

from ConfigParser import SafeConfigParser

def die(msg, ex=None):
    print msg
    if ex: print ex
    sys.exit(1)

def setup_logging(settings, enable_debug, daemon):
    debug = logging.getLogger('util').debug
    info = logging.getLogger('util').info
    if daemon:
        daemon_log = settings['DAEMON_LOG']
        if daemon_log:
            fh = logging.FileHandler(daemon_log, 'a')
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
            fh.setFormatter(formatter)
            logging.getLogger().addHandler(fh)
            if enable_debug:
                logging.getLogger().setLevel(logging.DEBUG)
            else:
                logging.getLogger().setLevel(logging.INFO)

    else:
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

        if enable_debug:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.INFO)

    debug("Debug Mode Enabled")
    for k,v in settings.iteritems():
        debug("%s:\t%s" % (k,v))

    debug('Logging started successfully')

def read_config(config_file):
    settings = {}

    # Read config file
    if not os.path.exists(config_file):
        die('Config file does not exist: %s' % config_file)

    parser = SafeConfigParser()
    parser.read(config_file)
    
    # Read DB info
    try:
        section = 'database'
        settings['DB_SERVER'] = parser.get(section, 'SERVER')
        settings['DB_USER'] = parser.get(section, 'USER')
        settings['DB_PASS'] = parser.get(section, 'PASS')
        settings['DB_NAME'] = parser.get(section, 'NAME')
    except Exception, err:
        die('Error in the database section of the config file.', err)

    try:
        section = 'dispatch'
        settings['POLL_INTERVAL'] = int(parser.get(section, 'POLL_INTERVAL'))
        settings['LOCK_FILE'] = parser.get(section, 'LOCK_FILE')
        settings['DAEMON_LOG'] = parser.get(section, 'DAEMON_LOG')
    except Exception, err:
        die('Error in the transfermanager section of the config file.', err)

    return settings

def send_email(msg, to=None):

    if not to:
        to = ['cpowell@vubiquity.com']
    # TODO: Add servername and addresses to config file
    servername = '192.168.21.112'
    from_address = 'dispatch@vubiquity.com'
    subject = 'Dispatch has disabled a poller...'
    message = """\
From: %s
To: %s
Subject: %s

%s
""" % (from_address, ', '.join(to), subject, msg)

    server = smtplib.SMTP(servername)
    server.sendmail(from_address, to, message)
    server.quit()

def getsize(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    else:
        total = 0
        for (path, dirs, files) in os.walk(path):
            for f in files:
#                total += os.path.getsize(f)
                total += os.path.getsize(os.path.join(path, f))
        return total

class StoppableThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self):
        super(StoppableThread, self).__init__()
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()
                                                     
    def stopped(self):
        return self._stop.isSet()
