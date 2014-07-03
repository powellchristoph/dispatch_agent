import logging
import multiprocessing
import os
import shutil
import signal
import subprocess
import sys
import time
from collections import deque
from datetime import datetime
from socket import gethostname

from daemon import createDaemon
from pollers import PollerManager
from table_def import Poller, TransferLog, ErrorMgr
from util import die, send_email, getsize

from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker

info = logging.getLogger('transfermanager').info
debug = logging.getLogger('transfermanager').debug
warning = logging.getLogger('transfermanager').warning
critical = logging.getLogger('transfermanager').critical

class TransferManager:
    """
    The TransferManager spawns a PollerManager which executes each poller. The pollers
    find and validate files and then submits them to the TransferManager. The TM then 
    executes the actual transfer of the files and updates the relavant information in 
    the database.
    """

    def __init__(self, settings, lock_file, daemon):
        info('Creating TransferManager')
        self.__db_user = settings['DB_USER']
        self.__db_pass = settings['DB_PASS']
        self.__db_name = settings['DB_NAME']
        self.__db_server = settings['DB_SERVER']
        self.__poll_interval = settings['POLL_INTERVAL']
        self.__daemon_log = settings['DAEMON_LOG']
        self.__ssh_keys = settings['SSH_KEYS']
        self.lock_file = lock_file
        self.daemon = daemon
        self.transfer_queue = {}
        self.process_list = {}

        if daemon:
            info('Launching Dispatch daemon...')
            self.lock_file.remove()
            
            retCode = createDaemon()                                                                                        
            debug(retCode)
            if retCode == 0:
                signal.signal(signal.SIGTERM, self.kill_daemon)                                                                      
                signal.signal(signal.SIGUSR1, self.graceful_kill_daemon)                                                                      
                self.lock_file.create()
                info('Dispatch daemon is now running, %s', os.getpid())

                try:
                    self.run_loop()
                except Exception, e:
                    debug('Error')
                    debug(str(e))

            else:
                die('Error creating daemon: %s (%d)' % (retCode[1], retCode[0]))

        else:
            signal.signal(signal.SIGINT, self.kill_non_daemon)
            info('Launching Dispatch non-daemon mode...')
            info('Use CTRL-C to quit...')
            self.run_loop()

    def connect_to_db(self):
        info('Connected to %s' % self.__db_server)
        db_uri = 'mysql://{user}:{password}@{host}/{db}'
        engine = create_engine(db_uri.format(
            user        = self.__db_user,
            password    = self.__db_pass,
            host        = self.__db_server,
            db          = self.__db_name)
            )
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def start_poller_mgr(self):
        info('Gathering poller information')
        try:
            self.pollers = self.session.query(Poller).filter(Poller.enabled == True).all()
        except Exception, err:
            self.lock_file.remove()
            critical('Error gathering subpollers: %s' % str(err))
            sys.exit(1)

        for p in self.pollers:
            if not os.path.exists(p.path):
                self.lock_file.remove()
                critical('Poller path for %s does not exist: %s' % (p.name, p.path))
                sys.exit(1)
            self.reset_errors(p.name)

        info('Forking poller manager')
        try:
            self.pollermgr = PollerManager(self.pollers, self.transfer_queue, self.process_list, self.__poll_interval)
            self.pollermgr.start()
        except Exception, e:
            self.lock_file.remove()
            die('Error starting poller manager', e)

    def kill_daemon(self, sigum, frame):
        info('Dispatch daemon is shutting down...')
        self.clean_up_transfers()
        info('Dispatch shutdown successfully.')
        sys.exit(0)

    def graceful_kill_daemon(self, sigum, frame):
        info('Dispatch daemon shutting down, gracefully...')
        self.pollermgr.stop()
        self.pollermgr.join()

        for name, proclist in self.process_list.iteritems():
                done = False
                while not done:
                    if proclist:
                        info('Waiting for %s transfers to finish...' % name)
                        self.check_procs()
                        time.sleep(5)
                    else:
                        done = True

        self.session.commit()
        self.session.close_all()
        info('Dispatch shutdown successfully.')
        sys.exit(0)

    def kill_non_daemon(self, sigum, frame):
        debug('Received CTRL-C')
        info('Dispatch process is shutting down')

        self.clean_up_transfers()

        self.lock_file.remove()
        info('Dispatch shutdown successfully.')
        sys.exit(0)

    def clean_up_transfers(self):
        transfers = self.session.query(TransferLog).\
            filter(TransferLog.status=='Transferring').all()

        for t in transfers:
            t.ended = datetime.utcnow()
            t.status = 'Cancelled'

        for name, proclist in self.process_list.iteritems():
            if proclist:
                warning('Terminating running transfers for %s' % name)
                for p in proclist:
                    p.terminate()

        self.session.commit()
        self.session.close_all()

    def check_poller_updates(self):

#        debug('Checking for errored pollers...')
        # get errors with time_disabled != None and locking_agent == hostname
        errored_pollers = self.session.query(ErrorMgr).\
                filter(ErrorMgr.time_disabled != None).\
                filter(ErrorMgr.locking_agent == gethostname()).\
                all()
        if errored_pollers:
            for p in errored_pollers:
                delta = datetime.utcnow() - p.time_disabled
                # Enabled any pollers that have been disabled for 4 hours.
                if delta.seconds >= (3600 * 4):
                    info('Re-enabling %s poller...' % p.name)
                    p = self.session.query(Poller).filter(Poller.name == p.name).first()
                    p.enabled = True

            self.session.commit()

#        debug('Checking for updated pollers...')
        new_pollers = self.session.query(Poller).filter(Poller.enabled == True).all()
        if new_pollers != self.pollers:
            info('Found updated pollers, restarting poller_mgr...')

            # If adding poller, just restart
            if len(new_pollers) > len(self.pollers):
                debug('Adding new poller...')
                self.pollermgr.stop()
                self.pollermgr.join()

                self.pollers = new_pollers
                for p in self.pollers:
                    self.reset_errors(p.name)
                self.pollermgr = PollerManager(self.pollers, self.transfer_queue, self.process_list, self.__poll_interval)
                self.pollermgr.start()

            # Removing a poller, more complicated
            elif len(new_pollers) < len(self.pollers):
                self.pollermgr.stop()
                self.pollermgr.join()

                bad_pollers = list(set(self.pollers) - set(new_pollers))
                for p in bad_pollers:
                    info('Removing poller: %s' % p.name)
                    del self.transfer_queue[p.name]
                    del self.process_list[p.name]

                    # Cleanup any transfer logs in the database
                    running_transfers = self.session.query(TransferLog).\
                        filter(TransferLog.name==p.name).\
                        filter(TransferLog.status=='Transferring').\
                        order_by('-id').all()

                    for t in running_transfers:
                        t.status = 'Cancelled'
                        t.ended = datetime.utcnow()
                        t.error = 'Cancelled because the poller was disabled.'

                    self.session.commit()

                self.pollers = new_pollers
                self.pollermgr = PollerManager(self.pollers, self.transfer_queue, self.process_list, self.__poll_interval)
                self.pollermgr.start()

    def reset_errors(self, poller_name):
#        debug('Reseting errors on %s poller' % poller_name)
        perror = self.session.query(ErrorMgr).filter(ErrorMgr.name == poller_name).first()
        if perror.total_errors != 0:
            perror.total_errors = 0
            perror.time_disabled = None
            perror.locking_agent = None
            self.session.commit()

    def run_loop(self):
        self.connect_to_db()
        self.start_poller_mgr()

        while True:

            for poller in self.pollers:
#                debug('%s queue: %s' % (poller.name, self.transfer_queue[poller.name]))

                # While number of current processes < max_transfers and the number of elements in the queue are > 0.
                while (len(self.process_list[poller.name]) < poller.max_transfers) and (len(self.transfer_queue[poller.name]) > 0):
                    source = self.transfer_queue[poller.name].pop(0)
                    if source:
                        self.transfer(poller, source)

            # Commit sessions and expire queries and sleep
            self.session.commit()
            time.sleep(2)

            self.check_procs()
            self.check_poller_updates()

    def transfer(self, poller, source):
        info('Transferring %s' % source)
        
        aspera_cmd = ""

        # Build ascp command
        if poller.password:                                                                                                                                 
            aspera_cmd += 'ASPERA_SCP_PASS="%s" ' % str(poller.password).encode('string-escape')

        if poller.encrypt:
            aspera_cmd += 'ASPERA_SCP_FILEPASS=%s ' % str(poller.encrypt_passphrase).encode('string-escape')
        
        aspera_cmd += '/bin/ascp --ignore-host-key -k2 -d -l %sM -m 10K -TQ -P %d ' % (poller.transfer_speed, int(poller.ssh_port))
        
        if poller.ssh_key:
            key_name = os.path.join(self.__ssh_keys, poller.name + '.pub')
            with open(key_name, 'wb') as f:
                f.write(poller.ssh_key)
            aspera_cmd += '-i %s ' % key_name

        if poller.encrypt:
            aspera_cmd += '--file-crypt=encrypt '
        
        aspera_cmd += '--src-base=%s %s %s@%s:/' % (poller.path, source, poller.username, poller.host)
        
        if poller.destination:
            aspera_cmd += '%s/' % poller.destination
        
#        debug('Compiled command: %s' % aspera_cmd)

#        aspera_cmd = 'sleep 2'

        # Update the database
        new_transfer = TransferLog(poller.name, source, 'Transferring', gethostname(), getsize(source))
        self.session.add(new_transfer)
        self.session.commit()

        # Create process and add to process_list
        self.process_list[poller.name].append(ExtendedPopen(poller.name, source, aspera_cmd))

    def check_procs(self):

        def done(p):
            return p.poll() is not None
        def success(p):
            return p.returncode == 0
        def failure():
            pass

#        debug('Checking current processes...')
        for poller, procs in self.process_list.iteritems():
            for p in procs:
                if done(p):
                    res = self.session.query(TransferLog).\
                        filter(TransferLog.name==poller).\
                        filter(TransferLog.filename==p.source).\
                        filter(TransferLog.status=='Transferring').\
                        order_by('-id').first()
                    if success(p):
                        debug('%s for %s was successful' % (p.source, p.name))
                        try:
                            debug('Removing %s' % p.source)
                            if os.path.isfile(p.source):
                                os.remove(p.source)
                            else:
                                shutil.rmtree(p.source)
                        except OSError as err:
                            critical('Error removing %s: %s' % (p.source, str(err)))

                        res.status = 'Complete'
                        res.ended = datetime.utcnow()

                        # Check for error and reset error counter
                        self.reset_errors(p.name)

                    else:
                        warning('%s for %s failed!' % (p.source, p.name))

                        # Re-add to transfer_queue to attempt again
                        self.transfer_queue[poller].append(p.source)

                        stdout = p.stdout.read().strip()
                        stderr = p.stderr.read().strip()

                        res.status = 'Error'
                        res.ended = datetime.utcnow()
                        if stderr:
                            res.error = stderr
                        else:
                            res.error = 'No error given: %s' % str(p.returncode)

                        # Update the error table, disable poller if required
                        perror = self.session.query(ErrorMgr).filter(ErrorMgr.name==p.name).first()
                        perror.total_errors += 1

#                        debug('Total errors: %d' % perror.total_errors)

                        if perror.total_errors >= 5 and not perror.time_disabled:
                            msg = '%s has been disabled for exceeding the maximum amount of errors.' % p.name.upper()
                            msg += '\nThe last transfer errored with:\n\n%s' % stderr
                            send_email(msg)
                            perror.time_disabled = datetime.utcnow()
                            perror.locking_agent = gethostname()
                            updated_poller = self.session.query(Poller).filter(Poller.name == p.name).first()
                            updated_poller.enabled = False
                            info(msg)

                    self.process_list[poller].remove(p)
                    self.session.commit()


class ExtendedPopen(subprocess.Popen):
    """ Extended the subprocess.Popen so I could add some class vars without
    duck punching it. """
    def __init__(self, name, source, cmd):
        self.name = name
        self.source = source
        super(ExtendedPopen, self).__init__(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
