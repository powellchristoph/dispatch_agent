# pollers.py

import logging
import os
import shutil
import time
import threading

from Dispatch.util import StoppableThread

info = logging.getLogger('pollers').info
debug = logging.getLogger('pollers').debug
warning = logging.getLogger('pollers').warning
critical = logging.getLogger('pollers').critical

class PollerManager(StoppableThread):
    """ The PollerManager creates the given pollers and then periodically calls
    each poller's poll method. """

    def __init__(self, poller_settings, transfer_queue, process_list, poll_interval):
        super(PollerManager, self).__init__()
        self.setDaemon(True)
        self.poll_interval = poll_interval
        self.poller_list = []
        self.create_pollers(poller_settings, transfer_queue, process_list)

    def run(self):
        """ Main run loop. """
        debug('Starting Poller Manager')
        total_sleeps = self.poll_interval / 5
        while True:
            num_of_sleeps = 0
            for poller in self.poller_list:
                poller.poll()
#            debug('Polling complete, sleeping for %s secs...' % self.poll_interval)
            while not self.stopped() and num_of_sleeps < total_sleeps:
                num_of_sleeps += 1
                time.sleep(5)
            if self.stopped():
                break

    def create_pollers(self, poller_settings, transfer_queue, process_list):
        """ Creates pollers from the given settings. Adds then to the transfer_queue
        and the process_list. It will also add/remove pollers from them. """

        for s in poller_settings:
            if s.poller_type in globals().keys():
#                debug("Creating poller: %s" % s.name)
                p = globals()[s.poller_type](s.name, s.path)
                self.poller_list.append(p)

                if s.name not in transfer_queue.keys():
#                    debug('Creating %s queue'% s.name)
                    transfer_queue[s.name] = []
                    process_list[s.name] = []
#                else:
#                    debug('%s queue exists, skipping' % s.name)

            else:
                critical("%s is not a valid poller type." % s.poller_type)                                         
                raise Exception('%s poller does not exist' % s.poller_type)
        PollerBase.set_transfer_queue(transfer_queue)
        PollerBase.set_process_list(process_list)

class PollerBase(object):
    """ Base poller class. All pollers must inherit from this class and override the 
    poll method. Use the validate_and_submit method to queue possible files with the
    TransferManager. """

    transfer_queue = {}
    process_list = {}

    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.debug = logging.getLogger('pollers.%s' % self.name).debug
        self.info = logging.getLogger('pollers.%s' % self.name).info
        self.warning = logging.getLogger('pollers.%s' % self.name).warning

    def poll(self):
        raise Exception('You must overload this function.')

    def validate_and_submit(self, filename):
        """ Check if filename is already in the queue or currently being transferred.
        If not, then it will validate that the file/directory is not actively being 
        written too. If it passes, then it is added to the transfer_queue. """

        matches = [p for p in self.process_list[self.name] if filename == p.source]
        if filename not in self.transfer_queue[self.name] and not matches:
            t = threading.Thread(target=self.is_stable, args=(filename,))
            t.setDaemon(True)
            t.start()
#        else:
#            self.debug('%s is currently in the queue or transferring.' % filename)

    @classmethod
    def set_transfer_queue(cls, transfer_queue):
        cls.transfer_queue = transfer_queue

    @classmethod
    def set_process_list(cls, process_list):
        cls.process_list = process_list

    def is_stable(self, source):
        ''' Checks that the given source is stable and that there is no file system activity. '''

        """ NOTE!! It only checks the contents of the given directory, it will not recursively check 
        sub directories. """

        self.debug("Verifying %s is stable" % source.split('/')[-1])

        if not os.path.exists(source):
            self.warning('%s does not exist.' % source)
            return

        # If source is a file
        if os.path.isfile(source):
#            self.debug('%s is a file' % source.split('/')[-1])
            f = open(source, 'rb')
            f.seek(0,2)
            lb1 = f.tell()
            f.close()

            time.sleep(10)

            f = open(source, 'rb')
            f.seek(0,2)
            lb2 = f.tell()
            f.close()
            if lb1 != lb2:
                return
            else:
                self.debug('Adding %s to the queue' % source)
                self.transfer_queue[self.name].append(source)
                return 

        # If source is a directory
        elif os.path.isdir(source):
#            self.debug('%s is a directory' % source.split('/')[-1])
            file_list = [os.path.join(source, f) for f in os.listdir(source) if os.path.isfile(os.path.join(source, f))]

            # If empty dir, skip
            if not file_list:
                return 

            # Get the last byte of each file in the file_list
            eof = {}
            for file_name in file_list:
                f = open(file_name, 'rb')
                f.seek(0,2)
                eof[file_name] = f.tell()
                f.close()

            time.sleep(10)

            # Return False if any new files were created during our sleep
            second_file_list = [os.path.join(source, f) for f in os.listdir(source) if os.path.isfile(os.path.join(source, f))]
            if file_list != second_file_list:
                return 

            # Return false if the previous last byte of the file does not match the existing last byte
            for file_name in file_list:
                f = open(file_name, 'rb')
                f.seek(0,2)
                lb = f.tell()
                f.close()
                if lb != eof[file_name]:
                    return 
            
            # Everything passed!
            self.debug('Adding %s to the queue' % source)
            self.transfer_queue[self.name].append(source)
            return 

        # Don't know what we got, so quit
        else:
            self.critical('Unknown item %s...skipping.' % source)
            return 

class FilePoller(PollerBase):
    """ A basic poller that scans the given path and transfers the found files. Does not support directories or
    any type of recursion. """

    def poll(self):
        files = [f for f in os.listdir(self.path) if os.path.isfile(os.path.join(self.path, f)) and not f.startswith('.')]
        self.debug('Checking for files...')

        # Check for files
        if files:
            for f in files:
                self.validate_and_submit(os.path.join(self.path, f))
        else:
            self.debug('No files found...')

class DirPoller(PollerBase):
    """ Poller that scans the given path and transfers the found directories. It will initate a transfer               
    once the XML and DTD files exist. Does not support files or any recursion. """

    def poll(self):
        dirs = [d for d in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, d))]                         
        self.debug('Checking for directories...')

        if dirs:
            # For each dir found, transfer if ready
            for d in dirs:
                path = os.path.join(self.path, d)
                files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
                if 'ADI.DTD' in files and 'ADI.XML' in files:
                    self.validate_and_submit(os.path.join(self.path, d))
                else:
                    self.debug('Asset %s not ready' % d)
        else:
            self.debug('No directories found...')

class SubDirPoller(PollerBase):
    """ Poller that scans the given path for subdirectories, and then transfers each file found in the subdirs
    individually while maintaining the directory structure. This will not remove the subdirectories. """

    def poll(self):
        dirs = [d for d in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, d))]
        self.debug('Checking subdirectories...')

        for d in dirs:
            dirpath = os.path.join(self.path, d)

            files = [f for f in os.listdir(dirpath) if os.path.isfile(os.path.join(dirpath, f))]
            if files:
                for f in files:
                    filepath = os.path.join(dirpath, f)
                    self.validate_and_submit(filepath)
            else:
                self.debug('No files found at %s...' % d)

class TelusPoller(PollerBase):
    """ Custom Poller to support Telus. Directories are structured into provider_id/asset_id/<sd or hd>. The dirs
    are searched and files sent retaining this structure. """

    def poll(self):
        # Get all provider IDs
        provider_ids = [d for d in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, d))]
        if not provider_ids:
            self.debug('No providers found.')
            return

        # Scan each provider
        for provider in provider_ids:
            self.debug('Checking provider: %s' % provider)
            provider_path = os.path.join(self.path, provider)

            # Scan SD/HD
            dirs = [d for d in os.listdir(provider_path) if os.path.isdir(os.path.join(provider_path, d))]
            if not dirs:
                self.debug('No subdirs found for %s' % provider)
                continue

            # Scan for files
            for d in dirs:
                dirpath = os.path.join(provider_path, d)
                files = [f for f in os.listdir(dirpath) if os.path.isfile(os.path.join(dirpath, f))]
                if files:
                    for f in files:
                        filepath = os.path.join(dirpath, f)
                        self.validate_and_submit(filepath)
                else:
                    self.debug('No files found at %s/%s...' % (provider, d))

class PAPoller(PollerBase):
    """ A poller to support the provider/asset directory structure. It supports the following                              
    directory structure: /provider_id/asset_id/files. It will send the asset_id directory once
    the ADI.XML and ADI.DTD files exist. """

    def poll(self):
        # Get all provider IDs
        provider_ids = [d for d in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, d))]

        # For each provider
        for provider in provider_ids:
            self.debug('Checking provider: %s' % provider)
            provider_path = os.path.join(self.path, provider)

            # Get all asset_ids for the given provider
            asset_ids = [d for d in os.listdir(provider_path) if os.path.isdir(os.path.join(provider_path, d))]

            # Return if no assets found
            if not asset_ids:
                self.debug('No assets found.')
                continue

            # For each asset, check for files, if files then transfer
            for asset in asset_ids:
                asset_path = os.path.join(provider_path, asset)

                # Get all files for the given asset
                files = [f for f in os.listdir(asset_path) if os.path.isfile(os.path.join(asset_path, f))]
                self.debug('Checking asset %s...' % asset)

                # Check for ADI.XML, if found transfer it and remove the asset_id directory
                if 'ADI.XML' in files and 'ADI.DTD' in files:
                    self.debug('Found XML/DTD in %s' % asset)
                    self.validate_and_submit(asset_path)
                else:
                    self.debug('Asset %s not ready...' % asset)

class GooglePoller(PollerBase):
    """ Google Poller """

    def poll(self):
        dirs = [d for d in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, d))]                         
        self.debug('Checking for directories...')

        for d in dirs:
            dirpath = os.path.join(self.path, d)

            files = [f for f in os.listdir(dirpath) if os.path.isfile(os.path.join(dirpath, f)) and not f.startswith('.')]

            # If only the dispatch.done exists, remove asset dir
            if len(files) == 1 and 'dispatch.done' in files:
                self.debug('Found %s/dispatch.done, removing dir...' % d)
                try:
                    shutil.rmtree(dirpath)
                except Exception, e:
                    self.warning('Unable to remove asset dir %s' % d)
                    self.warning(str(e))

            # If only the delivery.complete exists, send it and create dispatch.done
            elif len(files) ==1 and 'delivery.complete' in files:
                self.debug('Found %s/delivery.complete, sending...' % d)
                self.validate_and_submit(os.path.join(dirpath, 'delivery.complete'))
                self.debug('Creating %s/dispatch.done' % d)
                open(os.path.join(dirpath, 'dispatch.done'), 'a').close()

            # If entire asset exists, send it and create delivery.complete
            elif 'ADI.XML' in files and 'ADI.DTD' in files and not 'delivery.complete' in files:
                self.debug('Found asset %s...' % d)
                for f in files:
                    self.validate_and_submit(os.path.join(dirpath, f))
                self.debug('Creating %s/delivery.complete' % d)
                open(os.path.join(dirpath, 'delivery.complete'), 'a').close()

            else:
                self.debug('Asset %s not ready...' % d)

class DirTarPoller(PollerBase):
    """ Poller that will search for asset subdirectories and then send the single .tar file that exists. """

    def poll(self):
        dirs = [d for d in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, d))]                         
        self.debug('Checking for directories...')

        if dirs:
            # For each dir found, transfer if ready
            for d in dirs:
                path = os.path.join(self.path, d)
                files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and f.endswith('.tar')]
                if files:
                    for f in files:
                        self.validate_and_submit(os.path.join(path, f))
        else:
            self.debug('No directories found...')
