Dispatch Agent
==============

Dispatch is an automated system which monitors directories for files and executes a transfers when certain criteria are met. Currently, it only supports Aspera file transfers but it could be extended with FTP, SFTP or WebDav support. To create a custom poller, inherit from the PollerBase class and override the poll method.

Current pollers:

* FilePoller - A basic poller that scans the given path and transfers the found files. Will not send directories.
* DirPoller - A poller that scans the given path and transfers the found directories. It will initate a transfer once the XML and DTD files exist.
* SubDirPoller - A poller that scans the given path for subdirectories and then transfers each file found in the subdirs individually while maintaining the directory structure. This will not remove the subdirectories.
* TelusPoller - A specialty poller to support Telus. This scans provider_id/asset_id/{hd|sd} for files.
* PAPoller - A poller to support the provider/asset directory structure. It supports the following directory structure: /provider_id/asset_id/files. It will send the asset_id directory once the ADI.XML and ADI.DTD files exist.
* GooglePoller - A specialty poller built to support Google. It is the same as the Dir Poller, but once it has completely sent the directory, it will send a blank file called delivery.complete.
* DirTarPoller - Scans for tar files with in the found subdirectories.

Dispatch Agent is the stateless agent which actually monitors the directories and initiates the transfers. You can run and agent on the same server as Dispatch web, or scale out to multiple nodes.

[Dispatch Web](https://github.com/powellchristoph/dispatch_web) is a Django web application that is the central web interface for controlling Dispatch agents. All configuration for the agents is stored in the local database. Agents poll for config changes and restart as neccessary. Logging and searching are provided on the interface as all transfers from the agents are logged centrally.
