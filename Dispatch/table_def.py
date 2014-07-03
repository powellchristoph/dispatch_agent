# table_def.py

import time

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float, Boolean, BigInteger
from sqlalchemy.ext.declarative import declarative_base

from datetime import datetime

Base = declarative_base()

class Poller(Base):

    __tablename__ = 'dispatch_web_poller'

    id = Column(Integer, primary_key=True)
    name = Column(String(200))
    path = Column(String(200))
    host = Column(String(200))
    username = Column(String(200))
    password = Column(String(200))
    ssh_key = Column(Text)
    poller_type = Column(String(200))
    transfer_speed = Column(Integer)
    excludes = Column(String(200))
    ssh_port = Column(Integer)
    destination = Column(String(250))
    encrypt = Column(String(5))
    encrypt_passphrase = Column(String(250))
    max_transfers = Column(Integer)
    enabled = Column(Boolean())

class TransferLog(Base):

    __tablename__ = 'dispatch_web_transferlog'

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    filename = Column(String(250))
    status = Column(String(250), default='Transferring') # pending, transferring, completed, error
    started = Column(DateTime, default=datetime.utcnow)
    ended = Column(DateTime)
    error = Column(Text)
    server = Column(String(250))
    filesize = Column(BigInteger)

    def __init__(self, name, filename, state, server, filesize):
        self.name = name
        self.filename = filename
        self.state = state
        self.server = server
        self.filesize = filesize

class ErrorMgr(Base):

    __tablename__ = 'dispatch_web_errormgr'

    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    total_errors = Column(Integer, default=0)
    time_disabled = Column(DateTime, default=None)
    locking_agent = Column(String(50))
