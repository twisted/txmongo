# coding: utf-8
# Copyright 2015 Ilya Skriblovsky <ilyaskriblovsky@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division

import tempfile
import shutil

from twisted.python.filepath import FilePath
from twisted.internet import defer, reactor
from twisted.internet.error import ProcessDone


class Mongod(object):

    # FIXME: this message might change in future versions of MongoDB
    # but waiting for this message is faster than pinging tcp port
    # so leaving this for now
    success_message = b"waiting for connections on port"

    def __init__(self, port=27017, auth=False, replset=None, dbpath=None, args=()):
        self.__proc = None
        self.__notify_waiting = []
        self.__notify_stop = []
        self.__output = b''
        self.__end_reason = None

        self.__datadir = None

        self.port = port
        self.auth = auth
        self.replset = replset
        self.args = args

        if dbpath is None:
            self.__datadir = tempfile.mkdtemp()
            self.__rmdatadir = True
        else:
            self.__datadir = dbpath
            self.__rmdatadir = False

        # Ensure it is always bytes
        self.__datadir = FilePath(self.__datadir).asBytesMode().path


    def start(self):
        d = defer.Deferred()
        self.__notify_waiting.append(d)

        args = [b"mongod",
                b"--port", (b'%d' % self.port),
                b"--dbpath", self.__datadir,
                # MongoDB 4.0 doesn't support nojournal + WiredTiger + ReplicaSet
                # b"--nojournal",
                # MongoDB 4.2 doesn't support MMAPv1 and so --noprealloc and --smallfiles
                # b"--noprealloc",
                # b"--smallfiles",
                # b"--nssize", b"1",
                b"--oplogSize", b"1",
        ]
        if self.auth: args.append(b"--auth")
        if self.replset: args.extend([b"--replSet", self.replset])
        args.extend(arg.encode() for arg in self.args)
        from os import environ
        self.__proc = reactor.spawnProcess(self, b"mongod", args, env=environ)
        return d

    def stop(self):
        if self.__end_reason is None:
            if self.__proc and self.__proc.pid:
                d = defer.Deferred()
                self.__notify_stop.append(d)
                self.__proc.signalProcess("INT")
                return d
            else:
                return defer.fail("Not started yet")
        else:
            if self.__end_reason.check(ProcessDone):
                return defer.succeed(None)
            else:
                return defer.fail(self.__end_reason)

    def kill(self, signal):
        self.__proc.signalProcess(signal)

    def makeConnection(self, process): pass
    def childConnectionLost(self, child_fd): pass
    def processExited(self, reason): pass

    def childDataReceived(self, child_fd, data):
        self.__output += data
        if self.success_message in self.__output:
            defs, self.__notify_waiting = self.__notify_waiting, []
            for d in defs:
                d.callback(None)

    def processEnded(self, reason):
        self.__end_reason = reason
        defs, self.__notify_stop, self.__notify_waiting = self.__notify_stop + self.__notify_waiting, [], []
        for d in defs:
            if reason.check(ProcessDone):
                d.callback(None)
            else:
                d.errback(reason)

        if self.__rmdatadir:
            shutil.rmtree(self.__datadir)


    def output(self): return self.__output
