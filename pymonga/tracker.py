# coding: utf-8
# Copyright 2009 Alexandre Fiori
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

from pymonga.objects import Database
from twisted.internet.defer import Deferred

class disconnected(object):
    def error(self):
        d = Deferred()
        d.errback(RuntimeError("not connected"))
        return d

    def _OP_INSERT(self, *args, **kwargs):
        return self.error()

    def _OP_UPDATE(self, *args, **kwargs):
        return self.error()

    def _OP_REMOVE(self, *args, **kwargs):
        return self.error()

    def _OP_QUERY(self, *args, **kwargs):
        return self.error()


class Tracker(object):
    def __init__(self):
        self.idx = 0
        self.size = 0
        self.pool = []
        self.disconnected = disconnected()

    def __call__(self):
        try:
            conn = self.pool[self.idx]
            if conn.connected == 1:
                self.idx = (self.idx + 1) % self.size
                return conn
            else:
                self.remove(conn)
                return self()
        except:
            return self.disconnected

    def disconnect(self):
        for idx in range(len(self.pool)):
            try:
                conn = self.pool.pop(0)
                conn.factory.continueTrying = 0
                conn.transport.loseConnection()
                self.size -= 1
            except:
                pass
        
    def append(self, proto):
        self.pool.append(proto)
        self.size += 1

    def remove(self, proto):
        self.pool.remove(proto)
        self.size -= 1

    def __repr__(self):
        try:
            cli = self.pool[0].transport.getPeer()
            info = "%s:%s - %d connection(s)" % (cli.host, cli.port, self.size)
        except:
            info = "not connected"

        return "<mongodb Connection: %s>" % info

    def __getitem__(self, database_name):
        return Database(self, database_name)

    def __getattr__(self, database_name):
        return Database(self, database_name)
