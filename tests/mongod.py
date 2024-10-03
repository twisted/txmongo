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

import os
import random
import shutil
import string
import tempfile
from abc import ABCMeta, abstractmethod
from typing import List, Union

from twisted.internet import defer, reactor
from twisted.internet.error import ProcessDone
from twisted.internet.protocol import ProcessProtocol
from twisted.python import failure

from tests.conf import MongoConf
from txmongo.connection import ConnectionPool


class MongodProcess(ProcessProtocol, metaclass=ABCMeta):
    # FIXME: this message might change in future versions of MongoDB
    # but waiting for this message is faster than pinging tcp port
    # so leaving this for now
    success_messages = [
        b"waiting for connections on port",
        b"Waiting for connections",
    ]

    def __init__(
        self,
        *,
        port=27017,
        auth=False,
        rootCreds=None,
        replset=None,
        dbpath=None,
        args=(),
        tlsCertificateKeyFile=None,
        tlsCAFile=None,
    ):
        self._proc = None
        self._notify_waiting = []
        self._notify_stop = []
        self._output = b""
        self._end_reason = None
        self._configured = False

        self.port = port
        self.auth = auth
        self.rootCreds = rootCreds
        self.replset = replset
        self.dbpath = dbpath
        self.args = args
        self.tlsCertificateKeyFile = tlsCertificateKeyFile
        self.tlsCAFile = tlsCAFile

    @abstractmethod
    def get_run_command(self): ...

    @defer.inlineCallbacks
    def start(self):
        d = defer.Deferred()
        self._notify_waiting.append(d)

        args = yield defer.maybeDeferred(self.get_run_command)

        if self.replset:
            args.extend(["--replSet", self.replset])
        args.extend(self.args)

        args = [arg.encode() for arg in args]

        from os import environ

        self._proc = reactor.spawnProcess(self, args[0], args, env=environ)
        yield d

    def _post_start_configure(self):
        pass

    def stop(self):
        if self._end_reason is None:
            if self._proc and self._proc.pid:
                d = defer.Deferred()
                self._notify_stop.append(d)
                self._proc.signalProcess("INT")
                return d
            else:
                return defer.fail("Not started yet")
        else:
            if self._end_reason.check(ProcessDone):
                return defer.succeed(None)
            else:
                return defer.fail(self._end_reason)

    def kill(self, signal):
        self._proc.signalProcess(signal)

    def process_is_ready(self) -> bool:
        return any(msg in self._output for msg in self.success_messages)

    @defer.inlineCallbacks
    def childDataReceived(self, child_fd: int, data: bytes):
        self._output += data
        if self._configured:
            return

        if not self.process_is_ready():
            return

        self._configured = True
        yield defer.maybeDeferred(self._post_start_configure)
        defs, self._notify_waiting = self._notify_waiting, []
        for d in defs:
            d.callback(None)

    def processEnded(self, reason):
        self._end_reason = reason
        defs, self._notify_stop, self._notify_waiting = (
            self._notify_stop + self._notify_waiting,
            [],
            [],
        )
        for d in defs:
            if reason.check(ProcessDone):
                d.callback(None)
            else:
                d.errback(reason)

    def output(self):
        return self._output


class ProcessOutputCollector(ProcessProtocol):
    def __init__(self, stopDeferred):
        super().__init__()
        self._output = b""
        self._stopDeferred = stopDeferred

    def outReceived(self, data: bytes) -> None:
        self._output += data

    def processExited(self, reason: failure.Failure) -> None:
        self._stopDeferred.callback(self._output)


def run_and_get_output(command: List[Union[str, bytes]]) -> bytes:
    command = [arg.encode() if isinstance(arg, str) else arg for arg in command]
    stopDeferred = defer.Deferred()
    collector = ProcessOutputCollector(stopDeferred)
    reactor.spawnProcess(collector, command[0], command)
    return stopDeferred


class LocalMongod(MongodProcess):
    def __init__(self, *args, **kwargs):
        dbpath = kwargs.pop("dbpath", None)
        if dbpath is None:
            dbpath = tempfile.mkdtemp()
            self._rmdbpath = True
        else:
            self._rmdbpath = False

        super().__init__(*args, **kwargs, dbpath=dbpath)

    @defer.inlineCallbacks
    def get_run_command(self):
        version_output = yield run_and_get_output(["mongod", "--version"])
        mongo40 = b"db version v4.0" in version_output

        trailing_args = []
        if self.auth:
            trailing_args.append("--auth")

        if mongo40:
            if self.tlsCertificateKeyFile:
                trailing_args.extend(["--sslMode", "requireSSL"])
                trailing_args.extend(["--sslPEMKeyFile", self.tlsCertificateKeyFile])
            if self.tlsCAFile:
                trailing_args.extend(["--sslCAFile", self.tlsCAFile])
        else:
            if self.tlsCertificateKeyFile:
                trailing_args.extend(["--tlsMode", "requireTLS"])
                trailing_args.extend(
                    ["--tlsCertificateKeyFile", self.tlsCertificateKeyFile]
                )
            if self.tlsCAFile:
                trailing_args.extend(["--tlsCAFile", self.tlsCAFile])

        return [
            "mongod",
            "--port",
            str(self.port),
            "--dbpath",
            self.dbpath,
            "--oplogSize",
            "1",
            *trailing_args,
        ]

    @defer.inlineCallbacks
    def _post_start_configure(self):
        if self.rootCreds:
            conn = ConnectionPool(f"mongodb://localhost:{self.port}")
            r = yield conn.admin.command(
                "createUser",
                self.rootCreds[0],
                pwd=self.rootCreds[1],
                roles=[{"role": "userAdminAnyDatabase", "db": "admin"}],
            )
            yield conn.disconnect()

    def processEnded(self, reason):
        super().processEnded(reason)

        if self._rmdbpath:
            shutil.rmtree(self.dbpath)


class DockerMongod(MongodProcess):
    mongodb_container_name_prefix = "txmongo-tests-mongodb-"

    def __init__(self, *args, **kwargs):
        self.version = kwargs.pop("version")
        self.network = kwargs.pop("network")
        self.container_name = self.mongodb_container_name_prefix + "".join(
            random.choices(string.ascii_uppercase + string.digits, k=10)
        )
        super().__init__(*args, **kwargs)

    def get_run_command(self):
        mongo40 = self.version.startswith("4.0")

        envs = ["--name", self.container_name]

        if self.network:
            envs.extend(
                [
                    "--network",
                    self.network,
                ]
            )

        if self.rootCreds:
            envs.extend(
                [
                    "-e",
                    f"MONGO_INITDB_ROOT_USERNAME={self.rootCreds[0]}",
                    "-e",
                    f"MONGO_INITDB_ROOT_PASSWORD={self.rootCreds[1]}",
                ]
            )

        trailing_args = []
        if self.auth:
            trailing_args.append("--auth")

        binds = []
        user = []
        if self.dbpath:
            binds.append([self.dbpath, "/data/db"])
            user = ["--user", f"{os.getuid()}:{os.getgid()}"]
        if mongo40:
            if self.tlsCertificateKeyFile:
                trailing_args.extend(["--sslMode", "requireSSL"])
                trailing_args.extend(["--sslPEMKeyFile", self.tlsCertificateKeyFile])
                binds.append([self.tlsCertificateKeyFile, self.tlsCertificateKeyFile])
            if self.tlsCAFile:
                trailing_args.extend(["--sslCAFile", self.tlsCAFile])
                binds.append([self.tlsCAFile, self.tlsCAFile])
        else:
            if self.tlsCertificateKeyFile:
                trailing_args.extend(["--tlsMode", "requireTLS"])
                trailing_args.extend(
                    ["--tlsCertificateKeyFile", self.tlsCertificateKeyFile]
                )
                binds.append([self.tlsCertificateKeyFile, self.tlsCertificateKeyFile])
            if self.tlsCAFile:
                trailing_args.extend(["--tlsCAFile", self.tlsCAFile])
                binds.append([self.tlsCAFile, self.tlsCAFile])

        bind_args = []
        for local, remote in binds:
            bind_args.extend(["-v", f"{local}:{remote}"])

        return [
            "docker",
            "run",
            "--rm",
            "-p",
            f"{self.port}:27017",
            *envs,
            *bind_args,
            *user,
            f"mongo:{self.version}",
            "--oplogSize",
            "1",
            *trailing_args,
        ]

    def process_is_ready(self) -> bool:
        if b"about to fork child process" in self._output:
            for msg in self.success_messages:
                index = self._output.find(msg)
                if index == -1:
                    continue

                return msg in self._output[index + 1 :]
        else:
            return super().process_is_ready()

        return False

    @defer.inlineCallbacks
    def kill(self, signal):
        args = ["docker", "kill", "-s", str(signal), self.container_name]

        yield run_and_get_output(args)


def create_mongod(*args, **kwargs):
    conf = MongoConf()
    if conf.run_in_docker:
        return DockerMongod(version=conf.version, network=conf.network, *args, **kwargs)
    return LocalMongod(*args, **kwargs)
