from twisted.internet import defer, reactor
from twisted.internet.error import ProcessDone


class Mongod(object):

    # FIXME: this message might change in future versions of MongoDB
    # but waiting for this message is faster than pinging tcp port
    # so leaving this for now
    success_message = "waiting for connections on port"

    def __init__(self, dbpath, port=27017, auth=False):
        self.__proc = None
        self.__notify_waiting = []
        self.__notify_stop = []
        self.__output = ''
        self.__end_reason = None

        self.dbpath = dbpath
        self.port = port
        self.auth = auth

    def start(self):
        d = defer.Deferred()
        self.__notify_waiting.append(d)

        args = ["mongod",
                "--port", str(self.port),
                "--dbpath", str(self.dbpath),
                "--noprealloc", "--nojournal",
                "--smallfiles", "--nssize", "1",
                "--nohttpinterface",
                ]

        if self.auth: args.append("--auth")

        self.__proc = reactor.spawnProcess(self, "mongod", args)
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
