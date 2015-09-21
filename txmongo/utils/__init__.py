from functools import wraps
from time import time
from txmongo.errors import TimeExceeded
from twisted.internet import defer, reactor


def timeout(func):
    """Decorator to add timeout to Deferred calls"""

    @wraps(func)
    @defer.inlineCallbacks
    def _timeout(*args, **kwargs):
        deadline = kwargs.pop("deadline", None)
        seconds = kwargs.pop("timeout", None)

        if seconds is None and deadline is not None:
            seconds = deadline - time()

        if seconds is not None and seconds < 0:
            raise TimeExceeded("Run time of {0}s exceeded.".format(seconds))

        raw_d = func(*args, **kwargs)

        if seconds is None:
            raw_result = yield raw_d
            defer.returnValue(raw_result)

        timeout_d = defer.Deferred()
        times_up = reactor.callLater(seconds, timeout_d.callback, None)

        try:
            raw_result, timeout_result = yield defer.DeferredList(
                [raw_d, timeout_d], fireOnOneCallback=True, fireOnOneErrback=True,
                consumeErrors=True)
        except defer.FirstError as e:  # Only raw_d should raise an exception
            assert e.index == 0
            times_up.cancel()
            e.subFailure.raiseException()
        else:  # timeout
            if timeout_d.called:
                raw_d.cancel()
                raise TimeExceeded("Run time of {0}s exceeded.".format(seconds))

        # no timeout
        times_up.cancel()
        defer.returnValue(raw_result)
    return _timeout
