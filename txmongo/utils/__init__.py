from functools import wraps
from time import time
from txmongo.errors import TimeExceeded
from twisted.internet import defer, reactor


def timeout(func):
    """Decorator to add timeout to Deferred calls"""

    @wraps(func)
    @defer.inlineCallbacks
    def _timeout(*args, **kwargs):
        now = time()
        deadline = kwargs.pop("deadline", None)
        seconds = kwargs.pop("timeout", None)

        if deadline is None and seconds is not None:
            deadline = now + seconds

        if deadline is not None and deadline < now:
            raise TimeExceeded("Run time exceeded by {0}s.".format(now-deadline))

        kwargs['_deadline'] = deadline
        raw_d = func(*args, **kwargs)

        if deadline is None:
            raw_result = yield raw_d
            defer.returnValue(raw_result)

        if seconds is None and deadline is not None and deadline - now > 0:
            seconds = deadline - now

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


def check_deadline(_deadline):
    if _deadline is not None and _deadline < time():
        raise TimeExceeded("Now: {0} Deadline: {1}".format(time(), _deadline))
