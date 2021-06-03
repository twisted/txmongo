from functools import wraps
from time import time
from txmongo.errors import TimeExceeded
from twisted.internet import defer, reactor


def timeout(func):
    """Decorator to add timeout to Deferred calls"""

    @wraps(func)
    def _timeout(*args, **kwargs):
        now = time()
        deadline = kwargs.pop("deadline", None)
        seconds = kwargs.pop("timeout", None)

        if deadline is None and seconds is not None:
            deadline = now + seconds

        if deadline is not None and deadline < now:
            raise TimeExceeded("TxMongo: run time exceeded by {0}s.".format(now-deadline))

        kwargs['_deadline'] = deadline
        raw_d = func(*args, **kwargs)

        if deadline is None:
            return raw_d

        if seconds is None and deadline is not None and deadline - now > 0:
            seconds = deadline - now

        timeout_d = defer.Deferred()
        times_up = reactor.callLater(seconds, timeout_d.callback, None)

        def on_ok(result):
            if timeout_d.called:
                raw_d.cancel()
                raise TimeExceeded("TxMongo: run time of {0}s exceeded.".format(seconds))
            else:
                times_up.cancel()
                return result[0]

        def on_fail(failure):
            failure.trap(defer.FirstError)
            assert failure.value.index == 0
            times_up.cancel()
            failure.value.subFailure.raiseException()


        return defer.DeferredList([raw_d, timeout_d], fireOnOneCallback=True,
                                  fireOnOneErrback=True, consumeErrors=True).addCallbacks(on_ok, on_fail)

    return _timeout



def check_deadline(_deadline):
    if _deadline is not None and _deadline < time():
        raise TimeExceeded("TxMongo: now '{0}', deadline '{1}'".format(time(), _deadline))


def get_err(document, default=None):
    err = document.get("err", None) or document.get("codeName", None)
    errmsg = document.get("errmsg", None)
    return ": ".join(filter(None, (err, errmsg))) or default
