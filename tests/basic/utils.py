from functools import wraps
from typing import Callable, List

from twisted.internet.defer import inlineCallbacks
from twisted.trial import unittest


def skip_for_mongodb_version(
    version_predicate: Callable[[List[int]], bool], message: str
):
    # Can only be used as a method decorator
    # Expects `self.conn` to be set to a txmongo connection

    def decorator(function):
        @wraps(function)
        @inlineCallbacks
        def method_wrapper(self, *args, **kwargs):
            server_status = yield self.conn.admin.command("serverStatus")
            version = [int(part) for part in server_status["version"].split(".")]
            # Strip minor number to allow comparison like `version <= 4.0`
            version = version[:2]
            if version_predicate(version):
                raise unittest.SkipTest(message)

            yield function(self, *args, **kwargs)

        return method_wrapper

    return decorator


def skip_for_mongodb_older_than(version: List[int], message: str):
    return skip_for_mongodb_version(lambda v: v < version, message)


def skip_for_mongodb_newer_than(version: List[int], message: str):
    return skip_for_mongodb_version(lambda v: v > version, message)
