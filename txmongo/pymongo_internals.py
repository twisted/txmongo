from pymongo.bulk import _UOP
from pymongo.errors import (DuplicateKeyError, WriteError, WTimeoutError, WriteConcernError, OperationFailure,
                            NotMasterError, ExecutionTimeout, CursorNotFound)
from pymongo.message import _INSERT, _DELETE, _UPDATE
from six import itervalues


# Copied from pymongo/helpers.py:32 at commit d7d94b2776098dba32686ddf3ada1f201172daaf

# From the SDAM spec, the "node is shutting down" codes.
_SHUTDOWN_CODES = frozenset([
    11600,  # InterruptedAtShutdown
    91,     # ShutdownInProgress
])
# From the SDAM spec, the "not master" error codes are combined with the
# "node is recovering" error codes (of which the "node is shutting down"
# errors are a subset).
_NOT_MASTER_CODES = frozenset([
    10107,  # NotMaster
    13435,  # NotMasterNoSlaveOk
    11602,  # InterruptedDueToReplStateChange
    13436,  # NotMasterOrSecondary
    189,    # PrimarySteppedDown
]) | _SHUTDOWN_CODES
# From the retryable writes spec.
_RETRYABLE_ERROR_CODES = _NOT_MASTER_CODES | frozenset([
    7,     # HostNotFound
    6,     # HostUnreachable
    89,    # NetworkTimeout
    9001,  # SocketException
])


# Copied from pymongo/helpers.py:193 at commit 47b0d8ebfd6cefca80c1e4521b47aec7cf8f529d
def _raise_last_write_error(write_errors):
    # If the last batch had multiple errors only report
    # the last error to emulate continue_on_error.
    error = write_errors[-1]
    if error.get("code") == 11000:
        raise DuplicateKeyError(error.get("errmsg"), 11000, error)
    raise WriteError(error.get("errmsg"), error.get("code"), error)


# Copied from pymongo/helpers.py:202 at commit 47b0d8ebfd6cefca80c1e4521b47aec7cf8f529d
def _raise_write_concern_error(error):
    if "errInfo" in error and error["errInfo"].get('wtimeout'):
        # Make sure we raise WTimeoutError
        raise WTimeoutError(
            error.get("errmsg"), error.get("code"), error)
    raise WriteConcernError(
        error.get("errmsg"), error.get("code"), error)


# Copied from pymongo/helpers.py:211 at commit 47b0d8ebfd6cefca80c1e4521b47aec7cf8f529d
def _check_write_command_response(result):
    """Backward compatibility helper for write command error handling.
    """
    # Prefer write errors over write concern errors
    write_errors = result.get("writeErrors")
    if write_errors:
        _raise_last_write_error(write_errors)

    error = result.get("writeConcernError")
    if error:
        _raise_write_concern_error(error)


# Copied from pymongo/bulk.py:93 at commit 96aaf2f5279fb9eee5d0c1a2ce53d243b2772eee
def _merge_command(run, full_result, results):
    """Merge a group of results from write commands into the full result.
    """
    for offset, result in results:

        affected = result.get("n", 0)

        if run.op_type == _INSERT:
            full_result["nInserted"] += affected

        elif run.op_type == _DELETE:
            full_result["nRemoved"] += affected

        elif run.op_type == _UPDATE:
            upserted = result.get("upserted")
            if upserted:
                n_upserted = len(upserted)
                for doc in upserted:
                    doc["index"] = run.index(doc["index"] + offset)
                full_result["upserted"].extend(upserted)
                full_result["nUpserted"] += n_upserted
                full_result["nMatched"] += (affected - n_upserted)
            else:
                full_result["nMatched"] += affected
            full_result["nModified"] += result["nModified"]

        write_errors = result.get("writeErrors")
        if write_errors:
            for doc in write_errors:
                # Leave the server response intact for APM.
                replacement = doc.copy()
                idx = doc["index"] + offset
                replacement["index"] = run.index(idx)
                # Add the failed operation to the error document.
                replacement[_UOP] = run.ops[idx]
                full_result["writeErrors"].append(replacement)

        wc_error = result.get("writeConcernError")
        if wc_error:
            full_result["writeConcernErrors"].append(wc_error)


# Copied from pymongo/helpers.py:105 at commit d7d94b2776098dba32686ddf3ada1f201172daaf
def _check_command_response(response, msg=None, allowable_errors=None,
                            parse_write_concern_error=False):
    """Check the response to a command for errors.
    """
    if "ok" not in response:
        # Server didn't recognize our message as a command.
        raise OperationFailure(response.get("$err"),
                               response.get("code"),
                               response)

    if parse_write_concern_error and 'writeConcernError' in response:
        _raise_write_concern_error(response['writeConcernError'])

    if not response["ok"]:

        details = response
        # Mongos returns the error details in a 'raw' object
        # for some errors.
        if "raw" in response:
            for shard in itervalues(response["raw"]):
                # Grab the first non-empty raw error from a shard.
                if shard.get("errmsg") and not shard.get("ok"):
                    details = shard
                    break

        errmsg = details["errmsg"]
        if allowable_errors is None or errmsg not in allowable_errors:

            code = details.get("code")
            # Server is "not master" or "recovering"
            if code in _NOT_MASTER_CODES:
                raise NotMasterError(errmsg, response)
            elif ("not master" in errmsg
                  or "node is recovering" in errmsg):
                raise NotMasterError(errmsg, response)

            # Server assertion failures
            if errmsg == "db assertion failure":
                errmsg = ("db assertion failure, assertion: '%s'" %
                          details.get("assertion", ""))
                raise OperationFailure(errmsg,
                                       details.get("assertionCode"),
                                       response)

            # Other errors
            # findAndModify with upsert can raise duplicate key error
            if code in (11000, 11001, 12582):
                raise DuplicateKeyError(errmsg, code, response)
            elif code == 50:
                raise ExecutionTimeout(errmsg, code, response)
            elif code == 43:
                raise CursorNotFound(errmsg, code, response)

            msg = msg or "%s"
            raise OperationFailure(msg % errmsg, code, response)
