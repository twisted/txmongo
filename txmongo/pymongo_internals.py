from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, NoReturn, Optional

from pymongo.errors import (
    CursorNotFound,
    DuplicateKeyError,
    ExecutionTimeout,
    NotPrimaryError,
    OperationFailure,
    WriteConcernError,
    WriteError,
    WTimeoutError,
)

from txmongo._bulk_constants import _DELETE, _INSERT, _UPDATE
from txmongo.pymongo_errors import _NOT_MASTER_CODES

if TYPE_CHECKING:
    from txmongo._bulk import _Run


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
    if "errInfo" in error and error["errInfo"].get("wtimeout"):
        # Make sure we raise WTimeoutError
        raise WTimeoutError(error.get("errmsg"), error.get("code"), error)
    raise WriteConcernError(error.get("errmsg"), error.get("code"), error)


# Copied from pymongo/helpers.py:211 at commit 47b0d8ebfd6cefca80c1e4521b47aec7cf8f529d
def _check_write_command_response(result):
    """Backward compatibility helper for write command error handling."""
    # Prefer write errors over write concern errors
    write_errors = result.get("writeErrors")
    if write_errors:
        _raise_last_write_error(write_errors)

    error = result.get("writeConcernError")
    if error:
        _raise_write_concern_error(error)


# Copied from pymongo/helpers_shared.py:266 at commit e03f8f24f2387882fcaa5d3099d2cef7ae100816
def _get_wce_doc(result: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    """Return the writeConcernError or None."""
    wce = result.get("writeConcernError")
    if wce:
        # The server reports errorLabels at the top level but it's more
        # convenient to attach it to the writeConcernError doc itself.
        error_labels = result.get("errorLabels")
        if error_labels:
            # Copy to avoid changing the original document.
            wce = wce.copy()
            wce["errorLabels"] = error_labels
    return wce


# Copied from pymongo/bulk_shared.py:72 at commit e03f8f24f2387882fcaa5d3099d2cef7ae100816
def _merge_command(
    run: _Run,
    full_result: MutableMapping[str, Any],
    offset: int,
    result: Mapping[str, Any],
) -> None:
    """Merge a write command result into the full bulk result."""
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
            full_result["nMatched"] += affected - n_upserted
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
            replacement["op"] = run.ops[idx]
            full_result["writeErrors"].append(replacement)

    wce = _get_wce_doc(result)
    if wce:
        full_result["writeConcernErrors"].append(wce)


# Copied from pymongo/helpers.py:105 at commit d7d94b2776098dba32686ddf3ada1f201172daaf
def _check_command_response(
    response, allowable_errors=None, parse_write_concern_error=False
):
    """Check the response to a command for errors."""
    if "ok" not in response:
        # Server didn't recognize our message as a command.
        raise OperationFailure(response.get("$err"), response.get("code"), response)

    if parse_write_concern_error and "writeConcernError" in response:
        _raise_write_concern_error(response["writeConcernError"])

    if not response["ok"]:

        details = response
        # Mongos returns the error details in a 'raw' object
        # for some errors.
        if "raw" in response:
            for shard in response["raw"].values():
                # Grab the first non-empty raw error from a shard.
                if shard.get("errmsg") and not shard.get("ok"):
                    details = shard
                    break

        errmsg = details["errmsg"]
        if allowable_errors is None or errmsg not in allowable_errors:

            code = details.get("code")
            # Server is "not master" or "recovering"
            if code in _NOT_MASTER_CODES:
                raise NotPrimaryError(errmsg, response)
            elif "not master" in errmsg or "node is recovering" in errmsg:
                raise NotPrimaryError(errmsg, response)

            # Server assertion failures
            if errmsg == "db assertion failure":
                errmsg = "db assertion failure, assertion: '%s'" % details.get(
                    "assertion", ""
                )
                raise OperationFailure(errmsg, details.get("assertionCode"), response)

            # Other errors
            # findAndModify with upsert can raise duplicate key error
            if code in (11000, 11001, 12582):
                raise DuplicateKeyError(errmsg, code, response)
            elif code == 50:
                raise ExecutionTimeout(errmsg, code, response)
            elif code == 43:
                raise CursorNotFound(errmsg, code, response)

            raise OperationFailure(errmsg, code, response)


def _reraise_with_unknown_commit(exc: Any) -> NoReturn:
    """Re-raise an exception with the UnknownTransactionCommitResult label."""
    exc._add_error_label("UnknownTransactionCommitResult")
    raise
