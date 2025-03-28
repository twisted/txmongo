from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING, AsyncContextManager, Optional, Type

import bson
from bson import Int64, UuidRepresentation
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument
from pymongo import WriteConcern
from pymongo.errors import (
    BulkWriteError,
    ConfigurationError,
    ConnectionFailure,
    InvalidOperation,
    NotPrimaryError,
    OperationFailure,
    PyMongoError,
)

from txmongo.pymongo_errors import _UNKNOWN_COMMIT_ERROR_CODES
from txmongo.pymongo_internals import _reraise_with_unknown_commit

if TYPE_CHECKING:
    from txmongo.connection import ConnectionPool
    from txmongo.types import Document


__all__ = [
    "ClientSession",
    "TransactionOptions",
]

_codec_options = DEFAULT_RAW_BSON_OPTIONS.with_options(
    uuid_representation=UuidRepresentation.STANDARD
)


class TxnState(Enum):
    NONE = 1
    STARTING = 2
    IN_PROGRESS = 3
    COMMITTED = 4
    COMMITTED_EMPTY = 5
    ABORTED = 6


@dataclass
class ServerSession:
    session_id: RawBSONDocument
    last_use: float

    _transaction_id: int = 0
    _is_dirty: bool = False

    @classmethod
    def create_with_local_id(cls) -> ServerSession:
        return cls(
            RawBSONDocument(
                bson.encode({"id": uuid.uuid4()}, codec_options=_codec_options),
                codec_options=_codec_options,
            ),
            time.monotonic(),
        )

    def is_about_to_expire(self, timeout_minutes: float) -> bool:
        return time.monotonic() - self.last_use > (timeout_minutes - 1) * 60

    def update_last_use(self) -> None:
        self.last_use = time.monotonic()

    @property
    def is_dirty(self) -> bool:
        return self._is_dirty

    def mark_dirty(self) -> None:
        self._is_dirty = True

    @property
    def transaction_id(self) -> int:
        return Int64(self._transaction_id)

    def inc_transaction_id(self) -> None:
        self._transaction_id += 1


@dataclass(frozen=True)
class SessionOptions:
    default_transaction_options: Optional[TransactionOptions] = None

    def __post_init__(self):
        if self.default_transaction_options is not None:
            if not isinstance(self.default_transaction_options, TransactionOptions):
                raise TypeError(
                    f"default_transaction_options must be an instance of"
                    f" txmongo.sessions.TransactionOptions, not: {self.default_transaction_options!r}"
                )


@dataclass(frozen=True)
class TransactionOptions:
    """Options for :meth:`ClientSession.start_transaction`.

    :param write_concern:
        The write concern to use for the transaction. If not provided, the write concern of the
        connection will be used.

    :param max_commit_time_ms:
        The maximum amount of time to allow a single commitTransaction command to run.
        This is an alias for the `maxTimeMS` option in the commitTransaction command.
        if `None` (the default), `maxTimeMS` is not used.
    """

    write_concern: Optional[WriteConcern] = None
    max_commit_time_ms: Optional[int] = None

    def __post_init__(self):
        if self.write_concern:
            if not isinstance(self.write_concern, WriteConcern):
                raise TypeError(
                    f"write_concern must be an instance of pymongo.write_concern.WriteConcern, not: {self.write_concern!r}"
                )
            if not self.write_concern.acknowledged:
                raise ConfigurationError(
                    f"transactions do not support unacknowledged write concern: {self.write_concern!r}"
                )

        if self.max_commit_time_ms is not None:
            if not isinstance(self.max_commit_time_ms, int):
                raise TypeError("max_commit_time_ms must be an integer or None")


class _TransactionContext:
    """Internal transaction context manager for start_transaction."""

    def __init__(self, session: ClientSession) -> None:
        self.__session = session

    async def __aenter__(self) -> _TransactionContext:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.__session.in_transaction():
            if exc_val is None:
                await self.__session.commit_transaction()
            else:
                await self.__session.abort_transaction()


class ClientSession:
    """
    A session for ordering sequential operations.

    :class:`ClientSession` instances should not be instantiated directly. Instead, use
    :meth:`ConnectionPool.start_session` to create a session.

    :class:`ClientSession` can be used as async context manager. When used as a context manager,
    it's :meth:`end_session()` method will be called automatically when exiting the context:
    ::
        async with conn.start_session() as session:
            record = await conn.db.coll.find_one(..., session=session)
            await conn.db.coll.update_one(..., session=session)

    If you are not using :class:`ClientSession` with `async with`, you need to end the session explicitly:
    ::
        session = conn.start_session()
        ...
        await session.end_session()
    """

    _implicit: bool = False

    _connection: ConnectionPool
    options: SessionOptions

    _server_session: ServerSession | None = None
    _is_ended: bool = False

    _cluster_time: Optional[Document] = None

    _txn_state: TxnState = TxnState.NONE
    _txn_options: TransactionOptions = TransactionOptions()

    def __init__(
        self,
        connection: ConnectionPool,
        options: Optional[SessionOptions],
        *,
        implicit: bool,
    ):
        self._connection = connection
        if options is None:
            options = SessionOptions()
        self.options = options
        self._implicit = implicit

    async def __aenter__(self) -> ClientSession:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.end_session()

    @property
    def connection(self) -> ConnectionPool:
        """The :class:`ConnectionPool` that this session was created from."""
        return self._connection

    @property
    def implicit(self) -> bool:
        """If this session is implicit. Application developers usually don't see implicit session objects."""
        return self._implicit

    @property
    def is_ended(self) -> bool:
        """If this session is ended. :class:`ClientSession` objects cannot be reused after they are ended."""
        return self._is_ended

    @property
    def session_id(self) -> RawBSONDocument:
        """A BSON document, the opaque server session identifier."""
        return self._materialize_server_session().session_id

    def _materialize_server_session(self) -> ServerSession:
        if self._is_ended:
            raise ValueError("Cannot use an ended session")

        if not self._server_session:
            self._server_session = self.connection._acquire_server_session()

        return self._server_session

    def _use_session_id(self) -> RawBSONDocument:
        session_id = self._materialize_server_session().session_id
        self._server_session.update_last_use()
        return session_id

    async def end_session(self) -> None:
        """Finish this session. If a transaction has started, abort it.

        It is an error to use the session after the session has ended.
        """
        try:
            if self._server_session is None:
                return

            if self.in_transaction():
                await self.abort_transaction()

            self.connection._return_server_session(self._server_session)
        finally:
            self._server_session = None
            self._is_ended = True

    def _mark_dirty(self) -> None:
        if self._server_session:
            self._server_session.mark_dirty()

    @property
    def cluster_time(self) -> Optional[Document]:
        """The cluster time returned by the last operation executed in this session."""
        return self._cluster_time

    def advance_cluster_time(self, cluster_time: Document) -> None:
        """Update the cluster time for this session.

        :param cluster_time: The
            :data:`ClientSession.cluster_time` from another `ClientSession` instance.
        """
        if self._cluster_time is None:
            self._cluster_time = cluster_time
        elif self._cluster_time["clusterTime"] < cluster_time["clusterTime"]:
            self._cluster_time = cluster_time

    def _check_ended(self):
        if self._is_ended:
            raise InvalidOperation("Cannot use ended session")

    def in_transaction(self) -> bool:
        """True if this session has an active multi-statement transaction."""
        return self._txn_state in {TxnState.STARTING, TxnState.IN_PROGRESS}

    def start_transaction(
        self, *, write_concern: WriteConcern = None, max_commit_time_ms: int = None
    ) -> AsyncContextManager:
        """Start a multi-statement transaction.

        When used as an async context manager, automatically commits the transaction if
        no exception is raised, and automatically aborts the transaction in case of exception.
        ::
            async with conn.start_session() as session:
                async with session.start_transaction():
                    await conn.db.coll.insert_one({"x": 1}, session=session)

        Can also be used without `async with`. In this case you must call `commit_transaction` or
        `abort_transaction` explicitly.
        ::
            session = conn.start_session()
            session.start_transaction()
            await conn.db.coll.insert_one({"x": 1}, session=session)
            await session.commit_transaction()
            await session.end_session()

        :param write_concern:
            If provided, this write concern will be used for `commitTransaction` and `abortTransaction` commands.

        :param max_commit_time_ms:
            The maximum amount of time to allow a single commitTransaction command to run.
        """
        self._check_ended()

        if self.in_transaction():
            raise InvalidOperation("Transaction already in progress")

        default_opts = self.options.default_transaction_options or TransactionOptions()

        # Note: ↓ this may raise due to validation
        self._txn_options = TransactionOptions(
            write_concern=write_concern or default_opts.write_concern,
            max_commit_time_ms=max_commit_time_ms or default_opts.max_commit_time_ms,
        )
        self._txn_state = TxnState.STARTING
        self._materialize_server_session()
        self._server_session.inc_transaction_id()
        return _TransactionContext(self)

    async def commit_transaction(self) -> None:
        """Commit a multi-statement transaction."""

        self._check_ended()

        if self._txn_state == TxnState.NONE:
            raise InvalidOperation("No transaction started")
        elif self._txn_state in {TxnState.STARTING, TxnState.COMMITTED_EMPTY}:
            self._txn_state = TxnState.COMMITTED_EMPTY
            return
        elif self._txn_state == TxnState.ABORTED:
            raise InvalidOperation(
                "Cannot call commitTransaction after calling abortTransaction"
            )
        elif self._txn_state == TxnState.COMMITTED:
            # We're explicitly retrying the commit, move the state back to
            # "in progress" so that in_transaction returns true.
            self._txn_state = TxnState.IN_PROGRESS

        try:
            await self._finish_transaction_with_retry("commitTransaction")
        except ConnectionFailure as exc:
            exc._remove_error_label("TransientTransactionError")
            _reraise_with_unknown_commit(exc)
        except OperationFailure as exc:
            if exc.code not in _UNKNOWN_COMMIT_ERROR_CODES:
                # The server reports errorLabels in the case.
                raise
            # We do not know if the commit was successfully applied on the
            # server or if it satisfied the provided write concern, set the
            # unknown commit error label.
            _reraise_with_unknown_commit(exc)
        finally:
            self._txn_state = TxnState.COMMITTED

    async def abort_transaction(self) -> None:
        """Abort a multi-statement transaction."""
        self._check_ended()

        if self._txn_state == TxnState.NONE:
            raise InvalidOperation("No transaction started")
        elif self._txn_state == TxnState.STARTING:
            self._txn_state = TxnState.ABORTED
            return
        elif self._txn_state == TxnState.ABORTED:
            raise InvalidOperation("Cannot call abortTransaction twice")
        elif self._txn_state in (TxnState.COMMITTED, TxnState.COMMITTED_EMPTY):
            raise InvalidOperation(
                "Cannot call abortTransaction after calling commitTransaction"
            )

        try:
            await self._finish_transaction_with_retry("abortTransaction")
        except (OperationFailure, ConnectionFailure):
            pass
        finally:
            self._txn_state = TxnState.ABORTED

    async def _finish_transaction_with_retry(self, command_name: str):
        for is_retry in [False, True]:
            try:
                await self._finish_transaction(command_name, is_retry)
                break
            except PyMongoError as exc:
                is_retryable = False

                if isinstance(exc, ConnectionFailure) and not isinstance(
                    exc, NotPrimaryError
                ):
                    is_retryable = True
                else:
                    error_details = None
                    if isinstance(exc, BulkWriteError):
                        wce = exc.details["writeConcernErrors"]
                        error_details = wce[-1] if wce else None
                    elif isinstance(exc, (NotPrimaryError, OperationFailure)):
                        error_details = exc.details
                    if error_details:
                        labels = error_details.get("errorLabels", [])
                        is_retryable = "RetryableWriteError" in labels

                if not is_retry and is_retryable:
                    continue
                raise

    async def _finish_transaction(self, command_name: str, is_retry: bool):
        assert self._txn_options is not None
        wc = self._txn_options.write_concern or self.connection.write_concern
        body = {command_name: 1}

        if command_name == "commitTransaction":
            if self._txn_options.max_commit_time_ms:
                body["maxTimeMS"] = self._txn_options.max_commit_time_ms

            if is_retry:
                wc_doc = wc.document
                wc_doc["w"] = "majority"
                wc_doc.setdefault("wtimeout", 10_000)
                wc = WriteConcern(**wc_doc)

        return await self.connection.admin.command(
            {**body, "writeConcern": wc.document},
            session=self,
        )

    def _apply_to_command(self, body: Document) -> None:
        """
        Apply this session's parameters to the command body
        NB: modifies the argument!
        """
        body["lsid"] = self._use_session_id()
        if self.in_transaction():
            if self._txn_state == TxnState.STARTING:
                self._txn_state = TxnState.IN_PROGRESS
                body["startTransaction"] = True

            body["txnNumber"] = self._server_session.transaction_id
            body["autocommit"] = False
