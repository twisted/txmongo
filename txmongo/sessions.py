from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING, Optional, Type

import bson
from bson import Int64, UuidRepresentation
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument
from pymongo.errors import ConnectionFailure, InvalidOperation, OperationFailure

from txmongo.pymongo_errors import _UNKNOWN_COMMIT_ERROR_CODES
from txmongo.pymongo_internals import _reraise_with_unknown_commit

if TYPE_CHECKING:
    from txmongo.connection import ConnectionPool
    from txmongo.types import Document


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
class SessionOptions: ...


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
    _implicit: bool = False

    connection: ConnectionPool
    options: SessionOptions

    _server_session: ServerSession | None = None
    _is_ended: bool = False

    _cluster_time: Optional[Document] = None

    _txn_state: TxnState = TxnState.NONE

    def __init__(
        self,
        connection: ConnectionPool,
        options: Optional[SessionOptions],
        *,
        implicit: bool,
    ):
        self.connection = connection
        if options is None:
            options = SessionOptions()
        self.options = options
        self._implicit = implicit

    async def __aenter__(self) -> ClientSession:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.end_session()

    @property
    def implicit(self) -> bool:
        return self._implicit

    @property
    def is_ended(self) -> bool:
        return self._is_ended

    @property
    def session_id(self) -> RawBSONDocument:
        return self.get_session_id()

    def get_session_id(self) -> RawBSONDocument:
        if self._is_ended:
            raise ValueError("Cannot use an ended session")

        if not self._server_session:
            self._server_session = self.connection._acquire_server_session()
        return self._server_session.session_id

    def _use_session_id(self) -> RawBSONDocument:
        session_id = self.get_session_id()
        self._server_session.update_last_use()
        return session_id

    async def end_session(self) -> None:
        self._is_ended = True

        if self._server_session is None:
            return

        if self.in_transaction():
            await self.abort_transaction()

        self.connection._return_server_session(self._server_session)
        self._server_session = None

    def mark_dirty(self) -> None:
        if self._server_session:
            self._server_session.mark_dirty()

    @property
    def cluster_time(self) -> Optional[Document]:
        return self._cluster_time

    def advance_cluster_time(self, cluster_time: Document) -> None:
        if self._cluster_time is None:
            self._cluster_time = cluster_time
        elif self._cluster_time["clusterTime"] < cluster_time["clusterTime"]:
            self._cluster_time = cluster_time

    def _check_ended(self):
        if self._is_ended:
            raise InvalidOperation("Cannot use ended session")

    def in_transaction(self) -> bool:
        return self._txn_state in {TxnState.STARTING, TxnState.IN_PROGRESS}

    def start_transaction(self) -> _TransactionContext:
        self._check_ended()

        if self.in_transaction():
            raise InvalidOperation("Transaction already in progress")

        self._txn_state = TxnState.STARTING
        # FIXME: ↓ materialize server session. Make this more explicit.
        self.get_session_id()
        self._server_session.inc_transaction_id()
        return _TransactionContext(self)

    async def commit_transaction(self) -> None:
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
        # FIXME: add retrying
        await self._finish_transaction(command_name)

    async def _finish_transaction(self, command_name: str):
        # FIXME: obey maxTimeMS from transaction options
        # FIXME: obey writeConcern from transaction options

        self.connection.admin.command({command_name: 1}, session=self)

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
