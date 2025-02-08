from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

import bson
from bson import UuidRepresentation
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument

if TYPE_CHECKING:
    from txmongo.connection import ConnectionPool
    from txmongo.types import Document


_codec_options = DEFAULT_RAW_BSON_OPTIONS.with_options(
    uuid_representation=UuidRepresentation.STANDARD
)


@dataclass
class ServerSession:
    session_id: RawBSONDocument
    last_use: float

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


@dataclass(frozen=True)
class SessionOptions: ...


class ClientSession:
    _implicit: bool = False

    connection: ConnectionPool
    options: SessionOptions

    _server_session: ServerSession | None = None
    _is_ended: bool = False

    _cluster_time: Optional[Document] = None

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
