from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, List, Tuple, Union

import bson
from bson import CodecOptions, ObjectId
from bson.raw_bson import RawBSONDocument
from pymongo import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
    WriteConcern,
)
from pymongo.common import (
    validate_is_document_type,
    validate_ok_for_replace,
    validate_ok_for_update,
)

from txmongo._bulk_constants import (
    _DELETE,
    _INSERT,
    _UPDATE,
    COMMAND_NAME,
    PAYLOAD_ARG_NAME,
)
from txmongo.protocol import MongoProtocol, Msg
from txmongo.types import Document

if TYPE_CHECKING:
    from .collection import Collection
    from .sessions import ClientSession

_WriteOp = Union[InsertOne, UpdateOne, UpdateMany, ReplaceOne, DeleteOne, DeleteMany]


class _Run:
    op_type: int
    ordered: bool

    ops: List[Any]
    index_map: List[int]

    def __init__(self, op_type: int, ordered: bool):
        self.op_type = op_type
        self.ordered = bool(ordered)
        self.ops = []
        self.index_map = []

    def index(self, idx: int) -> int:
        """Get the original index of an operation in this run.

        :param idx: The Run index that maps to the original index.
        """
        return self.index_map[idx]

    def add(self, index: int, operation: Any):
        self.index_map.append(index)
        self.ops.append(operation)

    def gen_messages(
        self,
        collection: Collection,
        session: ClientSession,
        write_concern: WriteConcern,
        proto: MongoProtocol,
        codec_options: CodecOptions,
    ) -> Iterator[Tuple[int, Msg]]:
        payload_arg_name = PAYLOAD_ARG_NAME[self.op_type]
        msg = collection.connection._create_message(
            session,
            collection.database,
            body={
                COMMAND_NAME[self.op_type]: collection.name,
                "ordered": self.ordered,
            },
            payload={payload_arg_name: []},
            write_concern=write_concern,
            codec_options=codec_options,
            acknowledged=write_concern.acknowledged,
        )
        docs_offset = 0
        msg_doc_count = 0
        msg_size = empty_msg_size = msg.size_in_bytes()
        for doc in self.ops:
            doc_bytes = bson.encode(doc, codec_options=codec_options)

            enough_docs = msg_doc_count >= proto.max_write_batch_size
            enough_size = msg_size + len(doc_bytes) >= proto.max_message_size
            if enough_docs or enough_size:
                yield docs_offset, msg

                msg.payload[payload_arg_name] = []
                docs_offset += msg_doc_count
                msg_doc_count = 0
                msg_size = empty_msg_size

            msg.payload[payload_arg_name].append(doc_bytes)
            msg_doc_count += 1
            msg_size += len(doc_bytes)

        yield docs_offset, msg


class _Bulk:
    ordered: bool
    ops: List[Tuple[int, Document]]

    def __init__(self, ordered: bool):
        self.ordered = bool(ordered)
        self.ops = []

    def add_write_op(self, op: _WriteOp):
        if isinstance(op, InsertOne):
            self.add_insert(op._doc)
        elif isinstance(op, UpdateOne):
            self.add_update(op._filter, op._doc, multi=False, upsert=op._upsert)
        elif isinstance(op, UpdateMany):
            self.add_update(op._filter, op._doc, multi=True, upsert=op._upsert)
        elif isinstance(op, ReplaceOne):
            self.add_replace(op._filter, op._doc, upsert=op._upsert)
        elif isinstance(op, DeleteOne):
            self.add_delete(op._filter, limit=1)
        elif isinstance(op, DeleteMany):
            self.add_delete(op._filter, limit=0)
        else:
            raise TypeError(f"{op!r} is not a valid write operation")

    def add_insert(self, document: Document):
        validate_is_document_type("document", document)
        if not isinstance(document, RawBSONDocument) and "_id" not in document:
            document["_id"] = ObjectId()
        self.ops.append((_INSERT, document))

    def add_update(
        self, selector: Document, update: Document, *, multi: bool, upsert: bool
    ):
        validate_ok_for_update(update)
        cmd = {
            "q": selector,
            "u": update,
            "multi": multi,
            "upsert": upsert,
        }
        self.ops.append((_UPDATE, cmd))

    def add_replace(self, selector: Document, replacement: Document, *, upsert: bool):
        validate_ok_for_replace(replacement)
        cmd = {
            "q": selector,
            "u": replacement,
            "upsert": upsert,
        }
        self.ops.append((_UPDATE, cmd))

    def add_delete(self, selector: Document, *, limit: int):
        cmd = {"q": selector, "limit": limit}
        self.ops.append((_DELETE, cmd))

    def gen_ordered(self) -> Iterator[_Run]:
        run = None
        for index, (op_type, operation) in enumerate(self.ops):
            if run is None:
                run = _Run(op_type, ordered=True)
            elif run.op_type != op_type:
                yield run
                run = _Run(op_type, ordered=True)
            run.add(index, operation)
        if run:
            yield run

    def gen_unordered(self) -> Iterator[_Run]:
        runs = {
            _INSERT: _Run(_INSERT, ordered=False),
            _UPDATE: _Run(_UPDATE, ordered=False),
            _DELETE: _Run(_DELETE, ordered=False),
        }
        for index, (op_type, operation) in enumerate(self.ops):
            runs[op_type].add(index, operation)

        for run in runs.values():
            if run.ops:
                yield run

    def gen_runs(self) -> Iterator[_Run]:
        if self.ordered:
            yield from self.gen_ordered()
        else:
            yield from self.gen_unordered()
