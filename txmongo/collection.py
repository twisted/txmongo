# Copyright 2009-2015 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import collections.abc
from operator import itemgetter
from typing import Iterable, List, Optional

from bson import ObjectId
from bson.codec_options import CodecOptions
from bson.son import SON
from pymongo.collection import ReturnDocument
from pymongo.common import (
    validate_boolean,
    validate_is_document_type,
    validate_is_mapping,
    validate_ok_for_replace,
    validate_ok_for_update,
)
from pymongo.errors import BulkWriteError, InvalidName, InvalidOperation
from pymongo.results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from pymongo.write_concern import WriteConcern
from twisted.internet import defer
from twisted.internet.defer import Deferred, FirstError, gatherResults, inlineCallbacks
from twisted.python.compat import comparable

from txmongo import filter as qf
from txmongo._bulk import _Bulk, _Run
from txmongo._bulk_constants import _INSERT
from txmongo.protocol import QUERY_PARTIAL, QUERY_SLAVE_OK, MongoProtocol, Msg
from txmongo.pymongo_internals import _check_write_command_response, _merge_command
from txmongo.types import Document
from txmongo.utils import check_deadline, timeout

_DEFERRED_METHODS = frozenset(
    {
        "addCallback",
        "addCallbacks",
        "addErrback",
        "addBoth",
        "called",
        "paused",
        "addTimeout",
        "chainDeferred",
        "callback",
        "errback",
        "pause",
        "unpause",
        "cancel",
        "send",
        "asFuture",
    }
)


class Cursor(Deferred):
    cursor_id: Optional[int] = None
    exhausted: bool = False

    next_batch_deferreds: Optional[List[Deferred]] = None
    _current_loading_op: Optional[defer.Deferred] = None

    _find_deferred: Optional[Deferred] = None

    def __init__(
        self,
        collection: "Collection",
        command: Msg,
        batch_size: int,
        timeout: Optional[float],
    ):
        super().__init__()
        self.collection = collection
        self.command = command
        self.batch_size = batch_size
        self._timeout = timeout

    @inlineCallbacks
    def _old_style_find(self):
        result = []
        try:
            while not self.exhausted:
                batch = yield self.next_batch(timeout=self._timeout)
                if not batch:
                    continue
                result.extend(batch)
            return result
        finally:
            self.close()

    def _make_old_style_find_call(self):
        if self._find_deferred:
            return
        self._find_deferred = self._old_style_find()

    def __getattribute__(self, item):
        if item in _DEFERRED_METHODS:
            self._make_old_style_find_call()
            value = getattr(self._find_deferred, item)
            return value
        return super().__getattribute__(item)

    def _after_connection(self, proto: MongoProtocol, _deadline: Optional[float]):
        return proto.send_msg(self.command, self.collection.codec_options).addCallback(
            self._after_reply, _deadline
        )

    def _get_more(self, proto: MongoProtocol, _deadline: Optional[float]):
        get_more = {
            "getMore": self.cursor_id,
            "$db": self.collection.database.name,
            "collection": self.collection.name,
        }
        if self.batch_size:
            get_more["batchSize"] = self.batch_size

        return proto.send_msg(
            Msg.create(get_more, codec_options=self.collection.codec_options),
            self.collection.codec_options,
        ).addCallback(self._after_reply, _deadline)

    def _after_reply(self, reply: dict, _deadline: Optional[float]):
        check_deadline(_deadline)

        if "cursor" not in reply:
            # For example, when we run `explain` command
            self.cursor_id = None
            self.exhausted = True
            return [reply]
        else:
            cursor = reply["cursor"]
            self.cursor_id = cursor["id"]
            self.exhausted = not self.cursor_id
            return cursor["nextBatch" if "nextBatch" in cursor else "firstBatch"]

    @timeout
    def next_batch(self, _deadline: Optional[float]) -> Deferred[List[dict]]:
        if self.exhausted:
            return defer.succeed([])

        def on_cancel(d):
            self.next_batch_deferreds.remove(d)
            if not self.next_batch_deferreds:
                self.next_batch_deferreds = None
                self._current_loading_op.cancel()

        dfr = defer.Deferred(on_cancel)
        if self.next_batch_deferreds is not None:
            self.next_batch_deferreds.append(dfr)
            return dfr

        self.next_batch_deferreds = [dfr]

        def on_result(result):
            if isinstance(result, defer.Failure):
                self.close()
            if self.next_batch_deferreds:
                deferreds, self.next_batch_deferreds = self.next_batch_deferreds, None
                for d in deferreds:
                    d.callback(result)

        self._current_loading_op = (
            self.collection.database.connection.getprotocol()
            .addCallback(
                (self._after_connection if self.cursor_id is None else self._get_more),
                _deadline,
            )
            .addBoth(on_result)
        )

        return dfr

    def close(self) -> defer.Deferred:
        if not self.cursor_id:
            return defer.succeed(None)
        return self.collection.database.connection.getprotocol().addCallback(
            self.collection._close_cursor_without_response, self.cursor_id
        )

    async def batches(self):
        try:
            while not self.exhausted:
                batch = await self.next_batch(timeout=self._timeout)
                if not batch:
                    continue
                yield batch
        finally:
            self.close()

    async def __aiter__(self):
        async for batch in self.batches():
            for doc in batch:
                yield doc


@comparable
class Collection:
    """Creates new :class:`Collection` object

    :param database:
        the :class:`Database` instance to get collection from

    :param name:
        the name of the collection to get

    :param write_concern:
        An instance of :class:`~pymongo.write_concern.WriteConcern`.
        If ``None``, ``database.write_concern`` is used.

    :param codec_options:
        An instance of :class:`~bson.codec_options.CodecOptions`.
        If ``None``, ``database.codec_options`` is used.
    """

    def __init__(self, database, name, write_concern=None, codec_options=None):
        if not isinstance(name, (bytes, str)):
            raise TypeError("TxMongo: name must be an instance of (bytes, str).")

        if not name or ".." in name:
            raise InvalidName("TxMongo: collection names cannot be empty.")
        if "$" in name and not (
            name.startswith("oplog.$main") or name.startswith("$cmd")
        ):
            msg = "TxMongo: collection names must not contain '$', '{0}'".format(
                repr(name)
            )
            raise InvalidName(msg)
        if name[0] == "." or name[-1] == ".":
            msg = "TxMongo: collection names must not start or end with '.', '{0}'".format(
                repr(name)
            )
            raise InvalidName(msg)
        if "\x00" in name:
            raise InvalidName(
                "TxMongo: collection names must not contain the null character."
            )

        self._database = database
        self._collection_name = str(name)
        self.__write_concern = write_concern
        self.__codec_options = codec_options

    def __str__(self):
        return "%s.%s" % (self._database.name, self._collection_name)

    def __repr__(self):
        return "Collection(%s, %s)" % (self._database, self._collection_name)

    @property
    def full_name(self):
        """Full name of this :class:`Collection`, i.e.
        `db_name.collection_name`"""
        return "{0}.{1}".format(self._database.name, self._collection_name)

    @property
    def name(self):
        """Name of this :class:`Collection` (without database name)."""
        return self._collection_name

    @property
    def database(self):
        """The :class:`~txmongo.database.Database` that this :class:`Collection`
        is a part of."""
        return self._database

    def __getitem__(self, collection_name):
        """Get a sub-collection of this collection by name."""
        return Collection(
            self._database, "%s.%s" % (self._collection_name, collection_name)
        )

    def __cmp__(self, other):
        if isinstance(other, Collection):

            def cmp(a, b):
                return (a > b) - (a < b)

            return cmp(
                (self._database, self._collection_name),
                (other._database, other._collection_name),
            )
        return NotImplemented

    def __getattr__(self, collection_name):
        """Get a sub-collection of this collection by name."""
        return self[collection_name]

    def __call__(self, collection_name):
        """Get a sub-collection of this collection by name."""
        return self[collection_name]

    @property
    def codec_options(self):
        """Read only access to the :class:`~bson.codec_options.CodecOptions`
        of this instance.

        Use ``coll.with_options(codec_options=CodecOptions(...))`` to change
        codec options.
        """
        return self.__codec_options or self._database.codec_options

    @property
    def write_concern(self):
        """Read only access to the :class:`~pymongo.write_concern.WriteConcern`
        of this instance.

        Use ``coll.with_options(write_concern=WriteConcern(...))`` to change
        the Write Concern.
        """
        return self.__write_concern or self._database.write_concern

    def with_options(self, **kwargs):
        """with_options(*, write_concern=None, codec_options=None)

        Get a clone of collection changing the specified settings.

        :param write_concern: *(keyword only)*
            new :class:`~pymongo.write_concern.WriteConcern` to use.

        :param codec_options: *(keyword only)*
            new :class:`~bson.codec_options.CodecOptions` to use.
        """
        # PyMongo's method gets several positional arguments. We support
        # only write_concern for now which is the 3rd positional argument.
        # So we are using **kwargs here to force user's code to specify
        # write_concern as named argument, so adding other args in future
        # won't break compatibility
        write_concern = kwargs.get("write_concern") or self.__write_concern
        codec_options = kwargs.get("codec_options") or self.codec_options

        return Collection(
            self._database,
            self._collection_name,
            write_concern=write_concern,
            codec_options=codec_options,
        )

    @staticmethod
    def _normalize_fields_projection(fields):
        """
        transform a list of fields from ["a", "b"] to {"a":1, "b":1}
        """
        if fields is None:
            return None

        if isinstance(fields, dict):
            return fields

        # Consider fields as iterable
        as_dict = {}
        for field in fields:
            if not isinstance(field, (bytes, str)):
                raise TypeError("TxMongo: fields must be a list of key names.")
            as_dict[field] = 1
        if not as_dict:
            # Empty list should be treated as "_id only"
            as_dict = {"_id": 1}
        return as_dict

    @staticmethod
    def _gen_index_name(keys):
        return "_".join(["%s_%s" % item for item in keys])

    @timeout
    def options(self, _deadline=None):
        """options()

        Get the options set on this collection.

        :returns:
            :class:`Deferred` that called back with dictionary of options
            and their values or with empty dict if collection doesn't exist.
        """

        def on_ok(response):
            assert response["cursor"]["id"] == 0
            first_batch = response["cursor"]["firstBatch"]
            if first_batch:
                options = first_batch[0].get("options", {})
            else:
                options = {}
            if "create" in options:
                del options["create"]
            return options

        return self._database.command(
            {"listCollections": 1, "filter": {"name": self.name}}
        ).addCallback(on_ok)

    def find(
        self,
        filter=None,
        projection=None,
        skip=0,
        limit=0,
        sort=None,
        *,
        batch_size=0,
        allow_partial_results: bool = False,
        flags=0,
        timeout: Optional[float] = None,
    ):
        """find(filter=None, projection=None, skip=0, limit=0, sort=None, allow_partial_results=False)

        Find documents in a collection.

        Ordering, indexing hints and other query parameters can be set with
        `sort` argument. See :mod:`txmongo.filter` for details.

        :param filter:
            MongoDB query document. To return all documents in a collection,
            omit this parameter or pass an empty document (``{}``). You can pass
            ``{"key": "value"}`` to select documents having ``key`` field
            equal to ``"value"`` or use any of `MongoDB's query selectors
            <https://docs.mongodb.org/manual/reference/operator/query/#query-selectors>`_.

        :param projection:
            a list of field names that should be returned for each document
            in the result set or a dict specifying field names to include or
            exclude. If `projection` is a list ``_id`` fields will always be
            returned. Use a dict form to exclude fields:
            ``projection={"_id": False}``.

        :param skip:
            the number of documents to omit from the start of the result set.

        :param limit:
            the maximum number of documents to return. All documents are
            returned when `limit` is zero.

        :param sort:
            query filter. You can specify ordering, indexing hints and other query
            parameters with this argument. See :mod:`txmongo.filter` for details.

        :param allow_partial_results:
            if True, mongos will return partial results if some shards are down
            instead of returning an error

        :returns: an instance of :class:`Deferred` that called back with a list with
            all documents found.
        """
        return self._find_with_cursor_v2(
            filter,
            projection,
            skip,
            limit,
            sort,
            batch_size=batch_size,
            allow_partial_results=allow_partial_results,
            flags=flags,
            timeout=timeout,
        )

    @staticmethod
    def __apply_find_filter(spec, c_filter):
        if c_filter:
            if "query" not in spec:
                spec = {"$query": spec}

            for k, v in c_filter.items():
                if isinstance(v, (list, tuple)):
                    spec["$" + k] = SON(v)
                else:
                    spec["$" + k] = v

        return spec

    @timeout
    def find_with_cursor(
        self,
        filter=None,
        projection=None,
        skip=0,
        limit=0,
        sort=None,
        batch_size=0,
        *,
        allow_partial_results: bool = False,
        flags=0,
        _deadline=None,
    ):
        """find_with_cursor(filter=None, projection=None, skip=0, limit=0, sort=None, batch_size=0, allow_partial_results=False)

        Find documents in a collection and return them in one batch at a time.

        Arguments are the same as for :meth:`find()`.

        :returns: an instance of :class:`Deferred` that fires with tuple of ``(docs, dfr)``,
            where ``docs`` is a partial result, returned by MongoDB in a first batch and
            ``dfr`` is a :class:`Deferred` that fires with next ``(docs, dfr)``. Last result
            will be ``([], None)``. You can iterate over the result set with code like that:
            ::
                @defer.inlineCallbacks
                def query():
                    docs, dfr = yield coll.find(query, cursor=True)
                    while docs:
                        for doc in docs:
                            do_something(doc)
                        docs, dfr = yield dfr
        """

        cursor = self._find_with_cursor_v2(
            filter,
            projection,
            skip,
            limit,
            sort,
            batch_size=batch_size,
            allow_partial_results=allow_partial_results,
            flags=flags,
        )

        def on_batch(batch, this_func):
            if batch:
                return batch, cursor.next_batch(deadline=_deadline).addCallback(
                    this_func, this_func
                )
            else:
                return [], None

        return cursor.next_batch(deadline=_deadline).addCallback(on_batch, on_batch)

    def _find_with_cursor_v2(
        self,
        filter=None,
        projection=None,
        skip=0,
        limit=0,
        sort=None,
        batch_size=0,
        *,
        allow_partial_results: bool = False,
        flags=0,
        timeout: Optional[float] = None,
    ):
        if filter is None:
            filter = SON()

        if not isinstance(filter, dict):
            raise TypeError("TxMongo: filter must be an instance of dict.")
        if not isinstance(projection, (dict, list)) and projection is not None:
            raise TypeError("TxMongo: projection must be an instance of dict or list.")
        if not isinstance(skip, int):
            raise TypeError("TxMongo: skip must be an instance of int.")
        if not isinstance(limit, int):
            raise TypeError("TxMongo: limit must be an instance of int.")
        if not isinstance(batch_size, int):
            raise TypeError("TxMongo: batch_size must be an instance of int.")
        if sort:
            validate_is_mapping("sort", sort)

        projection = self._normalize_fields_projection(projection)

        filter = self.__apply_find_filter(filter, sort)

        cmd = self._gen_find_command(
            self.database.name,
            self.name,
            filter,
            projection,
            skip,
            limit,
            batch_size,
            allow_partial_results,
            flags,
        )
        return Cursor(self, cmd, batch_size, timeout)

    _MODIFIERS = {
        "$query": "filter",
        "$orderby": "sort",
        "$hint": "hint",
        "$comment": "comment",
        "$maxScan": "maxScan",
        "$maxTimeMS": "maxTimeMS",
        "$max": "max",
        "$min": "min",
        "$returnKey": "returnKey",
        "$showRecordId": "showRecordId",
        "$showDiskLoc": "showRecordId",  # <= MongoDB 3.0
    }

    def _gen_find_command(
        self,
        db_name: str,
        coll_name: str,
        filter_with_modifiers,
        projection,
        skip,
        limit,
        batch_size,
        allow_partial_results,
        flags: int,
    ) -> Msg:
        cmd = {"find": coll_name}
        if "$query" in filter_with_modifiers:
            cmd.update(
                [
                    (
                        (self._MODIFIERS[key], val)
                        if key in self._MODIFIERS
                        else (key, val)
                    )
                    for key, val in filter_with_modifiers.items()
                ]
            )
        else:
            cmd["filter"] = filter_with_modifiers

        if projection:
            cmd["projection"] = projection
        if skip:
            cmd["skip"] = skip
        if limit:
            cmd["limit"] = abs(limit)
            if limit < 0:
                cmd["singleBatch"] = True
                cmd["batchSize"] = abs(limit)
        if batch_size:
            cmd["batchSize"] = batch_size

        if flags & QUERY_SLAVE_OK:
            cmd["$readPreference"] = {"mode": "secondaryPreferred"}
        if allow_partial_results or flags & QUERY_PARTIAL:
            cmd["allowPartialResults"] = True

        if "$explain" in filter_with_modifiers:
            cmd.pop("$explain")
            cmd = {"explain": cmd}

        cmd["$db"] = db_name
        return Msg.create(cmd, codec_options=self.codec_options)

    def _close_cursor_without_response(self, proto: MongoProtocol, cursor_id: int):
        proto.send_msg(
            Msg.create(
                {
                    "killCursors": self.name,
                    "$db": self._database.name,
                    "cursors": [cursor_id],
                },
                acknowledged=False,
            )
        )

    @timeout
    def find_one(
        self,
        filter=None,
        projection=None,
        skip=0,
        sort=None,
        *,
        allow_partial_results=False,
        flags=0,
        _deadline=None,
    ):
        """find_one(filter=None, projection=None, skip=0, sort=None, allow_partial_results=False)

        Get a single document from the collection.

        All arguments to :meth:`find()` are also valid for :meth:`find_one()`,
        except `limit` which is always 1.

        :returns:
            a :class:`Deferred` that called back with single document
            or ``None`` if no matching documents is found.
        """
        if isinstance(filter, ObjectId):
            filter = {"_id": filter}

        cursor = self._find_with_cursor_v2(
            filter,
            projection,
            skip,
            limit=1,
            sort=sort,
            allow_partial_results=allow_partial_results,
            flags=flags,
        )
        rows = []

        def on_batch(batch, this_func):
            rows.extend(batch)
            if not cursor.exhausted:
                return cursor.next_batch(deadline=_deadline).addCallback(
                    this_func, this_func
                )
            else:
                return rows[0] if rows else None

        return cursor.next_batch(deadline=_deadline).addCallback(on_batch, on_batch)

    @timeout
    def count(self, filter=None, **kwargs):
        """Get the number of documents in this collection.

        :param filter:
            argument is a query document that selects which documents to
            count in the collection.

        :param hint: *(keyword only)*
            :class:`~txmongo.filter.hint` instance specifying index to use.

        :param int limit: *(keyword only)*
            The maximum number of documents to count.

        :param int skip: *(keyword only)*
            The number of matching documents to skip before returning results.

        :returns: a :class:`Deferred` that called back with a number of
                  documents matching the criteria.
        """
        if "hint" in kwargs:
            hint = kwargs["hint"]
            if not isinstance(hint, qf.hint):
                raise TypeError("hint must be an instance of txmongo.filter.hint")
            kwargs["hint"] = SON(kwargs["hint"]["hint"])

        return self._database.command(
            "count", self._collection_name, query=filter or SON(), **kwargs
        ).addCallback(lambda result: int(result["n"]))

    @timeout
    def filemd5(self, spec, **kwargs):
        if not isinstance(spec, ObjectId):
            raise ValueError(
                "TxMongo: filemd5 expected an objectid for its non-keyword argument."
            )

        return self._database.command(
            "filemd5", spec, root=self._collection_name, **kwargs
        ).addCallback(lambda result: result.get("md5"))

    @timeout
    def insert_one(
        self, document: Document, _deadline=None
    ) -> Deferred[InsertOneResult]:
        """insert_one(document)

        Insert a single document into collection

        :param document: Document to insert

        :returns:
            :class:`Deferred` that called back with
            :class:`pymongo.results.InsertOneResult`
        """
        validate_is_document_type("document", document)
        return self._insert_one(document, _deadline)

    @defer.inlineCallbacks
    def _insert_one(
        self, document: Document, _deadline: Optional[float]
    ) -> InsertOneResult:
        if "_id" not in document:
            document["_id"] = ObjectId()
        inserted_id = document["_id"]

        msg = Msg.create(
            {
                "insert": self.name,
                "$db": self.database.name,
                "writeConcern": self.write_concern.document,
            },
            {
                "documents": [document],
            },
            codec_options=self.codec_options,
            acknowledged=self.write_concern.acknowledged,
        )

        proto = yield self._database.connection.getprotocol()
        check_deadline(_deadline)
        reply: Optional[dict] = yield proto.send_msg(msg, self.codec_options)
        if reply:
            _check_write_command_response(reply)
        return InsertOneResult(inserted_id, self.write_concern.acknowledged)

    @timeout
    def insert_many(self, documents: Iterable[Document], ordered=True, _deadline=None):
        """insert_many(documents, ordered=True)

        Insert an iterable of documents into collection

        :param documents:
            An iterable of documents to insert (``list``,
            ``tuple``, ...)

        :param ordered:
            If ``True`` (the default) documents will be inserted on the server
            serially, in the order provided. If an error occurs, all remaining
            inserts are aborted. If ``False``, documents will be inserted on
            the server in arbitrary order, possibly in parallel, and all
            document inserts will be attempted.

        :returns:
            :class:`Deferred` that called back with
            :class:`pymongo.results.InsertManyResult`
        """
        inserted_ids = []

        def gen():
            for doc in documents:
                validate_is_document_type("document", doc)
                oid = doc.get("_id")
                if oid is None:
                    oid = ObjectId()
                    doc["_id"] = oid
                inserted_ids.append(oid)
                yield (_INSERT, doc)

        bulk = _Bulk(ordered)
        bulk.ops = list(gen())
        return self._execute_bulk(bulk, _deadline).addCallback(
            lambda _: InsertManyResult(inserted_ids, self.write_concern.acknowledged)
        )

    @defer.inlineCallbacks
    def _update(self, filter, update, upsert, multi, _deadline):
        msg = Msg.create(
            {
                "update": self.name,
                "$db": self.database.name,
                "writeConcern": self.write_concern.document,
            },
            {
                "updates": [
                    {
                        "q": filter,
                        "u": update,
                        "upsert": bool(upsert),
                        "multi": bool(multi),
                    }
                ],
            },
            codec_options=self.codec_options,
            acknowledged=self.write_concern.acknowledged,
        )

        proto = yield self._database.connection.getprotocol()
        check_deadline(_deadline)
        reply = yield proto.send_msg(msg, self.codec_options)
        if reply:
            _check_write_command_response(reply)
            if reply.get("n") and "upserted" in reply:
                # MongoDB >= 2.6.0 returns the upsert _id in an array
                # element. Break it out for backward compatibility.
                if "upserted" in reply:
                    reply["upserted"] = reply["upserted"][0]["_id"]

        return UpdateResult(reply, self.write_concern.acknowledged)

    @timeout
    def update_one(self, filter, update, upsert=False, _deadline=None):
        """update_one(filter, update, upsert=False)

        Update a single document matching the filter.

        :raises ValueError:
            if `update` document is empty.

        :raises ValueError:
            if `update` document has any fields that don't start with `$` sign.
            This method only allows *modification* of document (with `$set`,
            `$inc`, etc.), not *replacing* it. For replacing use
            :meth:`replace_one()` instead.

        :param filter:
            A query that matches the document to update.

        :param update:
            update document to be used for updating or upserting. See `MongoDB
            Update docs <https://docs.mongodb.org/manual/tutorial/modify-documents/>`_
            for allowed operators.

        :param upsert:
            If ``True``, perform an insert if no documents match the `filter`.

        :returns:
            deferred instance of :class:`pymongo.results.UpdateResult`.
        """
        validate_ok_for_update(update)
        validate_is_mapping("filter", filter)
        validate_boolean("upsert", upsert)

        return self._update(filter, update, upsert, False, _deadline)

    @timeout
    def update_many(self, filter, update, upsert=False, _deadline=None):
        """update_many(filter, update, upsert=False)

        Update one or more documents that match the filter.

        :raises ValueError:
            if `update` document is empty.

        :raises ValueError:
            if `update` document has fields that don't start with `$` sign.
            This method only allows *modification* of document (with `$set`,
            `$inc`, etc.), not *replacing* it. For replacing use
            :meth:`replace_one()` instead.

        :param filter:
            A query that matches the documents to update.

        :param update:
            update document to be used for updating or upserting. See `MongoDB
            Update docs <https://docs.mongodb.org/manual/tutorial/modify-documents/>`_
            for allowed operators.

        :param upsert:
            If ``True``, perform an insert if no documents match the `filter`.

        :returns:
            deferred instance of :class:`pymongo.results.UpdateResult`.
        """
        validate_ok_for_update(update)
        validate_is_mapping("filter", filter)
        validate_boolean("upsert", upsert)

        return self._update(filter, update, upsert, True, _deadline)

    @timeout
    def replace_one(self, filter, replacement, upsert=False, _deadline=None):
        """replace_one(filter, replacement, upsert=False)

        Replace a single document matching the filter.

        :raises ValueError:
            if `update` document is empty

        :raises ValueError:
            if `update` document has fields that starts with `$` sign.
            This method only allows *replacing* document completely. Use
            :meth:`update_one()` for modifying existing document.

        :param filter:
            A query that matches the document to replace.

        :param replacement:
            The new document to replace with.

        :param upsert:
            If ``True``, perform an insert if no documents match the filter.

        :returns:
            deferred instance of :class:`pymongo.results.UpdateResult`.
        """
        validate_ok_for_replace(replacement)
        validate_is_mapping("filter", filter)
        validate_boolean("upsert", upsert)

        return self._update(filter, replacement, upsert, False, _deadline)

    @defer.inlineCallbacks
    def _delete(
        self, filter: dict, let: Optional[dict], multi: bool, _deadline: Optional[float]
    ):
        body = {
            "delete": self.name,
            "$db": self.database.name,
            "writeConcern": self.write_concern.document,
        }

        if let:
            body["let"] = let

        msg = Msg.create(
            body,
            {
                "deletes": [
                    {
                        "q": filter,
                        "limit": 0 if multi else 1,
                    },
                ],
            },
            codec_options=self.codec_options,
            acknowledged=self.write_concern.acknowledged,
        )

        proto = yield self._database.connection.getprotocol()
        check_deadline(_deadline)
        reply = yield proto.send_msg(msg, self.codec_options)
        if reply:
            _check_write_command_response(reply)

        return DeleteResult(reply, self.write_concern.acknowledged)

    @timeout
    def delete_one(
        self,
        filter: dict,
        let: Optional[dict] = None,
        _deadline: Optional[float] = None,
    ) -> Deferred[DeleteResult]:
        """delete_one(filter)"""
        validate_is_mapping("filter", filter)
        if let:
            validate_is_mapping("let", let)
        return self._delete(filter, let, multi=False, _deadline=_deadline)

    @timeout
    def delete_many(
        self,
        filter: dict,
        let: Optional[dict] = None,
        _deadline: Optional[float] = None,
    ) -> Deferred[DeleteResult]:
        """delete_many(filter)"""
        validate_is_mapping("filter", filter)
        if let:
            validate_is_mapping("let", let)
        return self._delete(filter, let, multi=True, _deadline=_deadline)

    @timeout
    def drop(self, _deadline=None):
        """drop()"""
        return self._database.drop_collection(self.name, _deadline=_deadline)

    def create_index(self, sort_fields, **kwargs):
        if not isinstance(sort_fields, qf.sort):
            raise TypeError("TxMongo: sort_fields must be an instance of filter.sort")

        if "name" not in kwargs:
            name = self._gen_index_name(sort_fields["orderby"])
        else:
            name = kwargs.pop("name")

        key = SON()
        for k, v in sort_fields["orderby"]:
            key[k] = v

        index = {"name": name, "key": key}

        if "bucket_size" in kwargs:
            index["bucketSize"] = kwargs.pop("bucket_size")

        index.update(kwargs)

        return self._database.command(
            "createIndexes", self.name, indexes=[index]
        ).addCallback(lambda _: name)

    @timeout
    def ensure_index(self, sort_fields, _deadline=None, **kwargs):
        # ensure_index is an alias of create_index since we are not
        # keeping an index cache same way pymongo does
        return self.create_index(sort_fields, **kwargs)

    @timeout
    def drop_index(self, index_identifier, _deadline=None):
        """drop_index(index_identifier)"""
        if isinstance(index_identifier, (bytes, str)):
            name = index_identifier
        elif isinstance(index_identifier, qf.sort):
            name = self._gen_index_name(index_identifier["orderby"])
        else:
            raise TypeError(
                "TxMongo: index_identifier must be a name or instance of filter.sort"
            )

        return self._database.command(
            "deleteIndexes",
            self.name,
            index=name,
            allowable_errors=["ns not found"],
            _deadline=_deadline,
        )

    @timeout
    def drop_indexes(self, _deadline=None):
        """drop_indexes()"""
        return self.drop_index("*", _deadline=_deadline)

    @timeout
    def index_information(self, _deadline=None):
        """index_information()"""

        def on_ok(indexes_info):
            assert indexes_info["cursor"]["id"] == 0
            raw = indexes_info["cursor"]["firstBatch"]
            info = {}
            for idx in raw:
                info[idx["name"]] = idx
            return info

        codec = CodecOptions(document_class=SON)
        return self._database.command(
            "listIndexes", self.name, codec_options=codec
        ).addCallback(on_ok)

    @timeout
    def rename(self, new_name, _deadline=None):
        """rename(new_name)"""
        to = "%s.%s" % (self._database.name, new_name)
        return self._database("admin").command(
            "renameCollection", str(self), to=to, _deadline=_deadline
        )

    @timeout
    def distinct(self, key, filter=None, _deadline=None, **kwargs):
        """distinct(key, filter=None)"""
        params = {"key": key}
        filter = kwargs.pop("spec", filter)
        if filter:
            params["query"] = filter

        return self._database.command(
            "distinct", self.name, _deadline=_deadline, **params
        ).addCallback(lambda result: result.get("values"))

    @timeout
    def aggregate(
        self, pipeline, full_response=False, initial_batch_size=None, _deadline=None
    ):
        """aggregate(pipeline, full_response=False)"""

        def on_ok(raw, data=None):
            if data is None:
                data = []
            if "firstBatch" in raw["cursor"]:
                batch = raw["cursor"]["firstBatch"]
            else:
                batch = raw["cursor"].get("nextBatch", [])
            data += batch
            if raw["cursor"]["id"] == 0:
                if full_response:
                    raw["result"] = data
                    return raw
                return data
            next_reply = self._database.command(
                "getMore", collection=self.name, getMore=raw["cursor"]["id"]
            )
            return next_reply.addCallback(on_ok, data)

        if initial_batch_size is None:
            cursor = {}
        else:
            cursor = {"batchSize": initial_batch_size}

        return self._database.command(
            "aggregate",
            self.name,
            pipeline=pipeline,
            _deadline=_deadline,
            cursor=cursor,
        ).addCallback(on_ok)

    @timeout
    def map_reduce(self, map, reduce, full_response=False, **kwargs):
        params = {"map": map, "reduce": reduce, **kwargs}

        def on_ok(raw):
            if full_response:
                return raw
            return raw.get("results")

        return self._database.command("mapreduce", self.name, **params).addCallback(
            on_ok
        )

    def _find_and_modify(
        self,
        filter,
        projection,
        sort,
        upsert=None,
        return_document=ReturnDocument.BEFORE,
        _deadline=None,
        **kwargs,
    ):
        validate_is_mapping("filter", filter)
        if not isinstance(return_document, bool):
            raise ValueError(
                "TxMongo: return_document must be ReturnDocument.BEFORE "
                "or ReturnDocument.AFTER"
            )

        cmd = {
            "findAndModify": self.name,
            "query": filter,
            "new": return_document,
            **kwargs,
        }

        if projection is not None:
            cmd["fields"] = self._normalize_fields_projection(projection)

        if sort is not None:
            cmd["sort"] = dict(sort["orderby"])
        if upsert is not None:
            validate_boolean("upsert", upsert)
            cmd["upsert"] = upsert

        no_obj_error = "No matching object found"

        return self._database.command(
            cmd, allowable_errors=[no_obj_error], _deadline=_deadline
        ).addCallback(lambda result: result.get("value"))

    @timeout
    def find_one_and_delete(self, filter, projection=None, sort=None, _deadline=None):
        """find_one_and_delete(filter, projection=None, sort=None)"""
        return self._find_and_modify(
            filter, projection, sort, remove=True, _deadline=_deadline
        )

    @timeout
    def find_one_and_replace(
        self,
        filter,
        replacement,
        projection=None,
        sort=None,
        upsert=False,
        return_document=ReturnDocument.BEFORE,
        _deadline=None,
    ):
        """find_one_and_replace(filter, replacement, projection=None, sort=None, upsert=False, return_document=ReturnDocument.BEFORE)"""
        validate_ok_for_replace(replacement)
        return self._find_and_modify(
            filter,
            projection,
            sort,
            upsert,
            return_document,
            update=replacement,
            _deadline=_deadline,
        )

    @timeout
    def find_one_and_update(
        self,
        filter,
        update,
        projection=None,
        sort=None,
        upsert=False,
        return_document=ReturnDocument.BEFORE,
        _deadline=None,
    ):
        """find_one_and_update(filter, update, projection=None, sort=None, upsert=False, return_document=ReturnDocument.BEFORE)"""
        validate_ok_for_update(update)
        return self._find_and_modify(
            filter,
            projection,
            sort,
            upsert,
            return_document,
            update=update,
            _deadline=_deadline,
        )

    @timeout
    def bulk_write(self, requests, ordered=True, _deadline: Optional[float] = None):
        if not isinstance(requests, collections.abc.Iterable):
            raise TypeError("requests must be iterable")

        bulk = _Bulk(ordered)
        for request in requests:
            bulk.add_write_op(request)
        return self._execute_bulk(bulk, _deadline)

    @inlineCallbacks
    def _execute_bulk(self, bulk: _Bulk, _deadline: Optional[float]):
        if not bulk.ops:
            raise InvalidOperation("No operations to execute")

        proto = yield self._database.connection.getprotocol()
        check_deadline(_deadline)

        # There are four major cases with different behavior of bulk_write:
        #   * Unack, Unordered:  sending all batches and not handling responses at all
        #                        so ignoring any errors
        #
        #   * Ack, Unordered:    sending all batches, accumulating all responses and
        #                        returning aggregated response
        #
        #   * Unack, Ordered:    handling DB responses despite unacknowledged write_concern
        #                        because we must stop on first error (not raising it though)
        #
        #   * Ack, Ordered:      stopping on first error and raising BulkWriteError

        effective_write_concern = self.write_concern
        if bulk.ordered and self.write_concern.acknowledged is False:
            effective_write_concern = WriteConcern(w=1)

        all_responses: List[Deferred] = []

        full_result = {
            "writeErrors": [],
            "writeConcernErrors": [],
            "nInserted": 0,
            "nUpserted": 0,
            "nMatched": 0,
            "nModified": 0,
            "nRemoved": 0,
            "upserted": [],
        }

        def accumulate_response(response: dict, run: _Run, idx_offset: int) -> dict:
            _merge_command(run, full_result, idx_offset, response)
            return response

        got_error = False
        for run in bulk.gen_runs():
            for doc_offset, msg in run.gen_messages(
                self._database.name,
                self.name,
                effective_write_concern,
                proto,
                self.codec_options,
            ):
                check_deadline(_deadline)
                deferred = proto.send_msg(msg, self.codec_options)
                if effective_write_concern.acknowledged:
                    if bulk.ordered:
                        reply = yield deferred
                        accumulate_response(reply, run, doc_offset)
                        if "writeErrors" in reply:
                            got_error = True
                            break
                    else:
                        all_responses.append(
                            deferred.addCallback(accumulate_response, run, doc_offset)
                        )

            if got_error:
                break

        if effective_write_concern.acknowledged and not bulk.ordered:
            try:
                yield gatherResults(all_responses, consumeErrors=True)
            except FirstError as exc:
                exc.subFailure.raiseException()

        if self.write_concern.acknowledged:
            write_errors = full_result["writeErrors"]
            if write_errors:
                write_errors.sort(key=itemgetter("index"))
            if write_errors or full_result["writeConcernErrors"]:
                raise BulkWriteError(full_result)

        return BulkWriteResult(full_result, self.write_concern.acknowledged)
