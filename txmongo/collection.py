# Copyright 2009-2015 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import collections.abc
import warnings
from operator import itemgetter
from typing import Iterable, List, Optional

import bson
from bson import BSON, ObjectId
from bson.code import Code
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
from pymongo.errors import (
    BulkWriteError,
    InvalidName,
    InvalidOperation,
    OperationFailure,
)
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
from txmongo._bulk import _INSERT, _Bulk, _Run
from txmongo.filter import _QueryFilter
from txmongo.protocol import (
    DELETE_SINGLE_REMOVE,
    OP_MSG_MORE_TO_COME,
    UPDATE_MULTI,
    UPDATE_UPSERT,
    Delete,
    Insert,
    Msg,
    Update,
)
from txmongo.pymongo_internals import (
    _check_command_response,
    _check_write_command_response,
    _merge_command,
)
from txmongo.types import Document
from txmongo.utils import check_deadline, timeout


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

    def _list_collections_3_0(self):
        def on_ok(response):
            assert response["cursor"]["id"] == 0
            first_batch = response["cursor"]["firstBatch"]
            if first_batch:
                return first_batch[0]
            else:
                return None

        return self._database.command(
            SON([("listCollections", 1), ("filter", {"name": self.name})])
        ).addCallback(on_ok)

    @timeout
    def options(self, _deadline=None):
        """options()

        Get the options set on this collection.

        :returns:
            :class:`Deferred` that called back with dictionary of options
            and their values or with empty dict if collection doesn't exist.
        """

        def on_3_0_fail(failure):
            failure.trap(OperationFailure)
            return self._database.system.namespaces.find_one(
                {"name": str(self)}, _deadline=_deadline
            )

        def on_ok(result):
            if not result:
                result = {}
            options = result.get("options", {})
            if "create" in options:
                del options["create"]
            return options

        return self._list_collections_3_0().addErrback(on_3_0_fail).addCallbacks(on_ok)

    @staticmethod
    def _find_args_compat(*args, **kwargs):
        """
        signature of find() was changed from
        (spec=None, skip=0, limit=0, fields=None, filter=None, cursor=False, **kwargs)
        to
        (filter=None, projection=None, skip=0, limit=0, sort=None, **kwargs)

        This function makes it compatible with both
        """

        def old(
            spec=None, skip=0, limit=0, fields=None, filter=None, cursor=False, **kwargs
        ):
            warnings.warn(
                "find(), find_with_cursor() and find_one() signatures have "
                "changed. Please refer to documentation.",
                DeprecationWarning,
            )
            return new(spec, fields, skip, limit, filter, cursor=cursor, **kwargs)

        def new(filter=None, projection=None, skip=0, limit=0, sort=None, **kwargs):
            args = {
                "filter": filter,
                "projection": projection,
                "skip": skip,
                "limit": limit,
                "sort": sort,
            }
            args.update(kwargs)
            return args

        old_if = (
            "fields" in kwargs,
            "spec" in kwargs,
            len(args) == 0 and isinstance(kwargs.get("filter"), _QueryFilter),
            len(args) >= 1 and "filter" in kwargs,
            len(args) >= 2 and isinstance(args[1], int),
        )

        if any(old_if):
            return old(*args, **kwargs)
        else:
            return new(*args, **kwargs)

    @timeout
    def find(self, *args, **kwargs):
        """find(filter=None, projection=None, skip=0, limit=0, sort=None, **kwargs)

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

        :returns: an instance of :class:`Deferred` that called back with a list with
            all documents found.
        """
        new_kwargs = self._find_args_compat(*args, **kwargs)
        return self.__real_find(**new_kwargs)

    def __real_find(
        self, filter=None, projection=None, skip=0, limit=0, sort=None, **kwargs
    ):
        cursor = kwargs.pop("cursor", False)

        rows = []

        def on_ok(result, this_func):
            docs, dfr = result

            if cursor:
                warnings.warn(
                    "find() with cursor=True is deprecated. Please use"
                    "find_with_cursor() instead.",
                    DeprecationWarning,
                )
                return docs, dfr

            if docs:
                rows.extend(docs)
                return dfr.addCallback(this_func, this_func)
            else:
                return rows

        return self.__real_find_with_cursor(
            filter, projection, skip, limit, sort, **kwargs
        ).addCallback(on_ok, on_ok)

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
    def find_with_cursor(self, *args, **kwargs):
        """find_with_cursor(filter=None, projection=None, skip=0, limit=0, sort=None, batch_size=0, **kwargs)

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
        new_kwargs = self._find_args_compat(*args, **kwargs)
        return self.__real_find_with_cursor(**new_kwargs)

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

    @classmethod
    def _gen_find_command(
        cls,
        db_name: str,
        coll_name: str,
        filter_with_modifiers,
        projection,
        skip,
        limit,
        batch_size,
    ):
        cmd = {"find": coll_name}
        if "$query" in filter_with_modifiers:
            cmd.update(
                [
                    (cls._MODIFIERS[key], val) if key in cls._MODIFIERS else (key, val)
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

        if "$explain" in filter_with_modifiers:
            cmd.pop("$explain")
            cmd = {"explain": cmd}

        cmd["$db"] = db_name
        return cmd

    def __real_find_with_cursor(
        self,
        filter=None,
        projection=None,
        skip=0,
        limit=0,
        sort=None,
        batch_size=0,
        **kwargs,
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

        projection = self._normalize_fields_projection(projection)

        filter = self.__apply_find_filter(filter, sort)

        as_class = kwargs.get("as_class")
        codec_options = self.codec_options
        if as_class is not None:
            warnings.warn(
                "as_class argument of will be removed in the next version of TxMongo. Please use document_class parameter of codec_options.",
                DeprecationWarning,
            )
            codec_options = codec_options.with_options(document_class=as_class)

        deadline = kwargs.get("_deadline")

        def after_connection(proto):
            check_deadline(deadline)

            cmd = self._gen_find_command(
                self.database.name,
                self.name,
                filter,
                projection,
                skip,
                limit,
                batch_size,
            )

            return proto.send_MSG(
                Msg(body=bson.encode(cmd, codec_options=codec_options))
            ).addCallback(after_reply, after_reply, proto)

        # this_func argument is just a reference to after_reply function itself.
        # after_reply can reference to itself directly but this will create a circular
        # reference between closure and function object which will add unnecessary
        # work for GC.
        def after_reply(response, this_func, proto, fetched=0):
            reply = bson.decode(response.body, codec_options=codec_options)
            try:
                check_deadline(deadline)
            except Exception:
                cursor_id = reply.get("cursor", {}).get("id")
                if cursor_id:
                    proto.send_MSG(
                        Msg(
                            flag_bits=OP_MSG_MORE_TO_COME,
                            body=bson.encode(
                                {
                                    "killCursors": self.name,
                                    "$db": self.database.name,
                                    "cursors": [cursor_id],
                                },
                            ),
                        )
                    )
                raise

            _check_command_response(reply)

            if "cursor" not in reply:
                # For example, when we run `explain` command
                return [reply], defer.succeed(([], None))

            cursor = reply["cursor"]
            docs_key = "firstBatch"
            if "nextBatch" in cursor:
                docs_key = "nextBatch"

            docs_count = len(cursor[docs_key])
            if limit > 0:
                docs_count = min(docs_count, limit - fetched)
            fetched += docs_count
            out = cursor[docs_key][:docs_count]

            if cursor["id"]:
                if limit == 0:
                    to_fetch = 0  # no limit
                    if batch_size:
                        to_fetch = batch_size
                elif limit < 0:
                    # We won't actually get here because MongoDB won't
                    # create a cursor when limit < 0
                    to_fetch = None
                else:
                    to_fetch = limit - fetched
                    if to_fetch <= 0:
                        to_fetch = None  # close cursor
                    elif batch_size:
                        to_fetch = min(batch_size, to_fetch)

                if to_fetch is None:
                    # FIXME: extract this to a function
                    proto.send_MSG(
                        Msg(
                            flag_bits=OP_MSG_MORE_TO_COME,
                            body=bson.encode(
                                {
                                    "killCursors": self.name,
                                    "$db": self.database.name,
                                    "cursors": [cursor["id"]],
                                },
                            ),
                        )
                    )
                    return out, defer.succeed(([], None))

                # FIXME: extract this to a function
                get_more = {
                    "getMore": cursor["id"],
                    "collection": self.name,
                    "$db": self.database.name,
                }
                if batch_size:
                    get_more["batchSize"] = batch_size

                next_reply = proto.send_MSG(
                    Msg(body=bson.encode(get_more, codec_options=codec_options))
                )
                next_reply.addCallback(this_func, this_func, proto, fetched)
                return out, next_reply

            return out, defer.succeed(([], None))

        return self._database.connection.getprotocol().addCallback(after_connection)

    @timeout
    def find_one(self, *args, **kwargs):
        """find_one(filter=None, projection=None, **kwargs)

        Get a single document from the collection.

        All arguments to :meth:`find()` are also valid for :meth:`find_one()`,
        although `limit` will be ignored.

        :returns:
            a :class:`Deferred` that called back with single document
            or ``None`` if no matching documents is found.
        """
        new_kwargs = self._find_args_compat(*args, **kwargs)
        if isinstance(new_kwargs["filter"], ObjectId):
            new_kwargs["filter"] = {"_id": new_kwargs["filter"]}

        new_kwargs["limit"] = 1
        return self.__real_find(**new_kwargs).addCallback(
            lambda result: result[0] if result else None
        )

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
        if "spec" in kwargs:
            filter = kwargs["spec"]

        if "hint" in kwargs:
            hint = kwargs["hint"]
            if not isinstance(hint, qf.hint):
                raise TypeError("hint must be an instance of txmongo.filter.hint")
            kwargs["hint"] = SON(kwargs["hint"]["hint"])

        return self._database.command(
            "count", self._collection_name, query=filter or SON(), **kwargs
        ).addCallback(lambda result: int(result["n"]))

    @timeout
    def group(self, keys, initial, reduce, condition=None, finalize=None, **kwargs):
        warnings.warn(
            "Collection.group() method will be removed in the next version of TxMongo. Please use aggregate() or map_reduce().",
            DeprecationWarning,
        )

        body = {
            "ns": self._collection_name,
            "initial": initial,
            "$reduce": Code(reduce),
        }

        if isinstance(keys, (bytes, str)):
            body["$keyf"] = Code(keys)
        else:
            body["key"] = self._normalize_fields_projection(keys)

        if condition:
            body["cond"] = condition
        if finalize:
            body["finalize"] = Code(finalize)

        return self._database.command("group", body, **kwargs)

    @timeout
    def filemd5(self, spec, **kwargs):
        if not isinstance(spec, ObjectId):
            raise ValueError(
                "TxMongo: filemd5 expected an objectid for its non-keyword argument."
            )

        return self._database.command(
            "filemd5", spec, root=self._collection_name, **kwargs
        ).addCallback(lambda result: result.get("md5"))

    def _get_write_concern(self, safe=None, **options):
        from_opts = WriteConcern(
            options.get("w"),
            options.get("wtimeout"),
            options.get("j"),
            options.get("fsync"),
        )
        if from_opts.document:
            return from_opts

        if safe is None:
            return self.write_concern
        elif safe:
            if self.write_concern.acknowledged:
                return self.write_concern
            else:
                # Edge case: MongoConnection(w=0).db.coll.insert(..., safe=True)
                # In this case safe=True must issue getLastError without args
                # even if connection-level write concern was unacknowledged
                return WriteConcern()

        return WriteConcern(w=0)

    @timeout
    def insert(self, docs, safe=None, flags=0, **kwargs):
        """Insert a document(s) into this collection.

        *Please consider using new-style* :meth:`insert_one()` *or*
        :meth:`insert_many()` *methods instead.*

        If document doesn't have ``"_id"`` field, :meth:`insert()` will generate
        new :class:`~bson.ObjectId` and set it to ``"_id"`` field of the document.

        :param docs:
            Document or a list of documents to insert into a collection.

        :param safe:
            ``True`` or ``False`` forces usage of respectively acknowledged or
            unacknowledged Write Concern. If ``None``, :attr:`write_concern` is
            used.

        :param flags:
            If zero (default), inserting will stop after the first error
            encountered. When ``flags`` set to
            :const:`txmongo.protocol.INSERT_CONTINUE_ON_ERROR`, MongoDB will
            try to insert all documents passed even if inserting some of
            them will fail (for example, because of duplicate ``_id``). Not
            that :meth:`insert()` won't raise any errors when this flag is
            used.

        :returns:
            :class:`Deferred` that fires with single ``_id`` field or a list of
            ``_id`` fields of inserted documents.
        """
        warnings.warn(
            "Collection.insert() method will be removed in the next version of TxMongo. Please use insert_one() or insert_many().",
            DeprecationWarning,
        )

        if isinstance(docs, dict):
            ids = docs.get("_id", ObjectId())
            docs["_id"] = ids
            docs = [docs]
        elif isinstance(docs, list):
            ids = []
            for doc in docs:
                if isinstance(doc, dict):
                    oid = doc.get("_id", ObjectId())
                    ids.append(oid)
                    doc["_id"] = oid
                else:
                    raise TypeError(
                        "TxMongo: insert takes a document or a list of documents."
                    )
        else:
            raise TypeError("TxMongo: insert takes a document or a list of documents.")

        docs = [BSON.encode(d, codec_options=self.codec_options) for d in docs]
        insert = Insert(flags=flags, collection=str(self), documents=docs)

        def on_proto(proto):
            check_deadline(kwargs.pop("_deadline", None))
            proto.send_INSERT(insert)

            write_concern = self._get_write_concern(safe, **kwargs)
            if write_concern.acknowledged:
                return proto.get_last_error(
                    self._database.name, **write_concern.document
                ).addCallback(lambda _: ids)

            return ids

        return self._database.connection.getprotocol().addCallback(on_proto)

    @timeout
    @defer.inlineCallbacks
    def insert_one(self, document: Document, _deadline=None) -> InsertOneResult:
        """insert_one(document)

        Insert a single document into collection

        :param document: Document to insert

        :returns:
            :class:`Deferred` that called back with
            :class:`pymongo.results.InsertOneResult`
        """
        if "_id" not in document:
            document["_id"] = ObjectId()
        inserted_id = document["_id"]

        msg = Msg(
            flag_bits=Msg.create_flag_bits(self.write_concern.acknowledged),
            body=bson.encode(
                {
                    "insert": self.name,
                    "$db": self.database.name,
                    "writeConcern": self.write_concern.document,
                }
            ),
            payload={
                "documents": [bson.encode(document, codec_options=self.codec_options)],
            },
        )

        proto = yield self._database.connection.getprotocol()
        check_deadline(_deadline)
        response: Optional[Msg] = yield proto.send_MSG(msg)
        if response:
            reply = bson.decode(response.body, codec_options=self.codec_options)
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

    @timeout
    def update(
        self, spec, document, upsert=False, multi=False, safe=None, flags=0, **kwargs
    ):
        """Update document(s) in this collection

        *Please consider using new-style* :meth:`update_one()`, :meth:`update_many()`
        and :meth:`replace_one()` *methods instead.*

        :raises TypeError:
            if `spec` or `document` are not instances of `dict`
            or `upsert` is not an instance of `bool`.

        :param spec:
            query document that selects documents to be updated

        :param document:
            update document to be used for updating or upserting. See
            `MongoDB Update docs
            <https://docs.mongodb.org/manual/tutorial/modify-documents/>`_
            for the format of this document and allowed operators.

        :param upsert:
            perform an upsert if ``True``

        :param multi:
            update all documents that match `spec`, rather than just the first
            matching document. The default value is ``False``.

        :param safe:
            ``True`` or ``False`` forces usage of respectively acknowledged or
            unacknowledged Write Concern. If ``None``, :attr:`write_concern` is
            used.

        :returns:
            :class:`Deferred` that is called back when request is sent to
            MongoDB or confirmed by MongoDB (depending on selected Write Concern).
        """
        warnings.warn(
            "Collection.update() method will be removed in the next version of TxMongo. Please use update_one(), update_many() or replace_one().",
            DeprecationWarning,
        )

        if not isinstance(spec, dict):
            raise TypeError("TxMongo: spec must be an instance of dict.")
        if not isinstance(document, dict):
            raise TypeError("TxMongo: document must be an instance of dict.")
        if not isinstance(upsert, bool):
            raise TypeError("TxMongo: upsert must be an instance of bool.")

        if multi:
            flags |= UPDATE_MULTI
        if upsert:
            flags |= UPDATE_UPSERT

        spec = BSON.encode(spec, codec_options=self.codec_options)
        document = BSON.encode(document, codec_options=self.codec_options)
        update = Update(
            flags=flags, collection=str(self), selector=spec, update=document
        )

        def on_proto(proto):
            check_deadline(kwargs.pop("_deadline", None))
            proto.send_UPDATE(update)

            write_concern = self._get_write_concern(safe, **kwargs)
            if write_concern.acknowledged:
                return proto.get_last_error(
                    self._database.name, **write_concern.document
                )

        return self._database.connection.getprotocol().addCallback(on_proto)

    @defer.inlineCallbacks
    def _update(self, filter, update, upsert, multi, _deadline):
        validate_is_mapping("filter", filter)
        validate_boolean("upsert", upsert)

        msg = Msg(
            flag_bits=Msg.create_flag_bits(self.write_concern.acknowledged),
            body=bson.encode(
                {
                    "update": self.name,
                    "$db": self.database.name,
                    "writeConcern": self.write_concern.document,
                }
            ),
            payload={
                "updates": [
                    bson.encode(
                        {
                            "q": filter,
                            "u": update,
                            "upsert": bool(upsert),
                            "multi": bool(multi),
                        },
                        codec_options=self.codec_options,
                    )
                ],
            },
        )

        proto = yield self._database.connection.getprotocol()
        check_deadline(_deadline)
        response = yield proto.send_MSG(msg)
        reply = None
        if response:
            reply = bson.decode(response.body, codec_options=self.codec_options)
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

        return self._update(filter, replacement, upsert, False, _deadline)

    @timeout
    def save(self, doc, safe=None, **kwargs):
        warnings.warn(
            "Collection.save() method will be removed in the next version of TxMongo. "
            "Please use insert_one() or replace_one().",
            DeprecationWarning,
        )

        if not isinstance(doc, dict):
            raise TypeError(
                "TxMongo: cannot save objects of type {0}".format(type(doc))
            )
        oid = doc.get("_id")
        if oid:
            return self.update({"_id": oid}, doc, safe=safe, upsert=True, **kwargs)
        else:
            return self.insert(doc, safe=safe, **kwargs)

    @timeout
    def remove(self, spec, safe=None, single=False, flags=0, **kwargs):
        warnings.warn(
            "Collection.remove() method will be removed in the next version of TxMongo. Please use delete_one() or delete_many().",
            DeprecationWarning,
        )

        if isinstance(spec, ObjectId):
            spec = SON(dict(_id=spec))
        if not isinstance(spec, dict):
            raise TypeError(
                "TxMongo: spec must be an instance of dict, not {0}".format(type(spec))
            )

        if single:
            flags |= DELETE_SINGLE_REMOVE

        spec = BSON.encode(spec, codec_options=self.codec_options)
        delete = Delete(flags=flags, collection=str(self), selector=spec)

        def on_proto(proto):
            check_deadline(kwargs.pop("_deadline", None))
            proto.send_DELETE(delete)

            write_concern = self._get_write_concern(safe, **kwargs)
            if write_concern.acknowledged:
                return proto.get_last_error(
                    self._database.name, **write_concern.document
                )

        return self._database.connection.getprotocol().addCallback(on_proto)

    @defer.inlineCallbacks
    def _delete(
        self, filter: dict, let: Optional[dict], multi: bool, _deadline: Optional[float]
    ):
        validate_is_mapping("filter", filter)

        body = {
            "delete": self.name,
            "$db": self.database.name,
            "writeConcern": self.write_concern.document,
        }

        if let:
            validate_is_mapping("let", let)
            body["let"] = let

        msg = Msg(
            flag_bits=Msg.create_flag_bits(self.write_concern.acknowledged),
            body=bson.encode(body),
            payload={
                "deletes": [
                    bson.encode(
                        {
                            "q": filter,
                            "limit": 0 if multi else 1,
                        },
                        codec_options=self.codec_options,
                    )
                ],
            },
        )

        proto = yield self._database.connection.getprotocol()
        check_deadline(_deadline)
        response = yield proto.send_MSG(msg)
        reply = None
        if response:
            reply = bson.decode(response.body, codec_options=self.codec_options)
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
        return self._delete(filter, let, multi=False, _deadline=_deadline)

    @timeout
    def delete_many(
        self,
        filter: dict,
        let: Optional[dict] = None,
        _deadline: Optional[float] = None,
    ) -> Deferred[DeleteResult]:
        """delete_many(filter)"""
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

        if "drop_dups" in kwargs:
            index["dropDups"] = kwargs.pop("drop_dups")

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

    def __index_information_3_0(self):
        def on_ok(indexes_info):
            assert indexes_info["cursor"]["id"] == 0
            return indexes_info["cursor"]["firstBatch"]

        codec = CodecOptions(document_class=SON)
        return self._database.command(
            "listIndexes", self.name, codec_options=codec
        ).addCallback(on_ok)

    @timeout
    def index_information(self, _deadline=None):
        """index_information()"""

        def on_3_0_fail(failure):
            failure.trap(OperationFailure)
            return self._database.system.indexes.find(
                {"ns": str(self)}, as_class=SON, _deadline=_deadline
            )

        def on_ok(raw):
            info = {}
            for idx in raw:
                info[idx["name"]] = idx
            return info

        return self.__index_information_3_0().addErrback(on_3_0_fail).addCallback(on_ok)

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
        params = {"map": map, "reduce": reduce}
        params.update(**kwargs)

        def on_ok(raw):
            if full_response:
                return raw
            return raw.get("results")

        return self._database.command("mapreduce", self.name, **params).addCallback(
            on_ok
        )

    @timeout
    def find_and_modify(self, query=None, update=None, upsert=False, **kwargs):
        warnings.warn(
            "Collection.find_and_modify() method will be removed in the next version of TxMongo. "
            "Please use find_one_and_update(), find_one_and_replace() or find_one_and_delete().",
            DeprecationWarning,
        )

        no_obj_error = "No matching object found"

        if not update and not kwargs.get("remove", None):
            raise ValueError("TxMongo: must either update or remove.")

        if update and kwargs.get("remove", None):
            raise ValueError("TxMongo: can't do both update and remove.")

        params = kwargs
        # No need to include empty args
        if query:
            params["query"] = query
        if update:
            params["update"] = update
        if upsert:
            params["upsert"] = upsert

        def on_ok(result):
            if not result["ok"]:
                if result["errmsg"] == no_obj_error:
                    return None
                else:
                    # Should never get here because of allowable_errors
                    raise ValueError("TxMongo: unexpected error '{0}'".format(result))
            return result.get("value")

        return self._database.command(
            "findAndModify",
            self.name,
            allowable_errors=[no_obj_error],
            **params,
        ).addCallback(on_ok)

    # Distinct findAndModify utility method is needed because traditional
    # find_and_modify() accepts `sort` kwarg as dict and passes it to
    # MongoDB command without conversion. But in find_one_and_*
    # methods we want to take `filter.sort` instances
    def _new_find_and_modify(
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

        cmd = SON(
            [
                ("findAndModify", self.name),
                ("query", filter),
                ("new", return_document),
            ]
        )
        cmd.update(kwargs)

        if projection is not None:
            cmd["fields"] = self._normalize_fields_projection(projection)

        if sort is not None:
            cmd["sort"] = SON(sort["orderby"])
        if upsert is not None:
            validate_boolean("upsert", upsert)
            cmd["upsert"] = upsert

        no_obj_error = "No matching object found"

        return self._database.command(
            cmd, allowable_errors=[no_obj_error], _deadline=_deadline
        ).addCallback(lambda result: result.get("value"))

    @timeout
    def find_one_and_delete(self, filter, projection=None, sort=None, _deadline=None):
        """find_one_and_delete(filter, projection=None, sort=None, **kwargs)"""
        return self._new_find_and_modify(
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
        return self._new_find_and_modify(
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
        return self._new_find_and_modify(
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
            _check_command_response(response)
            _merge_command(run, full_result, idx_offset, response)
            return response

        def decode_response(response: Msg, codec_options: CodecOptions) -> dict:
            return bson.decode(response.body, codec_options=codec_options)

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
                deferred = proto.send_MSG(msg)
                if effective_write_concern.acknowledged:
                    deferred.addCallback(decode_response, self.codec_options)
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
