# Copyright 2009-2015 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from __future__ import absolute_import, division
import io
import struct
import bson
import collections
from bson import BSON, ObjectId
from bson.code import Code
from bson.son import SON
from pymongo.bulk import _Bulk, _COMMANDS, _merge_command
from pymongo.errors import InvalidName, BulkWriteError, InvalidOperation, OperationFailure
from pymongo.helpers import _check_write_command_response
from pymongo.operations import _WriteOp
from pymongo.message import _OP_MAP, _INSERT
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult, \
    DeleteResult, BulkWriteResult
from pymongo.common import validate_ok_for_update, validate_ok_for_replace, \
    validate_is_mapping, validate_boolean
from pymongo.collection import ReturnDocument
from pymongo.write_concern import WriteConcern
from txmongo.protocol import DELETE_SINGLE_REMOVE, UPDATE_UPSERT, UPDATE_MULTI, \
    Query, Getmore, Insert, Update, Delete, KillCursors, INSERT_CONTINUE_ON_ERROR
from txmongo.utils import check_deadline, timeout
from txmongo import filter as qf
from twisted.internet import defer
from twisted.python.compat import unicode, comparable


@comparable
class Collection(object):
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
        if not isinstance(name, (bytes, unicode)):
            raise TypeError("TxMongo: name must be an instance of (bytes, unicode).")

        if not name or ".." in name:
            raise InvalidName("TxMongo: collection names cannot be empty.")
        if "$" in name and not (name.startswith("oplog.$main") or
                                name.startswith("$cmd")):
            msg = "TxMongo: collection names must not contain '$', '{0}'".format(repr(name))
            raise InvalidName(msg)
        if name[0] == "." or name[-1] == ".":
            msg = "TxMongo: collection names must not start or end with '.', '{0}'".format(repr(name))
            raise InvalidName(msg)
        if "\x00" in name:
            raise InvalidName("TxMongo: collection names must not contain the null character.")

        self._database = database
        self._collection_name = unicode(name)
        self.__write_concern = write_concern
        self.__codec_options = codec_options

    def __str__(self):
        return "%s.%s" % (str(self._database), self._collection_name)

    def __repr__(self):
        return "Collection(%s, %s)" % (self._database, self._collection_name)

    @property
    def full_name(self):
        """Full name of this :class:`Collection`, i.e.
        `db_name.collection_name`"""
        return '{0}.{1}'.format(str(self._database), self._collection_name)

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
        return Collection(self._database,
                          "%s.%s" % (self._collection_name, collection_name))

    def __cmp__(self, other):
        if isinstance(other, Collection):

            def cmp(a, b):
                return (a > b) - (a < b)

            return cmp((self._database, self._collection_name),
                       (other._database, other._collection_name))
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
        """Get a clone of collection changing the specified settings.

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

        return Collection(self._database, self._collection_name,
                          write_concern=write_concern,
                          codec_options=codec_options)

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
            if not isinstance(field, (bytes, unicode)):
                raise TypeError("TxMongo: fields must be a list of key names.")
            as_dict[field] = 1
        if not as_dict:
            # Empty list should be treated as "_id only"
            as_dict = {"_id": 1}
        return as_dict

    @staticmethod
    def _gen_index_name(keys):
        return u'_'.join([u"%s_%s" % item for item in keys])

    @defer.inlineCallbacks
    def _list_collections_3_0(self):
        response = yield self._database.command(SON([("listCollections", 1),
                                                     ("filter", {"name": self.name})]))
        assert response["cursor"]["id"] == 0
        first_batch = response["cursor"]["firstBatch"]
        if first_batch:
            defer.returnValue(first_batch[0])
        else:
            defer.returnValue(None)

    @timeout
    @defer.inlineCallbacks
    def options(self, **kwargs):
        """Get the options set on this collection.

        :returns:
            :class:`Deferred` that called back with dictionary of options
            and their values.
        """
        try:
            result = yield self._list_collections_3_0()
        except OperationFailure:
            result = yield self._database.system.namespaces.find_one({"name": str(self)}, **kwargs)
        if not result:
            result = {}
        options = result.get("options", {})
        if "create" in options:
            del options["create"]
        defer.returnValue(options)

    @timeout
    @defer.inlineCallbacks
    def find(self, spec=None, skip=0, limit=0, fields=None, filter=None, cursor=False, **kwargs):
        """Find documents in a collection.

        The `spec` argument is a MongoDB query document. To return all documents
        in a collection, omit this parameter or pass an empty document (``{}``).
        You can pass ``{"key": "value"}`` to select documents having ``key``
        field equal to ``"value"`` or use any of `MongoDB's query selectors
        <https://docs.mongodb.org/manual/reference/operator/query/#query-selectors>`_.

        Ordering, indexing hints and other query parameters can be set with
        `filter` argument. See :mod:`txmongo.filter` for details.

        :param skip:
            the number of documents to omit from the start of the result set.

        :param limit:
            the maximum number of documents to return. All documents are
            returned when `limit` is zero.

        :param fields:
            a list of field names that should be returned for each document
            in the result set or a dict specifying field names to include or
            exclude. If `fields` is a list ``_id`` fields will always be
            returned. Use a dict form to exclude fields:
            ``fields={"_id": False}``.

        :param as_class: *(keyword only)*
            if not ``None``, returned documents will be converted to type
            specified. For example, you can use ``as_class=collections.OrderedDict``
            or ``as_class=bson.SON`` when field ordering in documents is important.

        :returns: an instance of :class:`Deferred` that called back with one of:

            - if `cursor` is ``False`` (the default) --- all documents found
            - if `cursor` is ``True`` --- tuple of ``(docs, dfr)``, where
              ``docs`` is a partial result, returned by MongoDB in a first
              batch and ``dfr`` is a :class:`Deferred` that fires with next
              ``(docs, dfr)``. Last result will be ``([], None)``. Using this
              mode you can iterate over the result set with code like that:
              ::
                  @defer.inlineCallbacks
                  def query():
                      docs, dfr = yield coll.find(query, cursor=True)
                      while docs:
                          for doc in docs:
                              do_something(doc)
                          docs, dfr = yield dfr
        """
        docs, dfr = yield self.find_with_cursor(spec=spec, skip=skip, limit=limit,
                                                fields=fields, filter=filter, **kwargs)

        if cursor:
            defer.returnValue((docs, dfr))

        result = []
        while docs:
            result.extend(docs)
            docs, dfr = yield dfr

        defer.returnValue(result)

    @staticmethod
    def __apply_find_filter(spec, c_filter):
        if c_filter:
            if "query" not in spec:
                spec = {"$query": spec}

            for k, v in c_filter.items():
                if isinstance(v, (list, tuple)):
                    spec['$' + k] = SON(v)
                else:
                    spec['$' + k] = v

        return spec

    @timeout
    def find_with_cursor(self, spec=None, skip=0, limit=0, fields=None, filter=None, **kwargs):
        """Find documents in a collection and return them in one batch at a time

        This methid is equivalent of :meth:`find()` with `cursor=True`.
        See :meth:`find()` for description of parameters and return value.
        """
        if spec is None:
            spec = SON()

        if not isinstance(spec, dict):
            raise TypeError("TxMongo: spec must be an instance of dict.")
        if not isinstance(fields, (dict, list)) and fields is not None:
            raise TypeError("TxMongo: fields must be an instance of dict or list.")
        if not isinstance(skip, int):
            raise TypeError("TxMongo: skip must be an instance of int.")
        if not isinstance(limit, int):
            raise TypeError("TxMongo: limit must be an instance of int.")

        fields = self._normalize_fields_projection(fields)

        spec = self.__apply_find_filter(spec, filter)

        as_class = kwargs.get("as_class")
        proto = self._database.connection.getprotocol()

        def after_connection(protocol):
            flags = kwargs.get("flags", 0)

            check_deadline(kwargs.pop("_deadline", None))

            query = Query(flags=flags, collection=str(self),
                          n_to_skip=skip, n_to_return=limit,
                          query=spec, fields=fields)

            deferred_query = protocol.send_QUERY(query)
            deferred_query.addCallback(after_reply, protocol, after_reply)
            return deferred_query

        # this_func argument is just a reference to after_reply function itself.
        # after_reply can reference to itself directly but this will create a circular
        # reference between closure and function object which will add unnecessary
        # work for GC.
        def after_reply(reply, protocol, this_func, fetched=0):
            documents = reply.documents
            docs_count = len(documents)
            if limit > 0:
                docs_count = min(docs_count, limit - fetched)
            fetched += docs_count

            options = self.codec_options
            if as_class is not None:
                options = options._replace(document_class=as_class)
            out = [document.decode(codec_options=options) for document in documents[:docs_count]]

            if reply.cursor_id:
                if limit == 0:
                    to_fetch = 0  # no limit
                elif limit < 0:
                    # We won't actually get here because MongoDB won't
                    # create cursor when limit < 0
                    to_fetch = None
                else:
                    to_fetch = limit - fetched
                    if to_fetch <= 0:
                        to_fetch = None  # close cursor

                if to_fetch is None:
                    protocol.send_KILL_CURSORS(KillCursors(cursors=[reply.cursor_id]))
                    return out, defer.succeed(([], None))

                next_reply = protocol.send_GETMORE(Getmore(
                    collection=str(self), cursor_id=reply.cursor_id,
                    n_to_return=to_fetch
                ))
                next_reply.addCallback(this_func, protocol, this_func, fetched)
                return out, next_reply

            return out, defer.succeed(([], None))

        proto.addCallback(after_connection)
        return proto

    @timeout
    @defer.inlineCallbacks
    def find_one(self, spec=None, fields=None, **kwargs):
        """Get a single document from the collection.

        All arguments to :meth:`find()` are also valid for :meth:`find_one()`,
        although `limit` will be ignored.

        :returns:
            a :class:`Deferred` that called back with single document
            or ``None`` if no matching documents is found.
        """
        if isinstance(spec, ObjectId):
            spec = {"_id": spec}

        kwargs.pop("limit", None)   # do not conflict hard limit
        result = yield self.find(
            spec=spec, limit=1, fields=fields, **kwargs)
        defer.returnValue(result[0] if result else None)

    @timeout
    @defer.inlineCallbacks
    def count(self, spec=None, **kwargs):
        """Get the number of documents in this collection.

        :param spec:
            argument is a query document that selects which documents to
            count in the collection.

        :param hint: *(keyword only)*
            :class:`~txmongo.filter.hint` instance specifying index to use.

        :returns: a :class:`Deferred` that called back with a number of
                  documents matching the criteria.
        """
        result = yield self._database.command("count", self._collection_name,
                                              query=spec or SON(), **kwargs)
        defer.returnValue(int(result["n"]))

    @timeout
    @defer.inlineCallbacks
    def group(self, keys, initial, reduce, condition=None, finalize=None, **kwargs):
        body = {
            "ns": self._collection_name,
            "initial": initial,
            "$reduce": Code(reduce),
        }

        if isinstance(keys, (bytes, unicode)):
            body["$keyf"] = Code(keys)
        else:
            body["key"] = self._normalize_fields_projection(keys)

        if condition:
            body["cond"] = condition
        if finalize:
            body["finalize"] = Code(finalize)

        result = yield self._database.command("group", body, **kwargs)
        defer.returnValue(result)

    @timeout
    @defer.inlineCallbacks
    def filemd5(self, spec, **kwargs):
        if not isinstance(spec, ObjectId):
            raise ValueError("TxMongo: filemd5 expected an objectid for its non-keyword argument.")

        result = yield self._database.command("filemd5", spec, root=self._collection_name, **kwargs)
        defer.returnValue(result.get("md5"))

    def _get_write_concern(self, safe=None, **options):
        from_opts = WriteConcern(options.get("w"),
                                 options.get("wtimeout"),
                                 options.get("j"),
                                 options.get("fsync"))
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
    @defer.inlineCallbacks
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
                    raise TypeError("TxMongo: insert takes a document or a list of documents.")
        else:
            raise TypeError("TxMongo: insert takes a document or a list of documents.")

        docs = [BSON.encode(d) for d in docs]
        insert = Insert(flags=flags, collection=str(self), documents=docs)

        proto = yield self._database.connection.getprotocol()
        check_deadline(kwargs.pop("_deadline", None))
        proto.send_INSERT(insert)

        write_concern = self._get_write_concern(safe, **kwargs)
        if write_concern.acknowledged:
            yield proto.get_last_error(str(self._database), **write_concern.document)

        defer.returnValue(ids)

    @defer.inlineCallbacks
    def _insert_one_or_many(self, documents, ordered=True, **kwargs):
        if self.write_concern.acknowledged:
            inserted_ids = []
            for doc in documents:
                if "_id" not in doc:
                    doc["_id"] = ObjectId()
                inserted_ids.append(doc["_id"])

            command = SON([("insert", self._collection_name),
                           ("documents", documents),
                           ("ordered", ordered),
                           ("writeConcern", self.write_concern.document)])
            response = yield self._database.command(command, **kwargs)
        else:
            # falling back to OP_INSERT in case of unacknowledged op
            flags = INSERT_CONTINUE_ON_ERROR if not ordered else 0
            inserted_ids = yield self.insert(documents, flags=flags, **kwargs)
            response = None

        defer.returnValue((inserted_ids, response))

    @timeout
    @defer.inlineCallbacks
    def insert_one(self, document, **kwargs):
        """Insert a single document into collection

        :param document: Document to insert
        :returns:
            :class:`Deferred` that called back with
            :class:`pymongo.results.InsertOneResult`
        """
        inserted_ids, response = yield self._insert_one_or_many([document], **kwargs)
        if response:
            _check_write_command_response([[0, response]])
        defer.returnValue(InsertOneResult(inserted_ids[0], self.write_concern.acknowledged))

    @staticmethod
    def _generate_batch_commands(collname, command, docs_field, documents, ordered,
                                 write_concern, max_bson, max_count):
        # Takes a list of documents and generates one or many `insert` commands
        # with documents list in each command is less or equal to max_bson bytes
        # and contains less or equal documents than max_count

        # Manually composing command in BSON form because this way we can
        # perform costly documents serialization only once

        msg = SON([(command, collname),
                   ("ordered", ordered),
                   ("writeConcern", write_concern.document)])

        buf = io.BytesIO()
        buf.write(BSON.encode(msg))
        buf.seek(-1, io.SEEK_END)  # -1 because we don't need final NUL from partial command
        buf.write(docs_field)  # type, name and length placeholder of 'documents' array
        docs_start = buf.tell() - 4

        def prepare_command():
            docs_end = buf.tell() + 1  # +1 for final NUL for 'documents'
            buf.write(b'\x00\x00')  # final NULs for 'documents' and the command itself
            total_length = buf.tell()

            # writing 'documents' length
            buf.seek(docs_start)
            buf.write(struct.pack('<i', docs_end - docs_start))

            # writing total message length
            buf.seek(0)
            buf.write(struct.pack('<i', total_length))

            return BSON(buf.getvalue())

        idx = 0
        idx_offset = 0
        for doc in documents:
            key = str(idx).encode('ascii')
            value = BSON.encode(doc)

            enough_size = buf.tell() + len(key)+2 + len(value) - docs_start > max_bson
            enough_count = idx >= max_count
            if enough_size or enough_count:
                yield idx_offset, prepare_command()

                buf.seek(docs_start + 4)
                buf.truncate()

                idx_offset += idx
                idx = 0
                key = b'0'

            buf.write(b'\x03' + key + b'\x00')  # type and key of document
            buf.write(value)

            idx += 1

        yield idx_offset, prepare_command()

    @timeout
    @defer.inlineCallbacks
    def insert_many(self, documents, ordered=True, **kwargs):
        """Insert an iterable of documents into collection

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
        for doc in documents:
            if isinstance(doc, collections.Mapping):
                inserted_ids.append(doc.setdefault("_id", ObjectId()))
            else:
                raise TypeError("TxMongo: insert_many takes list of documents.")

        bulk = _Bulk(self, ordered, bypass_document_validation=False)
        bulk.ops = [(_INSERT, doc) for doc in documents]
        yield self._execute_bulk(bulk)
        defer.returnValue(InsertManyResult(inserted_ids, self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def update(self, spec, document, upsert=False, multi=False, safe=None, flags=0, **kwargs):
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

        spec = BSON.encode(spec)
        document = BSON.encode(document)
        update = Update(flags=flags, collection=str(self),
                        selector=spec, update=document)

        proto = yield self._database.connection.getprotocol()
        check_deadline(kwargs.pop("_deadline", None))
        proto.send_UPDATE(update)

        write_concern = self._get_write_concern(safe, **kwargs)
        if write_concern.acknowledged:
            ret = yield proto.get_last_error(str(self._database), **write_concern.document)
            defer.returnValue(ret)

    @defer.inlineCallbacks
    def _update(self, filter, update, upsert, multi, **kwargs):
        validate_is_mapping("filter", filter)
        validate_boolean("upsert", upsert)

        if self.write_concern.acknowledged:
            updates = [SON([('q', filter), ('u', update),
                            ("upsert", upsert), ("multi", multi)])]

            command = SON([("update", self._collection_name),
                           ("updates", updates),
                           ("writeConcern", self.write_concern.document)])
            raw_response = yield self._database.command(command, **kwargs)
            _check_write_command_response([[0, raw_response]])

            # Extract upserted_id from returned array
            if raw_response.get("upserted"):
                raw_response["upserted"] = raw_response["upserted"][0]["_id"]

        else:
            yield self.update(filter, update, upsert=upsert, multi=multi, **kwargs)
            raw_response = None

        defer.returnValue(raw_response)

    @timeout
    @defer.inlineCallbacks
    def update_one(self, filter, update, upsert=False, **kwargs):
        """Update a single document matching the filter.

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

        raw_response = yield self._update(filter, update, upsert, multi=False, **kwargs)
        defer.returnValue(UpdateResult(raw_response, self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def update_many(self, filter, update, upsert=False, **kwargs):
        """Update one or more documents that match the filter.

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

        raw_response = yield self._update(filter, update, upsert, multi=True, **kwargs)
        defer.returnValue(UpdateResult(raw_response, self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def replace_one(self, filter, replacement, upsert=False, **kwargs):
        """Replace a single document matching the filter.

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

        raw_response = yield self._update(filter, replacement, upsert, multi=False, **kwargs)
        defer.returnValue(UpdateResult(raw_response, self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def save(self, doc, safe=None, **kwargs):
        if not isinstance(doc, dict):
            raise TypeError("TxMongo: cannot save objects of type {0}".format(type(doc)))
        oid = doc.get("_id")
        if oid:
            result = yield self.update(
                {"_id": oid}, doc, safe=safe, upsert=True, **kwargs)
        else:
            result = yield self.insert(doc, safe=safe, **kwargs)

        defer.returnValue(result)

    @timeout
    @defer.inlineCallbacks
    def remove(self, spec, safe=None, single=False, flags=0, **kwargs):
        if isinstance(spec, ObjectId):
            spec = SON(dict(_id=spec))
        if not isinstance(spec, dict):
            raise TypeError("TxMongo: spec must be an instance of dict, not {0}".format(type(spec)))

        if single:
            flags |= DELETE_SINGLE_REMOVE

        spec = BSON.encode(spec)
        delete = Delete(flags=flags, collection=str(self), selector=spec)
        proto = yield self._database.connection.getprotocol()
        check_deadline(kwargs.pop("_deadline", None))
        proto.send_DELETE(delete)

        write_concern = self._get_write_concern(safe, **kwargs)
        if write_concern.acknowledged:
            ret = yield proto.get_last_error(str(self._database), **write_concern.document)
            defer.returnValue(ret)

    @defer.inlineCallbacks
    def _delete(self, filter, multi, **kwargs):
        validate_is_mapping("filter", filter)

        if self.write_concern.acknowledged:
            deletes = [SON([('q', filter), ("limit", 0 if multi else 1)])]
            command = SON([("delete", self._collection_name),
                           ("deletes", deletes),
                           ("writeConcern", self.write_concern.document)])

            raw_response = yield self._database.command(command, **kwargs)
            _check_write_command_response([[0, raw_response]])

        else:
            yield self.remove(filter, single=not multi, **kwargs)
            raw_response = None

        defer.returnValue(raw_response)

    @timeout
    @defer.inlineCallbacks
    def delete_one(self, filter, **kwargs):
        raw_response = yield self._delete(filter, multi=False, **kwargs)
        defer.returnValue(DeleteResult(raw_response, self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def delete_many(self, filter, **kwargs):
        raw_response = yield self._delete(filter, multi=True, **kwargs)
        defer.returnValue(DeleteResult(raw_response, self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def drop(self, **kwargs):
        result = yield self._database.drop_collection(self._collection_name, **kwargs)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def create_index(self, sort_fields, **kwargs):
        if not isinstance(sort_fields, qf.sort):
            raise TypeError("TxMongo: sort_fields must be an instance of filter.sort")

        if "name" not in kwargs:
            name = self._gen_index_name(sort_fields["orderby"])
        else:
            name = kwargs.pop("name")

        key = SON()
        for k, v in sort_fields["orderby"]:
            key.update({k: v})

        index = SON(dict(
            ns=str(self),
            name=name,
            key=key
        ))

        if "drop_dups" in kwargs:
            kwargs["dropDups"] = kwargs.pop("drop_dups")

        if "bucket_size" in kwargs:
            kwargs["bucketSize"] = kwargs.pop("bucket_size")

        index.update(kwargs)
        yield self._database.system.indexes.insert(index, safe=True, **kwargs)
        defer.returnValue(name)

    @timeout
    @defer.inlineCallbacks
    def ensure_index(self, sort_fields, **kwargs):
        # ensure_index is an alias of create_index since we are not
        # keeping an index cache same way pymongo does
        result = yield self.create_index(sort_fields, **kwargs)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def drop_index(self, index_identifier, **kwargs):
        if isinstance(index_identifier, (bytes, unicode)):
            name = index_identifier
        elif isinstance(index_identifier, qf.sort):
            name = self._gen_index_name(index_identifier["orderby"])
        else:
            raise TypeError("TxMongo: index_identifier must be a name or instance of filter.sort")

        result = yield self._database.command("deleteIndexes", self._collection_name,
                                              index=name, allowable_errors=["ns not found"],
                                              **kwargs)
        defer.returnValue(result)

    @timeout
    @defer.inlineCallbacks
    def drop_indexes(self, **kwargs):
        result = yield self.drop_index("*", **kwargs)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def __index_information_3_0(self):
        indexes_info = yield self._database.command("listIndexes", self.name)
        assert indexes_info["cursor"]["id"] == 0
        defer.returnValue(indexes_info["cursor"]["firstBatch"])

    @timeout
    @defer.inlineCallbacks
    def index_information(self, **kwargs):
        try:
            raw = yield self.__index_information_3_0()
        except OperationFailure:
            raw = yield self._database.system.indexes.find({"ns": str(self)}, **kwargs)

        info = {}
        for idx in raw:
            info[idx["name"]] = idx
        defer.returnValue(info)

    @timeout
    @defer.inlineCallbacks
    def rename(self, new_name, **kwargs):
        to = "%s.%s" % (str(self._database), new_name)
        result = yield self._database("admin").command(
            "renameCollection", str(self), to=to, **kwargs)
        defer.returnValue(result)

    @timeout
    @defer.inlineCallbacks
    def distinct(self, key, spec=None, **kwargs):
        params = {"key": key}
        if spec:
            params["query"] = spec
        params.update(kwargs)

        result = yield self._database.command("distinct", self._collection_name, **params)
        defer.returnValue(result.get("values"))

    @timeout
    @defer.inlineCallbacks
    def aggregate(self, pipeline, full_response=False, **kwargs):
        raw = yield self._database.command(
            "aggregate", self._collection_name, pipeline=pipeline, **kwargs)
        if full_response:
            defer.returnValue(raw)
        defer.returnValue(raw.get("result"))

    @timeout
    @defer.inlineCallbacks
    def map_reduce(self, map, reduce, full_response=False, **kwargs):
        params = {"map": map, "reduce": reduce}
        params.update(**kwargs)
        raw = yield self._database.command("mapreduce", self._collection_name, **params)
        if full_response:
            defer.returnValue(raw)
        defer.returnValue(raw.get("results"))

    @timeout
    @defer.inlineCallbacks
    def find_and_modify(self, query=None, update=None, upsert=False, **kwargs):
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

        result = yield self._database.command("findAndModify", self._collection_name,
                                              allowable_errors=[no_obj_error],
                                              **params)
        if not result["ok"]:
            if result["errmsg"] == no_obj_error:
                defer.returnValue(None)
            else:
                # Should never get here because of allowable_errors
                raise ValueError("TxMongo: unexpected error '{0}'".format(result))
        defer.returnValue(result.get("value"))

    # Distinct findAndModify utility method is needed because traditional
    # find_and_modify() accepts `sort` kwarg as dict and passes it to
    # MongoDB command without conversion. But in find_one_and_*
    # methods we want to take `filter.sort` instances
    @defer.inlineCallbacks
    def _new_find_and_modify(self, filter, projection, sort, upsert=None,
                             return_document=ReturnDocument.BEFORE, **kwargs):
        validate_is_mapping("filter", filter)
        if not isinstance(return_document, bool):
            raise ValueError("TxMongo: return_document must be ReturnDocument.BEFORE "
                             "or ReturnDocument.AFTER")

        cmd = SON([("findAndModify", self._collection_name),
                   ("query", filter),
                   ("new", return_document)])
        cmd.update(kwargs)

        if projection is not None:
            cmd["fields"] = self._normalize_fields_projection(projection)

        if sort is not None:
            cmd["sort"] = dict(sort["orderby"])
        if upsert is not None:
            validate_boolean("upsert", upsert)
            cmd["upsert"] = upsert

        no_obj_error = "No matching object found"

        result = yield self._database.command(cmd, allowable_errors=[no_obj_error], **kwargs)
        defer.returnValue(result.get("value"))

    @timeout
    @defer.inlineCallbacks
    def find_one_and_delete(self, filter, projection=None, sort=None, **kwargs):
        result = yield self._new_find_and_modify(filter, projection, sort, remove=True, **kwargs)
        defer.returnValue(result)

    @timeout
    @defer.inlineCallbacks
    def find_one_and_replace(self, filter, replacement, projection=None, sort=None,
                             upsert=False, return_document=ReturnDocument.BEFORE, **kwargs):
        validate_ok_for_replace(replacement)
        result = yield self._new_find_and_modify(filter, projection, sort, upsert, return_document,
                                                 update=replacement, **kwargs)
        defer.returnValue(result)

    @timeout
    @defer.inlineCallbacks
    def find_one_and_update(self, filter, update, projection=None, sort=None,
                            upsert=False, return_document=ReturnDocument.BEFORE, **kwargs):
        validate_ok_for_update(update)
        result = yield self._new_find_and_modify(filter, projection, sort, upsert, return_document,
                                                 update=update, **kwargs)
        defer.returnValue(result)

    def bulk_write(self, requests, ordered=True):
        if not isinstance(requests, collections.Iterable):
            raise TypeError("requests must be iterable")

        requests = list(requests)

        blk = _Bulk(self, ordered, bypass_document_validation=False)
        for request in requests:
            if not isinstance(request, _WriteOp):
                raise TypeError("{} is not a valid request".format(request))
            request._add_to_bulk(blk)

        return self._execute_bulk(blk)

    @defer.inlineCallbacks
    def _execute_bulk(self, bulk):
        if not bulk.ops:
            raise InvalidOperation("No operations to execute")
        if bulk.executed:
            raise InvalidOperation("Bulk operations can only be executed once")

        bulk.executed = True

        if bulk.ordered:
            generator = bulk.gen_ordered()
        else:
            generator = bulk.gen_unordered()

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

        for run in generator:
            result = yield self._execute_batch_command(run.op_type, run.ops, bulk.ordered)
            _merge_command(run, full_result, result)

            if bulk.ordered and full_result["writeErrors"]:
                break

        if self.write_concern.acknowledged:
            if full_result["writeErrors"] or full_result["writeConcernErrors"]:
                if full_result["writeErrors"]:
                    full_result["writeErrors"].sort(key=lambda error: error["index"])
                raise BulkWriteError(full_result)

        defer.returnValue(BulkWriteResult(full_result, self.write_concern.acknowledged))

    @defer.inlineCallbacks
    def _execute_batch_command(self, command_type, documents, ordered):
        assert command_type in _OP_MAP

        cmd_collname = str(self._database["$cmd"])
        proto = yield self._database.connection.getprotocol()

        results = []

        def accumulate_result(reply, idx_offset):
            result = reply.documents[0].decode()
            results.append((idx_offset, result))
            return result

        # There are four major cases with different behavior of insert_many:
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

        actual_write_concern = self.write_concern
        if ordered and self.write_concern.acknowledged is False:
            actual_write_concern = WriteConcern(w=1)

        batches = self._generate_batch_commands(self._collection_name, _COMMANDS[command_type],
                                                _OP_MAP[command_type], documents, ordered,
                                                actual_write_concern, proto.max_bson_size,
                                                proto.max_write_batch_size)

        all_responses = []
        for idx_offset, batch in batches:
            batch_result = proto.send_QUERY(Query(collection=cmd_collname, query=batch))
            if self.write_concern.acknowledged or ordered:
                batch_result.addCallback(accumulate_result, idx_offset)
                if ordered:
                    result = yield batch_result
                    if "writeErrors" in result:
                        break
                else:
                    all_responses.append(batch_result)

        if self.write_concern.acknowledged and not ordered:
            try:
                yield defer.gatherResults(all_responses, consumeErrors=True)
            except defer.FirstError as e:
                e.subFailure.raiseException()

        defer.returnValue(results)
