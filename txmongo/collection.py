# Copyright 2009-2015 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from __future__ import absolute_import, division
import bson
from bson import BSON, ObjectId
from bson.code import Code
from bson.son import SON
from pymongo.errors import InvalidName
from pymongo.helpers import _check_write_command_response
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult, \
    DeleteResult
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
    def __init__(self, database, name, write_concern=None):
        if not isinstance(name, (bytes, unicode)):
            raise TypeError("name must be an instance of (bytes, unicode)")

        if not name or ".." in name:
            raise InvalidName("collection names cannot be empty")
        if "$" in name and not (name.startswith("oplog.$main") or
                                name.startswith("$cmd")):
            raise InvalidName("collection names must not contain '$': %r" % name)
        if name[0] == "." or name[-1] == ".":
            raise InvalidName("collection names must not start or end with '.': %r" % name)
        if "\x00" in name:
            raise InvalidName("collection names must not contain the null character")

        self._database = database
        self._collection_name = unicode(name)
        self.__write_concern = write_concern

    def __str__(self):
        return "%s.%s" % (str(self._database), self._collection_name)

    def __repr__(self):
        return "Collection(%s, %s)" % (self._database, self._collection_name)

    @property
    def full_name(self):
        return '{0}.{1}'.format(str(self._database), self._collection_name)

    @property
    def name(self):
        return self._collection_name

    @property
    def database(self):
        return self._database

    def __getitem__(self, collection_name):
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
        return self[collection_name]

    def __call__(self, collection_name):
        return self[collection_name]

    @property
    def write_concern(self):
        return self.__write_concern or self._database.write_concern

    def with_options(self, **kwargs):
        """Get a clone of collection changing the specified settings."""
        # PyMongo's method gets several positional arguments. We support
        # only write_concern for now which is the 3rd positional argument.
        # So we are using **kwargs here to force user's code to specify
        # write_concern as named argument, so adding other args in future
        # won't break compatibility
        write_concern = kwargs.get("write_concern") or self.__write_concern

        return Collection(self._database, self._collection_name,
                          write_concern=write_concern)

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
                raise TypeError("fields must be a list of key names")
            as_dict[field] = 1
        if not as_dict:
            # Empty list should be treated as "_id only"
            as_dict = {"_id": 1}
        return as_dict

    @staticmethod
    def _gen_index_name(keys):
        return u'_'.join([u"%s_%s" % item for item in keys])

    @timeout
    @defer.inlineCallbacks
    def options(self, **kwargs):
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
                    spec['$' + k] = dict(v)
                else:
                    spec['$' + k] = v

        return spec

    @timeout
    def find_with_cursor(self, spec=None, skip=0, limit=0, fields=None, filter=None, **kwargs):
        """ find method that uses the cursor to only return a block of
        results at a time.
        Arguments are the same as with find()
        returns deferred that results in a tuple: (docs, deferred) where
        docs are the current page of results and deferred results in the next
        tuple. When the cursor is exhausted, it will return the tuple
        ([], None)
        """
        if spec is None:
            spec = SON()

        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(fields, (dict, list)) and fields is not None:
            raise TypeError("fields must be an instance of dict or list")
        if not isinstance(skip, int):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, int):
            raise TypeError("limit must be an instance of int")

        fields = self._normalize_fields_projection(fields)

        spec = self.__apply_find_filter(spec, filter)

        as_class = kwargs.get("as_class", dict)
        proto = self._database.connection.getprotocol()

        def after_connection(protocol):
            flags = kwargs.get("flags", 0)

            check_deadline(kwargs.pop("_deadline", None))

            query = Query(flags=flags, collection=str(self),
                          n_to_skip=skip, n_to_return=limit,
                          query=spec, fields=fields)

            deferred_query = protocol.send_QUERY(query)
            deferred_query.addCallback(after_reply, protocol)
            return deferred_query

        def after_reply(reply, protocol, fetched=0):
            documents = reply.documents
            docs_count = len(documents)
            if limit > 0:
                docs_count = min(docs_count, limit - fetched)
            fetched += docs_count

            options = bson.codec_options.CodecOptions(document_class=as_class)
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
                next_reply.addCallback(after_reply, protocol, fetched)
                return out, next_reply

            return out, defer.succeed(([], None))

        proto.addCallback(after_connection)
        return proto

    @timeout
    @defer.inlineCallbacks
    def find_one(self, spec=None, fields=None, **kwargs):
        if isinstance(spec, ObjectId):
            spec = {"_id": spec}

        kwargs.pop("limit", None)   # do not conflict hard limit
        result = yield self.find(
            spec=spec, limit=1, fields=fields, **kwargs)
        defer.returnValue(result[0] if result else None)

    @timeout
    @defer.inlineCallbacks
    def count(self, spec=None, fields=None, **kwargs):
        fields = self._normalize_fields_projection(fields)

        result = yield self._database.command("count", self._collection_name,
                                              query=spec or SON(),
                                              fields=fields, **kwargs)
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
            raise ValueError("filemd5 expected an objectid for its "
                             "non-keyword argument")

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
                    raise TypeError("insert takes a document or a list of documents")
        else:
            raise TypeError("insert takes a document or a list of documents")

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
            _check_write_command_response([[0, response]])
        else:
            # falling back to OP_INSERT in case of unacknowledged op
            flags = INSERT_CONTINUE_ON_ERROR if not ordered else 0
            inserted_ids = yield self.insert(documents, flags=flags, **kwargs)

        defer.returnValue(inserted_ids)

    @timeout
    @defer.inlineCallbacks
    def insert_one(self, document, **kwargs):
        inserted_ids = yield self._insert_one_or_many([document], **kwargs)
        defer.returnValue(InsertOneResult(inserted_ids[0], self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def insert_many(self, documents, ordered=True, **kwargs):
        inserted_ids = yield self._insert_one_or_many(documents, ordered, **kwargs)
        defer.returnValue(InsertManyResult(inserted_ids, self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def update(self, spec, document, upsert=False, multi=False, safe=None, flags=0, **kwargs):
        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(document, dict):
            raise TypeError("document must be an instance of dict")
        if not isinstance(upsert, bool):
            raise TypeError("upsert must be an instance of bool")

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
        validate_ok_for_update(update)

        raw_response = yield self._update(filter, update, upsert, multi=False, **kwargs)
        defer.returnValue(UpdateResult(raw_response, self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def update_many(self, filter, update, upsert=False, **kwargs):
        validate_ok_for_update(update)

        raw_response = yield self._update(filter, update, upsert, multi=True, **kwargs)
        defer.returnValue(UpdateResult(raw_response, self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def replace_one(self, filter, replacement, upsert=False, **kwargs):
        validate_ok_for_replace(replacement)

        raw_response = yield self._update(filter, replacement, upsert, multi=False, **kwargs)
        defer.returnValue(UpdateResult(raw_response, self.write_concern.acknowledged))

    @timeout
    @defer.inlineCallbacks
    def save(self, doc, safe=None, **kwargs):
        if not isinstance(doc, dict):
            raise TypeError("cannot save objects of type %s" % type(doc))
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
            raise TypeError("spec must be an instance of dict, not %s" % type(spec))

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
            raise TypeError("sort_fields must be an instance of filter.sort")

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
            raise TypeError("index_identifier must be a name or instance of filter.sort")

        result = yield self._database.command("deleteIndexes", self._collection_name,
                                              index=name, allowable_errors=["ns not found"],
                                              **kwargs)
        defer.returnValue(result)

    @timeout
    @defer.inlineCallbacks
    def drop_indexes(self, **kwargs):
        result = yield self.drop_index("*", **kwargs)
        defer.returnValue(result)

    @timeout
    @defer.inlineCallbacks
    def index_information(self, **kwargs):
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
            raise ValueError("Must either update or remove")

        if update and kwargs.get("remove", None):
            raise ValueError("Can't do both update and remove")

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
                raise ValueError("Unexpected Error: %s" % (result,))
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
            raise ValueError("return_document must be ReturnDocument.BEFORE "
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
