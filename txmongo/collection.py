# Copyright 2009-2015 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

import types

import bson
from bson import BSON, ObjectId
from bson.code import Code
from bson.son import SON
from pymongo.errors import InvalidName
from txmongo import filter as qf
from txmongo.protocol import DELETE_SINGLE_REMOVE, UPDATE_UPSERT, UPDATE_MULTI, \
    Query, Getmore, Insert, Update, Delete, KillCursors
from twisted.internet import defer


class Collection(object):
    def __init__(self, database, name):
        if not isinstance(name, basestring):
            raise TypeError("name must be an instance of basestring")

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

    def __str__(self):
        return "%s.%s" % (str(self._database), self._collection_name)

    def __repr__(self):
        return "Collection(%s, %s)" % (self._database, self._collection_name)

    def __getitem__(self, collection_name):
        return Collection(self._database,
                          "%s.%s" % (self._collection_name, collection_name))

    def __cmp__(self, other):
        if isinstance(other, Collection):
            return cmp((self._database, self._collection_name),
                       (other._database, other._collection_name))
        return NotImplemented

    def __getattr__(self, collection_name):
        return self[collection_name]

    def __call__(self, collection_name):
        return self[collection_name]

    @staticmethod
    def _fields_list_to_dict(fields):
        """
        transform a list of fields from ["a", "b"] to {"a":1, "b":1}
        """
        as_dict = {}
        for field in fields:
            if not isinstance(field, types.StringTypes):
                raise TypeError("fields must be a list of key names")
            as_dict[field] = 1
        return as_dict

    @staticmethod
    def _gen_index_name(keys):
        return u'_'.join([u"%s_%s" % item for item in keys])

    def options(self):
        def wrapper(result):
            if result:
                options = result.get("options", {})
                if "create" in options:
                    del options["create"]
                return options
            return {}

        deferred_find_one = self._database.system.namespaces.find_one({"name": str(self)})
        deferred_find_one.addCallback(wrapper)
        return deferred_find_one


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

    def __apply_find_filter(self, spec, filter):
        if filter:
            if "query" not in spec:
                spec = {"$query": spec}

            for k, v in filter.iteritems():
                if isinstance(v, (list, tuple)):
                    spec['$' + k] = dict(v)
                else:
                    spec['$' + k] = v

        return spec

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

        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(fields, (types.DictType, types.ListType, types.NoneType)):
            raise TypeError("fields must be an instance of dict or list")
        if not isinstance(skip, types.IntType):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, types.IntType):
            raise TypeError("limit must be an instance of int")

        if fields is not None:
            if not isinstance(fields, types.DictType):
                if not fields:
                    fields = ["_id"]
                fields = self._fields_list_to_dict(fields)

        spec = self.__apply_find_filter(spec, filter)

        as_class = kwargs.get("as_class", dict)
        deferred_protocol = self._database.connection.getprotocol()

        def after_connection(proto):
            flags = kwargs.get("flags", 0)
            query = Query(flags=flags, collection=str(self),
                          n_to_skip=skip, n_to_return=limit,
                          query=spec, fields=fields)

            deferred_query = proto.send_QUERY(query)
            deferred_query.addCallback(after_reply, proto)
            return deferred_query

        def after_reply(reply, proto, fetched=0):
            documents = reply.documents
            docs_count = len(documents)
            if limit > 0:
                docs_count = min(docs_count, limit - fetched)
            fetched += docs_count

            try:
                # as_class is removed from PyMongo >= 3.0
                # trying to use CodecOptions instead
                options = bson.codec_options.CodecOptions(document_class=as_class)
                out = [document.decode(codec_options=options) for document in documents[:docs_count]]
            except AttributeError:
                # Falling back to as_class for PyMongo < 3.0
                out = [document.decode(as_class=as_class) for document in documents[:docs_count]]

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
                    proto.send_KILL_CURSORS(KillCursors(cursors=[reply.cursor_id]))
                    return out, defer.succeed(([], None))

                next_reply = proto.send_GETMORE(Getmore(
                    collection=str(self), cursor_id=reply.cursor_id,
                    n_to_return=to_fetch
                ))
                next_reply.addCallback(after_reply, proto, fetched)
                return out, next_reply

            return out, defer.succeed(([], None))


        deferred_protocol.addCallback(after_connection)
        return deferred_protocol

    def find_one(self, spec=None, fields=None, **kwargs):
        if isinstance(spec, ObjectId):
            spec = {"_id": spec}
        deferred_find = self.find(spec=spec, limit=1, fields=fields, **kwargs)
        deferred_find.addCallback(lambda r: r[0] if r else {})
        return deferred_find

    def count(self, spec=None, fields=None):
        def wrapper(result):
            return result["n"]

        if fields is not None:
            if not fields:
                fields = ["_id"]
            fields = self._fields_list_to_dict(fields)

        spec = SON([("count", self._collection_name),
                    ("query", spec or SON()),
                    ("fields", fields)])
        deferred_find_one = self._database["$cmd"].find_one(spec)
        deferred_find_one.addCallback(wrapper)
        return deferred_find_one

    def group(self, keys, initial, reduce, condition=None, finalize=None):
        body = {
            "ns": self._collection_name,
            "initial": initial,
            "$reduce": Code(reduce),
        }

        if isinstance(keys, basestring):
            body["$keyf"] = Code(keys)
        else:
            body["key"] = self._fields_list_to_dict(keys)

        if condition:
            body["cond"] = condition
        if finalize:
            body["finalize"] = Code(finalize)

        return self._database["$cmd"].find_one({"group": body})

    def filemd5(self, spec):
        def wrapper(result):
            return result.get("md5")

        if not isinstance(spec, ObjectId):
            raise ValueError("filemd5 expected an objectid for its "
                             "non-keyword argument")

        spec = SON([("filemd5", spec),
                    ("root", self._collection_name)])

        deferred_fine_one = self._database["$cmd"].find_one(spec)
        deferred_fine_one.addCallback(wrapper)
        return deferred_fine_one

    @defer.inlineCallbacks
    def insert(self, docs, safe=True, **kwargs):
        if isinstance(docs, types.DictType):
            ids = docs.get("_id", ObjectId())
            docs["_id"] = ids
            docs = [docs]
        elif isinstance(docs, types.ListType):
            ids = []
            for doc in docs:
                if isinstance(doc, types.DictType):
                    oid = doc.get("_id", ObjectId())
                    ids.append(oid)
                    doc["_id"] = oid
                else:
                    raise TypeError("insert takes a document or a list of documents")
        else:
            raise TypeError("insert takes a document or a list of documents")

        docs = [BSON.encode(d) for d in docs]
        flags = kwargs.get("flags", 0)
        insert = Insert(flags=flags, collection=str(self), documents=docs)

        proto = yield self._database.connection.getprotocol()

        proto.send_INSERT(insert)

        if safe:
            yield proto.get_last_error(str(self._database))

        defer.returnValue(ids)

    @defer.inlineCallbacks
    def update(self, spec, document, upsert=False, multi=False, safe=True, **kwargs):
        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(document, types.DictType):
            raise TypeError("document must be an instance of dict")
        if not isinstance(upsert, types.BooleanType):
            raise TypeError("upsert must be an instance of bool")

        flags = kwargs.get("flags", 0)

        if multi:
            flags |= UPDATE_MULTI
        if upsert:
            flags |= UPDATE_UPSERT

        spec = BSON.encode(spec)
        document = BSON.encode(document)
        update = Update(flags=flags, collection=str(self),
                        selector=spec, update=document)
        proto = yield self._database.connection.getprotocol()

        proto.send_UPDATE(update)

        if safe:
            ret = yield proto.get_last_error(str(self._database))
            defer.returnValue(ret)

    def save(self, doc, safe=True, **kwargs):
        if not isinstance(doc, types.DictType):
            raise TypeError("cannot save objects of type %s" % type(doc))

        oid = doc.get("_id")
        if oid:
            return self.update({"_id": oid}, doc, safe=safe, upsert=True, **kwargs)
        else:
            return self.insert(doc, safe=safe, **kwargs)

    @defer.inlineCallbacks
    def remove(self, spec, safe=True, single=False, **kwargs):
        if isinstance(spec, ObjectId):
            spec = SON(dict(_id=spec))
        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict, not %s" % type(spec))

        flags = kwargs.get("flags", 0)
        if single:
            flags |= DELETE_SINGLE_REMOVE

        spec = BSON.encode(spec)
        delete = Delete(flags=flags, collection=str(self), selector=spec)
        proto = yield self._database.connection.getprotocol()

        proto.send_DELETE(delete)

        if safe:
            ret = yield proto.get_last_error(str(self._database))
            defer.returnValue(ret)

    def drop(self, **kwargs):
        return self._database.drop_collection(self._collection_name)

    def create_index(self, sort_fields, **kwargs):
        def wrapper(result, name):
            return name

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
        deferred_insert = self._database.system.indexes.insert(index, safe=True)
        deferred_insert.addCallback(wrapper, name)
        return deferred_insert

    def ensure_index(self, sort_fields, **kwargs):
        # ensure_index is an alias of create_index since we are not
        # keeping an index cache same way pymongo does
        return self.create_index(sort_fields, **kwargs)

    def drop_index(self, index_identifier):
        if isinstance(index_identifier, types.StringTypes):
            name = index_identifier
        elif isinstance(index_identifier, qf.sort):
            name = self._gen_index_name(index_identifier["orderby"])
        else:
            raise TypeError("index_identifier must be a name or instance of filter.sort")

        cmd = SON([("deleteIndexes", self._collection_name), ("index", name)])
        return self._database["$cmd"].find_one(cmd)

    def drop_indexes(self):
        return self.drop_index("*")

    def index_information(self):
        def wrapper(raw):
            info = {}
            for idx in raw:
                info[idx["name"]] = idx
            return info

        deferred_find = self._database.system.indexes.find({"ns": str(self)})
        deferred_find.addCallback(wrapper)
        return deferred_find

    def rename(self, new_name):
        cmd = SON([("renameCollection", str(self)),
                   ("to", "%s.%s" % (str(self._database), new_name))])
        return self._database("admin")["$cmd"].find_one(cmd)

    def distinct(self, key, spec=None):
        def wrapper(result):
            if result:
                return result.get("values")
            return {}

        cmd = SON([("distinct", self._collection_name), ("key", key)])
        if spec:
            cmd["query"] = spec

        d = self._database["$cmd"].find_one(cmd)
        d.addCallback(wrapper)
        return d

    def aggregate(self, pipeline, full_response=False):
        def wrapper(result, full_response):
            if full_response:
                return result
            return result.get("result")

        cmd = SON([("aggregate", self._collection_name),
                   ("pipeline", pipeline)])

        d = self._database["$cmd"].find_one(cmd)
        d.addCallback(wrapper, full_response)
        return d

    def map_reduce(self, map, reduce, full_response=False, **kwargs):
        def wrapper(result, full_response):
            if full_response:
                return result
            return result.get("result")

        cmd = SON([("mapreduce", self._collection_name),
                   ("map", map), ("reduce", reduce)])
        cmd.update(**kwargs)
        deferred_find_one = self._database["$cmd"].find_one(cmd)
        deferred_find_one.addCallback(wrapper, full_response)
        return deferred_find_one

    def find_and_modify(self, query=None, update=None, upsert=False, **kwargs):
        def wrapper(result):
            no_obj_error = "No matching object found"
            if not result["ok"]:
                if result["errmsg"] == no_obj_error:
                    return None
                else:
                    raise ValueError("Unexpected Error: %s" % (result,))
            return result.get("value")

        if not update and not kwargs.get("remove", None):
            raise ValueError("Must either update or remove")

        if update and kwargs.get("remove", None):
            raise ValueError("Can't do both update and remove")

        cmd = SON([("findAndModify", self._collection_name)])
        cmd.update(kwargs)
        # No need to include empty args
        if query:
            cmd["query"] = query
        if update:
            cmd["update"] = update
        if upsert:
            cmd["upsert"] = upsert

        deferred_find_one = self._database["$cmd"].find_one(cmd)
        deferred_find_one.addCallback(wrapper)
        return deferred_find_one
