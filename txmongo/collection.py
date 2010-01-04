# coding: utf-8
# Copyright 2009 Alexandre Fiori
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import types
from txmongo import filter as qf
from txmongo._pymongo.son import SON
from txmongo._pymongo.code import Code
from txmongo._pymongo.objectid import ObjectId
from twisted.internet.defer import Deferred

class Collection(object):
    def __init__(self, database, collection_name):
        self._database = database
        self._collection_name = collection_name

    def __str__(self):
        return "%s.%s" % (str(self._database), self._collection_name)

    def __repr__(self):
        return "<mongodb Collection: %s>" % str(self)

    def __getitem__(self, collection_name):
        return Collection(self._database,
            "%s.%s" % (self._collection_name, collection_name))

    def __getattr__(self, collection_name):
        return self[collection_name]

    def __call__(self, collection_name):
        return self[collection_name]

    def _fields_list_to_dict(self, fields):
        """
        transform a list of fields from ["a", "b"] to {"a":1, "b":1}
        """
        as_dict = {}
        for field in fields:
            if not isinstance(field, types.StringType):
                raise TypeError("fields must be a list of key names")
            as_dict[field] = 1
        return as_dict

    def _gen_index_name(self, keys):
        return u"_".join([u"%s_%s" % item for item in keys])

    def find(self, spec=None, skip=0, limit=0, fields=None, filter=None, _proto=None):
        if spec is None: spec = SON()

        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(fields, (types.ListType, types.NoneType)):
            raise TypeError("fields must be an istance of list")
        if not isinstance(skip, types.IntType):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, types.IntType):
            raise TypeError("limit must be an instance of int")

        if fields is not None:
            if not fields:
                fields = ["_id"]
            fields = self._fields_list_to_dict(fields)

        if isinstance(filter, (qf.sort, qf.hint, qf.explain, qf.snapshot)):
            spec = SON(dict(query=spec))
            for k, v in filter.items():
                spec[k] = isinstance(v, types.TupleType) and SON(v) or v

        # send the command through a specific connection
        # this is required for the connection pool to work
        # when safe=True
        if _proto is None:
            proto = self._database._connection
        else:
            proto = _proto
        return proto.OP_QUERY(str(self), spec, skip, limit, fields)

    def find_one(self, spec=None, fields=None, _proto=None):
        def wrapper(docs):
            return docs and docs[0] or {}

        if isinstance(spec, ObjectId):
            spec = SON(dict(_id=spec))

        d = self.find(spec, limit=-1, fields=fields, _proto=_proto)
        d.addCallback(wrapper)
        return d

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
        d = self._database["$cmd"].find_one(spec)
        d.addCallback(wrapper)
        return d

    def group(self, keys, initial, reduce, condition=None, finalize=None):
        body = {
            "ns": self._collection_name,
            "key": self._fields_list_to_dict(keys),
            "initial": initial,
            "$reduce": Code(reduce),
        }

        if condition:
            body["cond"] = condition
        if finalize:
            body["finalize"] = Code(finalize)

        return self._database["$cmd"].find_one({"group":body})
        
    def __safe_operation(self, proto, safe=False):
        if safe is True:
            return self._database["$cmd"].find_one({"getlasterror":1}, _proto=proto)
        else:
            return None

    def __insert(self, docs, safe=False):
        if isinstance(docs, types.DictType):
            docs = [docs]
        if not isinstance(docs, types.ListType):
            raise TypeError("insert takes a document or a list of documents")
        proto = self._database._connection
        proto.OP_INSERT(str(self), docs)
        return self.__safe_operation(proto, safe)

    def insert(self, docs):
        return self.__insert(docs)

    def safe_insert(self, docs):
        return self.__insert(docs, safe=True)

    def __update(self, spec, document, upsert=False, safe=False):
        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(document, types.DictType):
            raise TypeError("document must be an instance of dict")
        if not isinstance(upsert, types.BooleanType):
            raise TypeError("upsert must be an instance of bool")
        proto = self._database._connection
        proto.OP_UPDATE(str(self), spec, document)
        return self.__safe_operation(proto, safe)

    def update(self, spec, document, upsert=False):
        return self.__update(spec, document, upsert)

    def safe_update(self, spec, document, upsert=False):
        return self.__update(spec, document, upsert, safe=True)

    def __save(self, doc, safe=False):
        if not isinstance(doc, types.DictType):
            raise TypeError("cannot save objects of type %s" % type(doc))

        objid = doc.get("_id")
        if objid:
            return self.update({"_id": objid}, doc, safe=safe)
        else:
            return self.insert(doc, safe=safe)

    def save(self, doc):
        return self.__save(doc)

    def safe_save(self, doc):
        return self.__save(doc, safe=True)
    
    def __remove(self, spec, safe=False):
        if isinstance(spec, ObjectId):
            spec = SON(dict(_id=spec))
        if not isinstance(spec, types.DictType):
            raise TypeError("spec must be an instance of dict, not %s" % type(spec))

        proto = self._database._connection
        proto.OP_DELETE(str(self), spec)
        return self.__safe_operation(proto, safe)

    def remove(self, spec):
        return self.__remove(spec)

    def safe_remove(self, spec):
        return self.__remove(spec, safe=True)

    def __drop(self, safe=False):
        return self.__remove({}, safe)

    def drop(self):
        return self.__drop()

    def safe_drop(self):
        return self.__drop(safe=True)

    def create_index(self, sort_fields, unique=False):
        def wrapper(result, name):
            return name

        if not isinstance(sort_fields, qf.sort):
            raise TypeError("sort_fields must be an instance of filter.sort")

        name = self._gen_index_name(sort_fields["orderby"])
        index = SON(dict(
            ns = str(self),
            name = name,
            key = SON(dict(sort_fields["orderby"])),
            unique = unique,
        ))

        d = self._database.system.indexes.__insert(index, safe=True)
        d.addCallback(wrapper, name)
        return d

    def drop_index(self, index_identifier):
        if isinstance(index_identifier, types.StringType):
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
                info[idx["name"]] = idx["key"].items()
            return info

        d = self._database.system.indexes.find({"ns": str(self)})
        d.addCallback(wrapper)
        return d

    def rename(self, new_name):
        cmd = SON([("renameCollection", str(self)), ("to", "%s.%s" % \
            (str(self._database), new_name))])
        return self._database("admin")["$cmd"].find_one(cmd)
