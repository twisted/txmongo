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
from collections import defaultdict

"""Query filters"""


def _DIRECTION(keys, direction):
    if isinstance(keys, types.StringTypes):
        return (keys, direction),
    elif isinstance(keys, (types.ListType, types.TupleType)):
        return tuple([(k, direction) for k in keys])


def ASCENDING(keys):
    """Ascending sort order"""
    return _DIRECTION(keys, 1)


def DESCENDING(keys):
    """Descending sort order"""
    return _DIRECTION(keys, -1)


def GEO2D(keys):
    """
    Two-dimensional geospatial index
    http://www.mongodb.org/display/DOCS/Geospatial+Indexing
    """
    return _DIRECTION(keys, "2d")


def GEOHAYSTACK(keys):
    """
    Bucket-based geospatial index
    http://www.mongodb.org/display/DOCS/Geospatial+Haystack+Indexing
    """
    return _DIRECTION(keys, "geoHaystack")


 
class _QueryFilter(defaultdict):
    def __init__(self):
        defaultdict.__init__(self, lambda: ())

    def __add__(self, obj):
        for k, v in obj.items():
            if isinstance(v, types.TupleType):
                self[k] += v
            else:
                self[k] = v
        return self

    def _index_document(self, operation, index_list):
        name = self.__class__.__name__
        try:
            assert isinstance(index_list, (types.ListType, types.TupleType))
            for key, direction in index_list:
                if not isinstance(key, types.StringTypes):
                    raise TypeError("Invalid %sing key: %s" % (name, repr(key)))
                if direction not in (1, -1, "2d", "geoHaystack"):
                    raise TypeError("Invalid %sing direction: %s" % (name, direction))
                self[operation] += tuple(((key, direction),))
        except Exception:
            raise TypeError("Invalid list of keys for %s: %s" % (name, repr(index_list)))

    def __repr__(self):
        return "<mongodb QueryFilter: %s>" % dict.__repr__(self)


class sort(_QueryFilter):
    """Sorts the results of a query."""

    def __init__(self, key_list):
        _QueryFilter.__init__(self)
        try:
            assert isinstance(key_list[0], (types.ListType, types.TupleType))
        except:
            key_list = (key_list,)
        self._index_document("orderby", key_list)


class hint(_QueryFilter):
    """Adds a `hint`, telling Mongo the proper index to use for the query."""

    def __init__(self, index_list):
        _QueryFilter.__init__(self)
        try:
            assert isinstance(index_list[0], (types.ListType, types.TupleType))
        except:
            index_list = (index_list,)
        self._index_document("$hint", index_list)


class explain(_QueryFilter):
    """Returns an explain plan for the query."""

    def __init__(self):
        _QueryFilter.__init__(self)
        self["explain"] = True


class snapshot(_QueryFilter):
    def __init__(self):
        _QueryFilter.__init__(self)
        self["snapshot"] = True
