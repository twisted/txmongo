# Copyright 2009-2014 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from __future__ import absolute_import, division
from collections import defaultdict
from twisted.python.compat import unicode

"""Query filters"""


def _direction(keys, direction):
    if isinstance(keys, (bytes, unicode)):
        return (keys, direction),
    elif isinstance(keys, (list, tuple)):
        return tuple([(k, direction) for k in keys])


def ASCENDING(keys):
    """Ascending sort order"""
    return _direction(keys, 1)


def DESCENDING(keys):
    """Descending sort order"""
    return _direction(keys, -1)


def GEO2D(keys):
    """
    Two-dimensional geospatial index
    http://www.mongodb.org/display/DOCS/Geospatial+Indexing
    """
    return _direction(keys, "2d")


def GEO2DSPHERE(keys):
    """
    Two-dimensional geospatial index
    http://www.mongodb.org/display/DOCS/Geospatial+Indexing
    """
    return _direction(keys, "2dsphere")


def GEOHAYSTACK(keys):
    """
    Bucket-based geospatial index
    http://www.mongodb.org/display/DOCS/Geospatial+Haystack+Indexing
    """
    return _direction(keys, "geoHaystack")


class _QueryFilter(defaultdict):
    def __init__(self):
        defaultdict.__init__(self, lambda: ())

    def __add__(self, obj):
        for k, v in obj.items():
            if isinstance(v, tuple):
                self[k] += v
            else:
                self[k] = v
        return self

    def _index_document(self, operation, index_list):
        name = self.__class__.__name__
        try:
            assert isinstance(index_list, (list, tuple))
            for key, direction in index_list:
                if not isinstance(key, (bytes, unicode)):
                    raise TypeError("Invalid %sing key: %s" % (name, repr(key)))
                if direction not in (1, -1, "2d", "2dsphere", "geoHaystack"):
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
            assert isinstance(key_list[0], (list, tuple))
        except:
            key_list = (key_list,)
        self._index_document("orderby", key_list)


class hint(_QueryFilter):
    """Adds a `hint`, telling Mongo the proper index to use for the query."""

    def __init__(self, index_list_or_name):
        _QueryFilter.__init__(self)
        if isinstance(index_list_or_name, (list, tuple)):
            if not isinstance(index_list_or_name[0], (list, tuple)):
                index_list_or_name = (index_list_or_name,)
            self._index_document("hint", index_list_or_name)
        else:
            self["hint"] = index_list_or_name


class explain(_QueryFilter):
    """Returns an explain plan for the query."""

    def __init__(self):
        _QueryFilter.__init__(self)
        self["explain"] = True


class snapshot(_QueryFilter):
    def __init__(self):
        _QueryFilter.__init__(self)
        self["snapshot"] = True


class comment(_QueryFilter):
    def __init__(self, comment):
        _QueryFilter.__init__(self)
        self["comment"] = comment
