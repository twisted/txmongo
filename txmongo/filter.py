# Copyright 2009-2014 The TxMongo Developers.  All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from collections import defaultdict
from typing import List, Literal, Mapping, Tuple, Union

"""Query filters"""


def _direction(keys, direction):
    if isinstance(keys, (bytes, str)):
        return ((keys, direction),)
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


def TEXT(keys):
    """
    Text-based index
    https://docs.mongodb.com/manual/core/index-text/
    """
    return _direction(keys, "text")


AllowedDirectionType = Literal[1, -1, "2d", "2dsphere", "geoHaystack", "text"]
SortArgument = Union[
    Mapping[str, AllowedDirectionType],
    List[Tuple[str, AllowedDirectionType]],
    Tuple[Tuple[str, AllowedDirectionType]],
    Tuple[str, AllowedDirectionType],
]


class _QueryFilter(defaultdict):

    ALLOWED_DIRECTIONS = {1, -1, "2d", "2dsphere", "geoHaystack", "text"}

    def __init__(self):
        super().__init__(lambda: ())

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
                if not isinstance(key, (bytes, str)):
                    raise TypeError(
                        "TxMongo: invalid {0}ing key '{1}'".format(name, repr(key))
                    )
                if direction not in self.ALLOWED_DIRECTIONS:
                    raise TypeError(
                        "TxMongo invalid {0}ing direction '{1}'".format(name, direction)
                    )
                self[operation] += tuple(((key, direction),))
        except Exception:
            raise TypeError(
                "TxMongo: invalid list of keys for {0}, {1}".format(
                    name, repr(index_list)
                )
            )

    def __repr__(self):
        return "<mongodb QueryFilter: %s>" % dict.__repr__(self)


class sort(_QueryFilter):
    """Sorts the results of a query."""

    def __init__(self, key_list: SortArgument):
        super().__init__()
        if isinstance(key_list, Mapping):
            key_list = list(key_list.items())
        elif not isinstance(key_list[0], (list, tuple)):
            key_list = (key_list,)
        self._index_document("orderby", key_list)


class hint(_QueryFilter):
    """Adds a `hint`, telling Mongo the proper index to use for the query."""

    def __init__(self, index_list_or_name):
        super().__init__()
        if isinstance(index_list_or_name, Mapping):
            index_list_or_name = list(index_list_or_name.items())
        if isinstance(index_list_or_name, (list, tuple)):
            if not isinstance(index_list_or_name[0], (list, tuple)):
                index_list_or_name = (index_list_or_name,)
            self._index_document("hint", index_list_or_name)
        else:
            self["hint"] = index_list_or_name


class explain(_QueryFilter):
    """Returns an explain plan for the query."""

    def __init__(self):
        super().__init__()
        self["explain"] = True


class snapshot(_QueryFilter):
    def __init__(self):
        super().__init__()
        self["snapshot"] = True


class comment(_QueryFilter):
    def __init__(self, comment):
        super().__init__()
        self["comment"] = comment
