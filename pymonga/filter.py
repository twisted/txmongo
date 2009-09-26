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
    if isinstance(keys, types.StringType):
	return (keys, direction),
    elif isinstance(keys, (types.ListType, types.TupleType)):
	return tuple([(k, direction) for k in keys])

def ASCENDING(keys):
    return _DIRECTION(keys, 1)

def DESCENDING(keys):
    return _DIRECTION(keys, -1)

class _QueryFilter(defaultdict):
    def __init__(self):
	defaultdict.__init__(self, lambda: ())

    def __add__(self, obj):
	for k, v in obj.items():
	    if isinstance(v, types.TupleType): self[k] += v
	    else: self[k] = v
	return self

    def _index_document(self, operation, index_list):
	name = self.__class__.__name__
	try:
	    assert isinstance(index_list, (types.ListType, types.TupleType))
	    for key, direction in index_list:
		if not isinstance(key, types.StringTypes):
		    raise TypeError("Invalid %sing key: %s" % (name, repr(k)))
		if direction not in (1, -1):
		    raise TypeError("Invalid %sing direction: %s" % (name, direction))
	    self[operation] += tuple(index_list)
	except Exception, e:
	    print "ERROR:", e
	    raise TypeError("Invalid list of keys for %s: %s" % (name, repr(index_list)))

    def __repr__(self):
	return "<mongodb QueryFilter: %s>" % dict.__repr__(self)


class sort(_QueryFilter):
    def __init__(self, key_list):
	_QueryFilter.__init__(self)
	try: assert isinstance(key_list[0], (types.ListType, types.TupleType))
	except: key_list = (key_list,)
	self._index_document("orderby", key_list)


class hint(_QueryFilter):
    def __init__(self, index_list):
	_QueryFilter.__init__(self)
	try: assert isinstance(index_list[0], (types.ListType, types.TupleType))
	except: index_list = (index_list,)
	self._index_document("$hint", index_list)


class explain(_QueryFilter):
    def __init__(self):
	_QueryFilter.__init__(self)
	self["explain"] = True


class snapshot(_QueryFilter):
    def __init__(self):
	_QueryFilter.__init__(self)
	self["snapshot"] = True
