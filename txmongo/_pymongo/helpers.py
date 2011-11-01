# Copyright 2009-2010 10gen, Inc.
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

"""Bits and pieces used by the driver that don't really fit elsewhere.

This is a subset of the pymongo helpers.py file
"""

try:
    import hashlib
    _md5func = hashlib.md5
except:  # for Python < 2.5
    import md5
    _md5func = md5.new

def _password_digest(username, password):
    """Get a password digest to use for authentication.
    """
    if not isinstance(password, basestring):
        raise TypeError("password must be an instance of basestring")
    if not isinstance(username, basestring):
        raise TypeError("username must be an instance of basestring")

    md5hash = _md5func()
    md5hash.update("%s:mongo:%s" % (username.encode('utf-8'),
                                    password.encode('utf-8')))
    return unicode(md5hash.hexdigest())


def _auth_key(nonce, username, password):
    """Get an auth key to use for authentication.
    """
    digest = _password_digest(username, password)
    md5hash = _md5func()
    md5hash.update("%s%s%s" % (nonce, unicode(username), digest))
    return unicode(md5hash.hexdigest())


def _fields_list_to_dict(fields):
    """Takes a list of field names and returns a matching dictionary.

    ["a", "b"] becomes {"a": 1, "b": 1}

    and

    ["a.b.c", "d", "a.c"] becomes {"a.b.c": 1, "d": 1, "a.c": 1}
    """
    as_dict = {}
    for field in fields:
        if not isinstance(field, basestring):
            raise TypeError("fields must be a list of key names as "
                            "(string, unicode)")
        as_dict[field] = 1
    return as_dict
