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

"""Tools for representing files stored in GridFS."""

from __future__ import absolute_import, division
from bson import Binary, ObjectId
import datetime
from io import BytesIO as StringIO
import math
import os
from twisted.python.compat import unicode
from twisted.internet import defer
from txmongo._gridfs.errors import CorruptGridFile
from txmongo.collection import Collection

"""Default chunk size, in bytes."""
DEFAULT_CHUNK_SIZE = 256 * 1024


def _create_property(field_name, docstring,
                     read_only=False, closed_only=False):
    """Helper for creating properties to read/write to files.
    """

    def getter(self):
        if closed_only and not self._closed:
            raise AttributeError("can only get %r on a closed file" %
                                 field_name)
        return self._file.get(field_name, None)

    def setter(self, value):
        if self._closed:
            raise AttributeError("cannot set %r on a closed file" %
                                 field_name)
        self._file[field_name] = value

    if read_only:
        docstring += "\n\nThis attribute is read-only."
    elif not closed_only:
        docstring = "%s\n\n%s" % (docstring, "This attribute can only be "
                                             "set before :meth:`close` has been called.")
    else:
        docstring = "%s\n\n%s" % (docstring, "This attribute is read-only and "
                                             "can only be read after :meth:`close` "
                                             "has been called.")

    if not read_only and not closed_only:
        return property(getter, setter, doc=docstring)
    return property(getter, doc=docstring)


class GridIn(object):
    """Class to write data to GridFS.
    """

    def __init__(self, root_collection, **kwargs):
        """Write a file to GridFS

        Application developers should generally not need to
        instantiate this class directly - instead see the methods
        provided by :class:`~gridfs.GridFS`.

        Raises :class:`TypeError` if `root_collection` is not an
        instance of :class:`~pymongo.collection.Collection`.

        Any of the file level options specified in the `GridFS Spec
        <http://dochub.mongodb.org/core/gridfsspec>`_ may be passed as
        keyword arguments. Any additional keyword arguments will be
        set as additional fields on the file document. Valid keyword
        arguments include:

          - ``"_id"``: unique ID for this file (default:
            :class:`~pymongo.objectid.ObjectId`)

          - ``"filename"``: human name for the file

          - ``"contentType"`` or ``"content_type"``: valid mime-type
            for the file

          - ``"chunkSize"`` or ``"chunk_size"``: size of each of the
            chunks, in bytes (default: 256 kb)

        :Parameters:
          - `root_collection`: root collection to write to
          - `**kwargs` (optional): file level options (see above)
        """
        if not isinstance(root_collection, Collection):
            raise TypeError("root_collection must be an instance of Collection")

        # Handle alternative naming
        if "content_type" in kwargs:
            kwargs["contentType"] = kwargs.pop("content_type")
        if "chunk_size" in kwargs:
            kwargs["chunkSize"] = kwargs.pop("chunk_size")

        # Defaults
        kwargs["_id"] = kwargs.get("_id", ObjectId())
        kwargs["chunkSize"] = kwargs.get("chunkSize", DEFAULT_CHUNK_SIZE)

        object.__setattr__(self, "_coll", root_collection)
        object.__setattr__(self, "_chunks", root_collection.chunks)
        object.__setattr__(self, "_file", kwargs)
        object.__setattr__(self, "_buffer", StringIO())
        object.__setattr__(self, "_position", 0)
        object.__setattr__(self, "_chunk_number", 0)
        object.__setattr__(self, "_closed", False)

    @property
    def closed(self):
        """Is this file closed?
        """
        return self._closed

    _id = _create_property("_id", "The ``'_id'`` value for this file.",
                           read_only=True)
    filename = _create_property("filename", "Name of this file.")
    content_type = _create_property("contentType", "Mime-type for this file.")
    length = _create_property("length", "Length (in bytes) of this file.",
                              closed_only=True)
    chunk_size = _create_property("chunkSize", "Chunk size for this file.",
                                  read_only=True)
    upload_date = _create_property("uploadDate",
                                   "Date that this file was uploaded.",
                                   closed_only=True)
    md5 = _create_property("md5", "MD5 of the contents of this file "
                                  "(generated on the server).",
                           closed_only=True)

    def __getattr__(self, name):
        if name in self._file:
            return self._file[name]
        raise AttributeError("GridIn object has no attribute '%s'" % name)

    def __setattr__(self, name, value):
        if self._closed:
            raise AttributeError("cannot set %r on a closed file" % name)
        object.__setattr__(self, name, value)

    @defer.inlineCallbacks
    def __flush_data(self, data):
        """Flush `data` to a chunk.
        """
        if data:
            assert (len(data) <= self.chunk_size)
            chunk = {"files_id": self._file["_id"],
                     "n": self._chunk_number,
                     "data": Binary(data)}

            # Continue writing after the insert completes (non-blocking)
            yield self._chunks.insert(chunk)
            self._chunk_number += 1
            self._position += len(data)

    @defer.inlineCallbacks
    def __flush_buffer(self):
        """Flush the buffer contents out to a chunk.
        """
        yield self.__flush_data(self._buffer.getvalue())
        self._buffer.close()
        self._buffer = StringIO()

    @defer.inlineCallbacks
    def __flush(self):
        """Flush the file to the database.
        """
        yield self.__flush_buffer()

        md5 = yield self._coll.filemd5(self._id)

        self._file["md5"] = md5
        self._file["length"] = self._position
        self._file["uploadDate"] = datetime.datetime.utcnow()
        yield self._coll.files.insert(self._file)

    @defer.inlineCallbacks
    def close(self):
        """Flush the file and close it.

        A closed file cannot be written any more. Calling
        :meth:`close` more than once is allowed.
        """
        if not self._closed:
            yield self.__flush()
            self._closed = True

    @defer.inlineCallbacks
    def write(self, data):
        """Write data to the file. There is no return value.

        `data` can be either a string of bytes or a file-like object
        (implementing :meth:`read`).

        Due to buffering, the data may not actually be written to the
        database until the :meth:`close` method is called. Raises
        :class:`ValueError` if this file is already closed. Raises
        :class:`TypeError` if `data` is not an instance of
        :class:`str` or a file-like object.

        :Parameters:
          - `data`: string of bytes or file-like object to be written
            to the file
        """
        if self._closed:
            raise ValueError("cannot write to a closed file")

        try:
            # file-like
            read = data.read
        except AttributeError:
            # string
            if not isinstance(data, (bytes, unicode)):
                raise TypeError("can only write strings or file-like objects")
            if isinstance(data, unicode):
                try:
                    data = data.encode(self.encoding)
                except AttributeError:
                    raise TypeError("must specify an encoding for file in "
                                    "order to write %s" % data)
            read = StringIO(data).read

        if self._buffer.tell() > 0:
            # Make sure to flush only when _buffer is complete
            space = self.chunk_size - self._buffer.tell()
            if space:
                to_write = read(space)
                self._buffer.write(to_write)
                if len(to_write) < space:
                    return  # EOF or incomplete
            yield self.__flush_buffer()
        to_write = read(self.chunk_size)
        while to_write and len(to_write) == self.chunk_size:
            yield self.__flush_data(to_write)
            to_write = read(self.chunk_size)
        self._buffer.write(to_write)

    @defer.inlineCallbacks
    def writelines(self, sequence):
        """Write a sequence of strings to the file.

        Does not add separators.
        """
        for line in sequence:
            yield self.write(line)

    def __enter__(self):
        """Support for the context manager protocol.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Support for the context manager protocol.

        Close the file and allow exceptions to propogate.
        """
        self.close()
        return False  # untrue will propogate exceptions


class GridOut(object):
    """Class to read data out of GridFS.
    """

    def __init__(self, root_collection, doc):
        """Read a file from GridFS

        Application developers should generally not need to
        instantiate this class directly - instead see the methods
        provided by :class:`~gridfs.GridFS`.

        Raises :class:`TypeError` if `root_collection` is not an instance of
        :class:`~pymongo.collection.Collection`.

        :Parameters:
          - `root_collection`: root collection to read from
          - `file_id`: value of ``"_id"`` for the file to read
        """
        if not isinstance(root_collection, Collection):
            raise TypeError("root_collection must be an instance of Collection")

        self.__chunks = root_collection.chunks
        self._file = doc
        self.__current_chunk = -1
        self.__buffer = b''
        self.__position = 0

    _id = _create_property("_id", "The ``'_id'`` value for this file.", True)
    name = _create_property("filename", "Name of this file.", True)
    content_type = _create_property("contentType", "Mime-type for this file.",
                                    True)
    length = _create_property("length", "Length (in bytes) of this file.",
                              True)
    chunk_size = _create_property("chunkSize", "Chunk size for this file.",
                                  True)
    upload_date = _create_property("uploadDate",
                                   "Date that this file was first uploaded.",
                                   True)
    aliases = _create_property("aliases", "List of aliases for this file.",
                               True)
    metadata = _create_property("metadata", "Metadata attached to this file.",
                                True)
    md5 = _create_property("md5", "MD5 of the contents of this file "
                                  "(generated on the server).", True)

    def __getattr__(self, name):
        if name in self._file:
            return self._file[name]
        raise AttributeError("GridOut object has no attribute '%s'" % name)

    @defer.inlineCallbacks
    def read(self, size=-1):
        """Read at most `size` bytes from the file (less if there
        isn't enough data).

        The bytes are returned as an instance of :class:`str`. If
        `size` is negative or omitted all data is read.

        :Parameters:
          - `size` (optional): the number of bytes to read
        """
        if size:
            remainder = int(self.length) - self.__position
            if size < 0 or size > remainder:
                size = remainder

            data = self.__buffer
            chunk_number = (len(data) + self.__position) / self.chunk_size

            while len(data) < size:
                chunk = yield self.__chunks.find_one({"files_id": self._id,
                                                      "n": chunk_number})
                if not chunk:
                    raise CorruptGridFile("no chunk #%d" % chunk_number)

                if not data:
                    data += chunk["data"][self.__position % self.chunk_size:]
                else:
                    data += chunk["data"]

                chunk_number += 1

            self.__position += size
            to_return = data[:size]
            self.__buffer = data[size:]
            defer.returnValue(to_return)

    def tell(self):
        """Return the current position of this file.
        """
        return self.__position

    def seek(self, pos, whence=os.SEEK_SET):
        """Set the current position of this file.

        :Parameters:
         - `pos`: the position (or offset if using relative
           positioning) to seek to
         - `whence` (optional): where to seek
           from. :attr:`os.SEEK_SET` (``0``) for absolute file
           positioning, :attr:`os.SEEK_CUR` (``1``) to seek relative
           to the current position, :attr:`os.SEEK_END` (``2``) to
           seek relative to the file's end.
        """
        if whence == os.SEEK_SET:
            new_pos = pos
        elif whence == os.SEEK_CUR:
            new_pos = self.__position + pos
        elif whence == os.SEEK_END:
            new_pos = int(self.length) + pos
        else:
            raise IOError(22, "Invalid value for `whence`")

        if new_pos < 0:
            raise IOError(22, "Invalid value for `pos` - must be positive")

        self.__position = new_pos

    def close(self):
        self.__buffer = ''
        self.__current_chunk = -1

    def __repr__(self):
        return str(self._file)


class GridOutIterator(object):
    def __init__(self, grid_out, chunks):
        self.__id = grid_out._id
        self.__chunks = chunks
        self.__current_chunk = 0
        self.__max_chunk = math.ceil(float(grid_out.length) /
                                     grid_out.chunk_size)

    def __iter__(self):
        return self

    @defer.inlineCallbacks
    def __next__(self):
        if self.__current_chunk >= self.__max_chunk:
            raise StopIteration
        chunk = yield self.__chunks.find_one({"files_id": self.__id,
                                              "n": self.__current_chunk})
        if not chunk:
            raise CorruptGridFile("no chunk #%d" % self.__current_chunk)
        self.__current_chunk += 1
        defer.returnValue(bytes(chunk["data"]))
    next = __next__
