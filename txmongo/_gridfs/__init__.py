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

"""GridFS is a specification for storing large objects in Mongo.

The :mod:`gridfs` package is an implementation of GridFS on top of
:mod:`pymongo`, exposing a file-like interface.
"""

from __future__ import absolute_import, division
from twisted.internet import defer
from txmongo._gridfs.errors import NoFile
from txmongo._gridfs.grid_file import GridIn, GridOut, GridOutIterator
from txmongo import filter
from txmongo.filter import ASCENDING, DESCENDING
from txmongo.database import Database

assert GridOutIterator

class GridFS(object):
    """An instance of GridFS on top of a single Database.
    """

    def __init__(self, database, collection="fs"):
        """Create a new instance of :class:`GridFS`.

        Raises :class:`TypeError` if `database` is not an instance of
        :class:`~pymongo.database.Database`.

        :Parameters:
          - `database`: database to use
          - `collection` (optional): root collection to use

        .. note::

            Instantiating a GridFS object will implicitly create it indexes.
            This could leads to errors if the underlying connection is closed
            before the indexes creation request has returned. To avoid this you
            should use the defer returned by :meth:`GridFS.indexes_created`.

        .. versionadded:: 1.6
           The `collection` parameter.
        """
        if not isinstance(database, Database):
            raise TypeError("TxMongo: database must be an instance of Database.")

        self.__database = database
        self.__collection = database[collection]
        self.__files = self.__collection.files
        self.__chunks = self.__collection.chunks
        self.__indexes_created_defer = defer.DeferredList([
            self.__files.create_index(
                filter.sort(ASCENDING("filename") + ASCENDING("uploadDate"))),
            self.__chunks.create_index(
                filter.sort(ASCENDING("files_id") + ASCENDING("n")), unique=True)
        ])

    def indexes_created(self):
        """Returns a defer on the creation of this GridFS instance's indexes
        """
        d = defer.Deferred()
        self.__indexes_created_defer.chainDeferred(d)
        return d

    def new_file(self, **kwargs):
        """Create a new file in GridFS.

        Returns a new :class:`~gridfs.grid_file.GridIn` instance to
        which data can be written. Any keyword arguments will be
        passed through to :meth:`~gridfs.grid_file.GridIn`.

        :Parameters:
          - `**kwargs` (optional): keyword arguments for file creation

        .. versionadded:: 1.6
        """
        return GridIn(self.__collection, **kwargs)

    def put(self, data, **kwargs):
        """Put data in GridFS as a new file.

        Equivalent to doing:

        >>> f = new_file(**kwargs)
        >>> try:
        >>>     f.write(data)
        >>> finally:
        >>>     f.close()

        `data` can be either an instance of :class:`str` or a
        file-like object providing a :meth:`read` method. Any keyword
        arguments will be passed through to the created file - see
        :meth:`~gridfs.grid_file.GridIn` for possible
        arguments. Returns the ``"_id"`` of the created file.

        :Parameters:
          - `data`: data to be written as a file.
          - `**kwargs` (optional): keyword arguments for file creation

        .. versionadded:: 1.6
        """
        grid_file = GridIn(self.__collection, **kwargs)

        def _finally(result):
            return grid_file.close().addCallback(lambda _: result)

        return grid_file.write(data)\
            .addBoth(_finally)\
            .addCallback(lambda _: grid_file._id)

    def get(self, file_id):
        """Get a file from GridFS by ``"_id"``.

        Returns an instance of :class:`~gridfs.grid_file.GridOut`,
        which provides a file-like interface for reading.

        :Parameters:
          - `file_id`: ``"_id"`` of the file to get

        .. versionadded:: 1.6
        """
        def ok(doc):
            if doc is None:
                raise NoFile("TxMongo: no file in gridfs with _id {0}".format(repr(file_id)))

            return GridOut(self.__collection, doc)
        return self.__collection.files.find_one({"_id": file_id}).addCallback(ok)

    def get_version(self, filename=None, version=-1):
        """Get a file from GridFS by ``"filename"``.
        Returns a version of the file in GridFS whose filename matches
        `filename` and whose metadata fields match the supplied keyword
        arguments, as an instance of :class:`~gridfs.grid_file.GridOut`.
        Version numbering is a convenience atop the GridFS API provided
        by MongoDB. If more than one file matches the query (either by
        `filename` alone, by metadata fields, or by a combination of
        both), then version ``-1`` will be the most recently uploaded
        matching file, ``-2`` the second most recently
        uploaded, etc. Version ``0`` will be the first version
        uploaded, ``1`` the second version, etc. So if three versions
        have been uploaded, then version ``0`` is the same as version
        ``-3``, version ``1`` is the same as version ``-2``, and
        version ``2`` is the same as version ``-1``. Note that searching by
        random (unindexed) meta data is not supported here.
        Raises :class:`~gridfs.errors.NoFile` if no such version of
        that file exists.
        :Parameters:
          - `filename`: ``"filename"`` of the file to get, or `None`
          - `version` (optional): version of the file to get (defaults
            to -1, the most recent version uploaded)
        """        
        query = {"filename": filename}
        skip = abs(version)
        if version < 0:
            skip -= 1
            myorder = DESCENDING("uploadDate")
        else:
            myorder = ASCENDING("uploadDate")

        def ok(cursor):
            if cursor:
                return GridOut(self.__collection, cursor[0])

            raise NoFile("no version %d for filename %r" % (version, filename))

        return self.__files.find(query, filter=filter.sort(myorder), limit=1, skip=skip)\
            .addCallback(ok)

    def count(self, filename):
        """Count the number of versions of a given file.
        Returns an integer number of versions of the file in GridFS whose filename matches
        `filename`, or raises NoFile if the file doesn't exist.
        :Parameters:
          - `filename`: ``"filename"`` of the file to get version count of
        """
        return self.__files.count({"filename": filename})

    def get_last_version(self, filename):
        """Get a file from GridFS by ``"filename"``.

        Returns the most recently uploaded file in GridFS with the
        name `filename` as an instance of
        :class:`~gridfs.grid_file.GridOut`. Raises
        :class:`~gridfs.errors.NoFile` if no such file exists.

        An index on ``{filename: 1, uploadDate: -1}`` will
        automatically be created when this method is called the first
        time.

        :Parameters:
          - `filename`: ``"filename"`` of the file to get

        .. versionadded:: 1.6
        """
        def ok(doc):
            if doc is None:
                raise NoFile("TxMongo: no file in gridfs with filename {0}".format(repr(filename)))

            return GridOut(self.__collection, doc)

        return self.__files.find_one({"filename": filename},
                                     filter = filter.sort(DESCENDING("uploadDate"))).addCallback(ok)

    # TODO add optional safe mode for chunk removal?
    def delete(self, file_id):
        """Delete a file from GridFS by ``"_id"``.

        Removes all data belonging to the file with ``"_id"``:
        `file_id`.

        .. warning:: Any processes/threads reading from the file while
           this method is executing will likely see an invalid/corrupt
           file. Care should be taken to avoid concurrent reads to a file
           while it is being deleted.

        :Parameters:
          - `file_id`: ``"_id"`` of the file to delete

        .. versionadded:: 1.6
        """
        return defer.DeferredList([
            self.__files.remove({"_id": file_id}, safe=True),
            self.__chunks.remove({"files_id": file_id})
        ])

    def list(self):
        """List the names of all files stored in this instance of
        :class:`GridFS`.

        .. versionchanged:: 1.6
           Removed the `collection` argument.
        """
        return self.__files.distinct("filename")
