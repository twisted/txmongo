Changelog
=========

Release 15.1 (UNRELEASED)
-------------------------

This is a major release in that while increasing code coverage to 91%
( see https://coveralls.io/builds/2650466 ), we've also caught several
bugs, added features and changed functionality to be more inline with PyMongo.

This is no small thanks to travis-ci and coveralls while using tox to cover all iterations
that we support.

We can officially say that we are Python 2.6, 2.7 and PyPy compatible.

API Changes
^^^^^^^^^^^

- **TxMongo now requires PyMongo 3.x**
- Better handling of replica-sets, we now raise an ``autoreconnect`` when master is unreachable.
- Changed the behaviour of ``find_one`` to return ``None`` instead of an empty
  dict ``{}`` when no result is found.
- New-style query methods: ``insert_one/many``, ``update_one/many``, ``delete_one/many``,
  ``replace_one`` and ``find_one_and_update/replace``

Features
^^^^^^^^

- Added ``db.command`` function, just like PyMongo.
- Added support for named indexes in ``filter``.
- ``insert()``, ``update()``, ``save()`` and ``remove()`` now support write-concern options via
  named args: ``w``, ``wtimeout``, ``j``, ``fsync``. ``safe`` argument is still supported for
  backward compatibility.
- Default write-concern can be specified for ``Connection`` using named arguments in constructor
  or by URI options.
- Write-concern options can also be set for ``Database`` and ``Collection`` with ``write_concern``
  named argument of their constructors. In this case write-concern is specified by instance of
  ``pymongo.write_concern.WriteConcern``
- ``txmongo.protocol.INSERT_CONTINUE_ON_ERROR`` flag defined for using with ``insert()``
- Replaced all traditional deferred callbacks (and errbacks) to use @defer.inlineCallbacks

Bugfixes
^^^^^^^^

- Fixed typo in ``map_reduce()`` when returning results.
- Fixed hang in ``create_collection()`` in case of error.
- Fixed typo in ``rename()`` that wasn't using the right factory.
- Fixed exception in ``drop_index`` that was being thrown when dropping a non-existent collection.
  This makes the function idempotent.
- Fixed URI prefixing when "mongodb://" is not present in URI string in ``connection``.
- Fixed fail-over when using replica-sets in ``connection``.  It now raises ``autoreconnect`` when
  there is a problem with the existing master. It is then up to the client code to reconnect to the
  new master.
- Fixed number of cursors in protocol so that it works with py2.6, py2.6 and pypy.


Release 15.0 (2015-05-04)
-------------------------

This is the first release using the Twisted versioning method.

API Changes
^^^^^^^^^^^

- ``collections.index_information`` now mirrors PyMongo's method.
- ``getrequestid`` is now ``get_request_id``

Features
^^^^^^^^

- Add support for 2dsphere indexes, see http://docs.mongodb.org/manual/tutorial/build-a-2dsphere-index/
- PEP8 across files as we work through them.
- Authentication reimplemented for ConnectionPool support with multiple DBs.
- Add support for MongoDB 3.0

Bugfixes
^^^^^^^^

- Fixed failing tests due to changes in Python in 2.6
- Fixed limit not being respected, which should help performance.
- Find now closes MongoDB cursors.
- Fixed 'hint' filter to correctly serialize with double dollar signs.


Improved Documentation
^^^^^^^^^^^^^^^^^^^^^^

- Added, updated and reworked documentation using Sphinx.
- The documentation is now hosted on https://txmongo.readthedocs.org/.


Release 0.6 (2015-01-23)
------------------------

This is the last release in this version scheme, we'll be switching to the Twisted version scheme in the next release.

API Changes
^^^^^^^^^^^

- TxMongo: None

Features
^^^^^^^^

- Added SSL support using Twisted SSLContext factory
- Added "find with cursor" like pymongo
- Test coverage is now measured. We're currently at around 78%.

Bugfixes
^^^^^^^^

- Fixed import in database.py


Release 0.5 (2014-10-02)
------------------------

Code review and cleanup


Bugfixes
^^^^^^^^

 - Bug fixes


Release 0.4 (2013-01-07)
------------------------

Significant performance improvements.

API Changes
^^^^^^^^^^^

- TxMongo: None

Features
^^^^^^^^

- Support AutoReconnect to connect to fail-over master.
- Use pymongo instead of in-tree copy.

Bugfixes
^^^^^^^^

 - Bug fixes

Release 0.3 (2010-09-13)
------------------------

Initial release.

License
^^^^^^^

- Apache 2.0
