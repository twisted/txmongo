Changelog
=========


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


Improved Documentation
^^^^^^^^^^^^^^^^^^^^^^

- Added, updated and reworked documentation using Sphinx.
- The documentation is now hosted on https://txmongo.readthedocs.org/.

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
