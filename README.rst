=======
txMongo
=======
:Info: See `the mongo site`_ for more information. See `github`_ for the latest
       source.
:Author: Alexandre Fiori <fiorix@gmail.com>


About
=====
An asynchronous Python driver for the Mongo database, using on Twisted.
The ``txmongo`` package is an alternative to the original ``pymongo``
shipped with the Mongo database.

Because the original ``pymongo`` package has it's own connection pool and
blocking low-level socket operations, it is hard to fully implement
network servers using the Twisted framework.
Instead of deferring database operations to threads, now it's possible
to do it asynchronously, as easy as using the original API.


Installation
============
`pip`_ is recommended over `setuptools`_ for installation, so best just to
run::

  $ sudo pip install txmongo

Failing that, you can fall back to::

  $ sudo python setup.py install


Docs and Examples
=================
You can generate the docs with::

 $ make docs

Do note, however, that you will need `epydoc` installed in order to do so.

For "living" docs, be sure to take a look at the sample code in the
``./examples/`` directory.


Hacking
=======
To create clean hacking environment with `virtualenv`::

  $ make env

To torture your code with tests and code style tools::

  $ make


Credits
=======
Thanks to (in no particular order):

- Mike Dirolf (mdirolf)

  - The author of original ``pymongo`` package.

- Renzo Sanchez-Silva (rnz0)

  - Initial twisted trial unit tests.

- Vanderson Mota (chunda)

  - Patching setup.py and PyPi maintenance

- flanked

  - For porting GridFS to txmongo

- Andre Ferraz

  - For creating and maintaining the debian package

- Mark L

  - Bugfixes and Unit Tests

- Alexey Palazhchenko (AlekSi)

  - Code clean up and hacking environment

- `Silas Sewell`_

  - Minor build and type fixes


.. Document Links
.. _the mongo site: http://www.mongodb.org/
.. _github: http://github.com/oubiwann/txmongo/tree
.. _pip: http://pypi.python.org/pypi/pip/
.. _setuptools: http://peak.telecommunity.com/DevCenter/setuptools
.. _Silas Sewell: https://github.com/silas
