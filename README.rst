=======
TxMongo
=======
:Info: See `the mongo site <http://www.mongodb.org>`_ for more information. See `github <http://github.com/fiorix/mongo-async-python-driver/tree>`_ for the latest source.
:Author: Alexandre Fiori <fiorix@gmail.com>

About
=====
An asynchronous Python driver for the Mongo database, based on Twisted.
The ``txmongo`` package is an alternative to the original ``pymongo``
shipped with the Mongo database.

Because the original ``pymongo`` package has it's own connection pool and
blocking low-level socket operations, it is hard to fully implement
network servers using the Twisted framework.
Instead of deferring database operations to threads, now it's possible
to do it asynchronously, as easy as using the original API.

Installation
============
You need `setuptools <http://peak.telecommunity.com/DevCenter/setuptools>`_
in order to get ``txmongo`` installed.
Just run **python setup.py install**

Examples
========
There are some examples in the *examples/* directory.

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
