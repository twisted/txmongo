=======
PyMonga
=======
:Info: See `the mongo site <http://www.mongodb.org>`_ for more information. See `github <http://github.com/fiorix/mongo-async-python-driver/tree>`_ for the latest source.
:Author: Alexandre Fiori <fiorix@gmail.com>

About
=====
An asynchronous Python driver for the Mongo database, based on Twisted.
The ``pymonga`` package is an alternative to the original ``pymongo``
shipped with the Mongo database.

Because the original ``pymongo`` package has it's own connection pool and
blocking low-level socket operations, it is hard to fully implement
network servers using the Twisted framework.
Instead of deferring database operations to threads, now it's possible
to do it asynchronously, as easy as using the original API.

It's still a work in progress and should NOT be used for production systems.

Installation
============
You need `setuptools <http://peak.telecommunity.com/DevCenter/setuptools>`_
in order to get ``pymonga`` installed.
Just run **python setup.py install**

Examples
========
There are some examples of basic usage in the *examples/* directory.

Credits
=======
Thanks to (in no particular order):

- Mike Dirolf (mdirolf)

  - The author of original ``pymongo`` package.
