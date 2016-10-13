TxMongo
=======
.. image:: https://travis-ci.org/twisted/txmongo.png?branch=master
    :target: https://travis-ci.org/twisted/txmongo

.. image:: https://coveralls.io/repos/twisted/txmongo/badge.svg?branch=master
    :target: https://coveralls.io/r/twisted/txmongo?branch=master

.. image:: https://badge.fury.io/py/txmongo.png
    :target: https://pypi.python.org/pypi/txmongo

.. image:: https://readthedocs.org/projects/txmongo/badge/?version=latest
    :target: https://txmongo.readthedocs.org/en/latest/?badge=latest
    :alt: Documentation Status


TxMongo is an asynchronous Python/Twisted driver for MongoDB that implements the wire
protocol on non-blocking sockets. The API derives from the original PyMongo.

Compatibility
-------------
Python 2.7, 3.3+ and PyPy
MongoDB 2.6+

Installing
----------

You can use setuptools to install:

```sh
sudo python setup.py install
```

Docs and examples
-----------------

Generate them with `make docs`. You will need `sphinx` installed.
There are some examples in the *examples/* directory.

Hacking
-------

Run `make env` to create clean hacking environment with `virtualenv`.
Run `make` to torture your code with tests and code style tools.

Take a look in Makefile for commonly used commands and tools we use to develop.

Packages
--------

Debian
^^^^^^

Packing for debian exists in *debian/*, you can build yourself a package
(remember to update debian/changelog) if you make changes.

```sh
dpkg-buildpackage -b
```

Then look for the package in your home directory.

Fedora
^^^^^^

```sh
rpmbuild -bb python-txmongo.spec
```

You might need to download Source0 from the .spec and place it in
rpmbuild/SOURCES first.
