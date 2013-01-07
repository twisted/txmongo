# MongoDB driver for Python Twisted

This package started out as a fork of fiorix/mongo-async-python-driver.
However, to implement the more difficult features of a MongoDB driver such as
failover, it was nearly completely rewritten.

The primary features of this rewrite include:

 * reconfiguration of protocol connections upon master change.
 * failover to new master upon AutoReconnect failure.
 * depend on pymongo to avoid in tree pymongo driver to avoid drift.
 * Use python bson driver rather than in tree version.
 * tries to be careful about building and parsing protocol strings to improve throughput.
 * use namedtuple for message structures.

The collection, database, and other higher level structures were left unchanged.

# Installing

You can use setuptools to install this txmongo fork.

```sh
sudo python setup.py install
```

# Packages

Packing for debian exists in debian/, you can build yourself a package
(remember to update debian/changelog). Alternatively you can download a
pre-built package from my github (chergert/mongo-async-python-driver).

# Contributing

If this fork provides the features you need, feel free to add to it. I'll try
to be responsive to merge requests.
