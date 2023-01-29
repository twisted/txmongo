#!/usr/bin/env python
from setuptools import setup

setup(
    name="txmongo",
    version="23.0.0",
    description="Asynchronous Python driver for MongoDB <http://www.mongodb.org>",
    author="Alexandre Fiori, Bret Curtis",
    author_email="fiorix@gmail.com, psi29a@gmail.com",
    url="https://github.com/twisted/txmongo",
    keywords=["mongo", "mongodb", "pymongo", "gridfs", "txmongo"],
    packages=["txmongo", "txmongo._gridfs"],
    install_requires=["twisted>=14.0", "pymongo>=3.0, <4.0"],
    extras_require={
        'srv': ['pymongo[srv]>=3.6'],
    },
    license="Apache License, Version 2.0",
    include_package_data=True,
    test_suite="nose.collector",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database"]
    )
