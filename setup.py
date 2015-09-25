#!/usr/bin/env python
import sys
import os
import shutil

from setuptools import setup
from setuptools import Feature
from distutils.cmd import Command
from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError
from distutils.errors import DistutilsPlatformError, DistutilsExecError
from distutils.core import Extension


requirements = ["twisted", "pymongo"]
try:
    import xml.etree.ElementTree
except ImportError:
    requirements.append("elementtree")


if sys.platform == 'win32' and sys.version_info > (2, 6):
   # 2.6's distutils.msvc9compiler can raise an IOError when failing to
   # find the compiler
   build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError,
                 IOError)
else:
   build_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)


setup(
    name="txmongo",
    version="15.3.0",
    description="Asynchronous Python driver for MongoDB <http://www.mongodb.org>",
    author="Alexandre Fiori, Bret Curtis",
    author_email="fiorix@gmail.com, psi29a@gmail.com",
    url="https://github.com/twisted/txmongo",
    keywords=["mongo", "mongodb", "pymongo", "gridfs", "txmongo"],
    packages=["txmongo", "txmongo._gridfs"],
    install_requires=requirements,
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
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database"]
    )
