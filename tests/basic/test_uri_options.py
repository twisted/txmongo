# Copyright 2026 The TxMongo Developers. All rights reserved.
# Use of this source code is governed by the Apache License that can be
# found in the LICENSE file.

from twisted.trial import unittest

from txmongo.connection import _parse_uri


class TestUriOptions(unittest.TestCase):
    """PyMongo >= 4.14 returns URI options as a plain dict with camelCase keys,
    while txmongo looks them up in lowercase. _parse_uri must normalize them
    so options are not silently ignored."""

    URI = (
        "mongodb://localhost:27017/db?replicaSet=rs0&authSource=admin"
        "&authMechanism=SCRAM-SHA-256&readPreference=secondary"
        "&wtimeoutMS=700&journal=true&w=1"
    )

    def test_option_keys_are_lowercase(self):
        options = _parse_uri(self.URI)["options"]

        self.assertEqual(options.get("replicaset"), "rs0")
        self.assertEqual(options.get("authsource"), "admin")
        self.assertEqual(options.get("authmechanism"), "SCRAM-SHA-256")
        self.assertEqual(options.get("readpreference"), "secondary")
        self.assertEqual(options.get("wtimeoutms"), 700)
        self.assertEqual(options.get("journal"), True)
        self.assertEqual(options.get("w"), 1)

    def test_other_uri_parts_are_preserved(self):
        parsed = _parse_uri(self.URI)

        self.assertEqual(parsed["database"], "db")
        self.assertEqual(parsed["nodelist"], [("localhost", 27017)])
