# coding: utf-8
# Copyright 2015 Ilya Skriblovsky <ilyaskriblovsky@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import shutil
import tempfile
from unittest.mock import patch

from pymongo.errors import OperationFailure
from twisted.internet import defer, ssl
from twisted.internet.defer import TimeoutError
from twisted.trial import unittest

from tests.basic.utils import only_for_mongodb_older_than
from tests.conf import MongoConf
from tests.mongod import create_mongod
from txmongo import connection
from txmongo.protocol import MongoAuthenticationError


class MongoAuth:
    @property
    def host(self):
        return "localhost"

    @property
    def port(self):
        return MongoConf().auth_test_port

    @property
    def uri(self):
        return f"mongodb://{self.host}:{self.port}/"


class TestMongoAuth(unittest.TestCase, MongoAuth):
    """
    NB: This testcase requires:
        * auth=true in MongoDB configuration file
        * no configured users
        * localhost exception enabled (this is default)
    """

    db1 = "authtest1"
    db2 = "authtest2"
    coll = "mycol"

    login1 = "user1"
    password1 = "pwd1"

    login2 = "user2"
    password2 = "pwd2"

    ua_login = "useradmin"
    ua_password = "useradminpwd"

    def __get_connection(self, pool_size=1):
        return connection.ConnectionPool(self.uri, pool_size)

    @defer.inlineCallbacks
    def setUp(self):
        self.__mongod = create_mongod(
            port=self.port, auth=True, rootCreds=(self.ua_login, self.ua_password)
        )
        yield self.__mongod.start()
        self.addCleanup(self.clean)

        conn = self.__get_connection()
        yield conn["admin"].authenticate(self.ua_login, self.ua_password)

        self.ismaster = yield conn.admin.command("ismaster")

        yield conn[self.db1].command(
            "createUser",
            self.login1,
            pwd=self.password1,
            roles=[{"role": "readWrite", "db": self.db1}],
        )

        yield conn[self.db2].command(
            "createUser",
            self.login2,
            pwd=self.password2,
            roles=[{"role": "readWrite", "db": self.db2}],
        )

        yield conn.disconnect()

    @defer.inlineCallbacks
    def clean(self):
        try:
            conn = self.__get_connection()
            yield conn["admin"].authenticate(self.ua_login, self.ua_password)
            yield conn[self.db1].authenticate(self.login1, self.password1)
            yield conn[self.db2].authenticate(self.login2, self.password2)

            yield conn[self.db1][self.coll].drop()
            yield conn[self.db2][self.coll].drop()
            yield conn[self.db1].command("dropUser", self.login1)
            yield conn[self.db2].command("dropUser", self.login2)
            yield conn.disconnect()
        finally:
            yield self.__mongod.stop()

    @defer.inlineCallbacks
    def test_AuthConnectionPool(self):
        pool_size = 2

        conn = self.__get_connection(pool_size)
        db = conn[self.db1]
        coll = db[self.coll]

        try:
            yield db.authenticate(self.login1, self.password1)

            n = pool_size + 1

            yield defer.gatherResults([coll.insert_one({"x": 42}) for _ in range(n)])

            cnt = yield coll.count()
            self.assertEqual(cnt, n)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthConnectionPoolUri(self):
        pool_size = 5

        conn = connection.ConnectionPool(
            "mongodb://{0}:{1}@{2}:{3}/{4}".format(
                self.login1, self.password1, self.host, self.port, self.db1
            )
        )
        db = conn.get_default_database()
        coll = db[self.coll]

        n = pool_size + 1

        try:
            yield defer.gatherResults([coll.insert_one({"x": 42}) for _ in range(n)])

            cnt = yield coll.count()
            self.assertEqual(cnt, n)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthConnectionPoolUriAuthSource(self):
        def authenticate(database, username, password, mechanism):
            self.assertEqual(database, self.db2)

        with patch(
            "txmongo.connection.ConnectionPool.authenticate", side_effect=authenticate
        ):
            conn = connection.ConnectionPool(
                "mongodb://{0}:{1}@{2}:{3}/{4}?authSource={5}".format(
                    self.login1,
                    self.password1,
                    self.host,
                    self.port,
                    self.db1,
                    self.db2,
                )
            )
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthFailAtStartup(self):
        conn = self.__get_connection()
        db = conn[self.db1]

        try:
            db.authenticate(self.login1, self.password1 + "x")
            yield self.assertFailure(
                conn[self.db1][self.coll].find_one(), OperationFailure
            )
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthFailAtRuntime(self):
        conn = self.__get_connection()
        db = conn[self.db1]

        try:
            yield self.assertFailure(
                conn[self.db1][self.coll].find_one(), OperationFailure
            )
            yield self.assertFailure(
                db.authenticate(self.login1, self.password1 + "x"),
                MongoAuthenticationError,
            )
            yield self.assertFailure(
                conn[self.db1][self.coll].find_one(), OperationFailure
            )
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthOnTwoDBsParallel(self):
        if self.ismaster["maxWireVersion"] >= 17:
            raise unittest.SkipTest(
                "MongoDB>=6.0 doesn't allow multiple authentication"
            )

        conn = self.__get_connection()

        try:
            yield self.assertFailure(
                conn[self.db1][self.coll].find_one(), OperationFailure
            )

            yield defer.gatherResults(
                [
                    conn[self.db1].authenticate(self.login1, self.password1),
                    conn[self.db2].authenticate(self.login2, self.password2),
                ]
            )

            yield defer.gatherResults(
                [
                    conn[self.db1][self.coll].insert_one({"x": 42}),
                    conn[self.db2][self.coll].insert_one({"x": 42}),
                ]
            )
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthOnTwoDBsSequential(self):
        if self.ismaster["maxWireVersion"] >= 17:
            raise unittest.SkipTest(
                "MongoDB>=6.0 doesn't allow multiple authentication"
            )

        conn = self.__get_connection()

        try:
            yield self.assertFailure(
                conn[self.db1][self.coll].find_one(), OperationFailure
            )

            yield conn[self.db1].authenticate(self.login1, self.password1)
            yield conn[self.db1][self.coll].find_one()

            yield conn[self.db2].authenticate(self.login2, self.password2)
            yield conn[self.db2][self.coll].find_one()
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_MechanismArg(self):
        conn = self.__get_connection()
        try:
            # Force connect
            yield self.assertFailure(
                conn[self.db1][self.coll].find_one(), OperationFailure
            )

            auth = conn[self.db1].authenticate(
                self.login1, self.password1, mechanism="XXX"
            )
            yield self.assertFailure(auth, MongoAuthenticationError)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_Explicit_SCRAM_SHA_1(self):
        if self.ismaster["maxWireVersion"] < 3:
            raise unittest.SkipTest("This test is only applicable to MongoDB >= 3")

        conn = self.__get_connection()
        try:
            yield conn[self.db1].authenticate(
                self.login1, self.password1, mechanism="SCRAM-SHA-1"
            )
            yield conn[self.db1][self.coll].find_one()
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_InvalidArgs(self):
        conn = self.__get_connection()
        try:
            yield self.assertRaises(
                TypeError, conn[self.db1].authenticate, self.login1, 123
            )
            yield self.assertRaises(
                TypeError, conn[self.db1].authenticate, 123, self.password1
            )
        finally:
            yield conn.disconnect()


class TestX509(MongoAuth, unittest.TestCase):
    # Commands used to generate following keys and certs:
    #
    # openssl genrsa 2048 >ca-key.pem
    # openssl req -new -x509 -nodes -days 9999 -key ca-key.pem -subj "/CN=testing/O=txmongo" >ca-cert.pem
    #
    # openssl req -newkey rsa:2048 -days 9999 -nodes -keyout client-key.pem -subj "/DC=client/O=txmongo" >client-req.pem
    # openssl x509 -req -in client-req.pem -days 9999 -CA ca-cert.pem -CAkey ca-key.pem -set_serial 01 >client-cert.pem
    #
    # openssl req -newkey rsa:2048 -days 9999 -nodes -keyout server-key.pem -subj "/DC=server/O=txmongo" >server-req.pem
    # openssl x509 -req -in server-req.pem -days 9999 -CA ca-cert.pem -CAkey ca-key.pem -set_serial 01 >server-cert.pem
    #
    # rm server-req.pem client-req.pem ca-key.pem

    ca_subject = "CN=testing,O=txmongo"
    ca_cert = """
-----BEGIN CERTIFICATE-----
MIIDKzCCAhOgAwIBAgIUfW52c3NklOEH66+DD1eLWok2EK4wDQYJKoZIhvcNAQEL
BQAwJDEQMA4GA1UEAwwHdGVzdGluZzEQMA4GA1UECgwHdHhtb25nbzAgFw0yMzAy
MDIyMTMxMjlaGA8yMDUwMDYxOTIxMzEyOVowJDEQMA4GA1UEAwwHdGVzdGluZzEQ
MA4GA1UECgwHdHhtb25nbzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
ALv15W5UlwcTzHYJsg0YNOWF5rbMXZrSVJNKHpv3/OoSe/Ak4QMEyjE2+zXn8VHm
f2GWf1wQVr79Ayooc4QAeTBaBWnz2HfhhCBWsYfR8BB+ZwXSlcXqBKzCYcV19ap2
glIryKp8ubrNMVDOqZfpgkX5HsQ6cWuYeb9UAqybAdlP1LwDazUYYd+3OyOStILc
d7aIBRL7aUv0LXTVWGeOwDJqteWJTyEpnz3B0TP6QFcXt4OGfzjq8lgWXM97AH1m
RBYN0a3QvvsD68phIvC35FsDgWafCJI8nehxX167mcPJux9DUR8Sgtoe7r/f6LYm
gayrcg1AozWbYX0uQhTasTUCAwEAAaNTMFEwHQYDVR0OBBYEFNtfUTHynRVLJ5er
Nad8h/2paNpSMB8GA1UdIwQYMBaAFNtfUTHynRVLJ5erNad8h/2paNpSMA8GA1Ud
EwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAIl1N2YTGr1KmbQr01qEdmdF
GA37kI2Zft0JeeIcb3rZBLouqQrARxxEEqizLJxkPa1kSJxbFZO75z0Ddi3Hc/KS
CXtrO9CZhB7iGirHHDGoyl1aCT0M5X1Tn1hqSIgHKK47C09YF3vVpPp76uCpI5zd
UyEHxKCqPmR4P0Z0vjEFa3j8RqVnhVab+MpCnNFvQDQzXyS7YRe81kozJD4GjePq
o97LZ2FHm1TnrhjSZeai+WPvufeFNi6+Zo2nPwzTHXcrxsz/vTe7yH9aysGqty7q
mcLqFjwDJbpGEiHXizQ7iKU0Y0U2PbOBFQTQPZVeYo4OosxPumoVff56JbqUUYQ=
-----END CERTIFICATE-----
"""

    server_subject = "DC=server,O=txmongo"
    server_keycert = """
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDR9nQN+RnyEPEl
HwlrKzK+MXBqBkjbKeG3aN9SFrA1p8MCXNpSORhC0FXb8H0GM2okxDhYGqyvejIE
VlXAtpWmO6lobONv64eM4A/X7R3kzYEgychKi9MWftZrmwkLlVnuCFynzQhKyoD3
xJg7/2Zz+iHMss+JE5Y3IqiC0XVYYtlb5rqn2V9c38rTbyO0CKbyPaFRYB10I3V2
Q3Jtr5/pYfuqm/StYPyIU7wmC36VK0fTtacyiXsH89B1sftLNnebEQJCxpJYX+Wh
4M3N/guAxb63d4oVXh1/w6cSzEqiHKEJpQz4FapOhn+vucLm3aZeJtioyi80yi7r
ZJ7lhsQDAgMBAAECggEAFUIiSv3jMh6kA9GqJvcnOpmP27RKMSKC74qHPez9J1mW
hYk4M6YDPlSQtOWbCwMupo1VdRoyTMwO/2Bsa0gxS7TzuJZ5yq2qukk/YWnpIm7q
OZMkm+OfWkI6XIfEeS9EfTTMiNLaGZR6TVvS3BRGX8y+GlcqayMYDPoN3LzvaCTf
EV/2h2uZZkCiQRV/fLwnf89VaCjDcIXGRU8rzS/GMiQb9Z5dAV7Sz+pho5EXhBpb
67XKcu6CzskeOqWxj/Vc50sCbgmd0UW5sXCVuhtm1QLRirUfQCNR3CWq2Ravr5+A
LZEFQPaDUL53qf1ztIxRsXRWnw+Y8vvoUPNCu6QtuQKBgQDoj7vnyuR53lr/mV0+
yqYq1rFwRNlmKOknTGHj+zrQITNMLMQe/bEpNL14bJoPTdHaAbElpD3de9XyfBs+
VRlxvaf1cymOhzx6/Ri8Zav572LIUnNkDYq7UGpgpXtFV6DNW4Ql2LsiLPtaGobJ
/hPmkdxX3GR+rR9zwllyURBpaQKBgQDnH6dsNIxjo3BYk66umc6ntG3DOu6NHU8R
zSfJp3Z5oqHEUPFBH6JkedT5jj5U8KoAzxPMC8KgaP3615VNdVDShxcT1K9Trr4H
Wcoz6QULl2FIPeifgpXxw1eKB3t3vzh+Fkmwb3mKJYaUHgAniKAmeBaSLsm6YlOf
1OWb7ThIiwKBgDlDUwhTDAj5+DwhfjU83seDj7NOMJ2YCzjS9POW47dlxWTHTQ84
dkvoIZMqYpDMH5Tnf7/9/gGwQVDwwCCMAbTg9x9X2O6wW/MIqPOSsVLcX91ld+Y1
82gF9/uOI5lCZ2TJWPOctw9GNinMiE3qUuoeFjxIFzKd3DW7sByW20ypAoGANdgm
BQto+X2GGtytAXhLsL5UI/uCv9VoKlmFP3GIJGZDEZ1Z1zYrewT3HPyKgPdgY7fa
gNrsiLptERdQbS87mRBny/Lsta4sD0JX3SgYSM8HlvD1VNPzYtODfWCo3wjpfFZs
7HHL+ucrJy6mEo+937hyabQEwytNGBtdMqpdRmkCgYEArwj4tStyNDiIfcZ8uXel
HEc4riiDJzqU6f32XqleyH/AU5pJaQBqLczINXzxeZWElogcCbxRiGeYJQzi6Npo
mdZ7Pl/RquX4cHsGXmm0HtHdfC3AF8RMN5TsLAYmvEHSSrEyWFLG9x5so27hXG76
MnaD9QB0pdY97/+BCkYo2Z0=
-----END PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIICxDCCAawCAQEwDQYJKoZIhvcNAQELBQAwJDEQMA4GA1UEAwwHdGVzdGluZzEQ
MA4GA1UECgwHdHhtb25nbzAgFw0yMzAyMDIyMTMyMTRaGA8yMDUwMDYxOTIxMzIx
NFowKjEWMBQGCgmSJomT8ixkARkWBnNlcnZlcjEQMA4GA1UECgwHdHhtb25nbzCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANH2dA35GfIQ8SUfCWsrMr4x
cGoGSNsp4bdo31IWsDWnwwJc2lI5GELQVdvwfQYzaiTEOFgarK96MgRWVcC2laY7
qWhs42/rh4zgD9ftHeTNgSDJyEqL0xZ+1mubCQuVWe4IXKfNCErKgPfEmDv/ZnP6
Icyyz4kTljciqILRdVhi2VvmuqfZX1zfytNvI7QIpvI9oVFgHXQjdXZDcm2vn+lh
+6qb9K1g/IhTvCYLfpUrR9O1pzKJewfz0HWx+0s2d5sRAkLGklhf5aHgzc3+C4DF
vrd3ihVeHX/DpxLMSqIcoQmlDPgVqk6Gf6+5wubdpl4m2KjKLzTKLutknuWGxAMC
AwEAATANBgkqhkiG9w0BAQsFAAOCAQEAM25Zv3zKlexzUaqH0xb5rlgwiXmMkHhl
Il8ZEa39Ovt+GXTlgRSxisEaLnkZIppxZoBVI00WqOrU2WTkhAgiNLCic3dBMaHE
zoFvYs/fSyG6YESKNKyHETDgO8/SoLHHJwEYvvuAQMvMY2e1ZaBuxJfF30vPF2VQ
FCcFNslawXi8Wno6/MwgZjdNKfFNgGUq6g41criQKpEHPes2t4xs7LHHq0NMd9rv
IvXPdK6Ff+6bHIlD0HCgJzhJN/dg5TUYVQSpyx5N6N7CcLGrGi4DTDFzAAJIlGFV
orGqPHExKS/m9luc+8Srm4udERVtetbr1nUeLxKckVYESof9kZcd2A==
-----END CERTIFICATE-----
"""

    client_subject = "O=txmongo,DC=client"
    client_cert = """
-----BEGIN CERTIFICATE-----
MIICxDCCAawCAQEwDQYJKoZIhvcNAQELBQAwJDEQMA4GA1UEAwwHdGVzdGluZzEQ
MA4GA1UECgwHdHhtb25nbzAgFw0yMzAyMDIyMTMxNDBaGA8yMDUwMDYxOTIxMzE0
MFowKjEWMBQGCgmSJomT8ixkARkWBmNsaWVudDEQMA4GA1UECgwHdHhtb25nbzCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMhwei+k23Og1nDVtHugRYp+
wgX9IZbIaW1VCBkfhNI9TX8XBXfyvn97WmP6bh/PPENibm6HrTISlwruJ7phLFxi
BcY9W4Q0OkT9yWaBcW0caSzHGxAGu1h7EvDfBHGxJPtiKVc9pk3BDNOcJTFmeiAu
WzhLpMAzPUvZAVOKqnJJFZYfOEZbJEDBWsBl0EYsjaL8opG3lo8SAlfxJPuTwiq4
fFx4Ruc754j4s2y12njiSBHe8R0aCU5UiS2Yh+40Be5JZ/wFX8vAy5DHQlK/Qh8f
Iord+1IaqwgHw1lH//hAEOFG7dUdoTrKjoOX3Pv7nqKw64L1b+6KqFB+ucHeAAkC
AwEAATANBgkqhkiG9w0BAQsFAAOCAQEAEHom27P1m9I85FWU9KZ5ITBPykmA4g0A
zcER1iaaNHs01gkXmEtGxUNWLsREB+PGeg80BygNV7gVHO32xqoxG3IuVviyyIW2
cSb2nKxgf9XcAQ862UqvOiyGEzLSpRlsoD+P/ZLRmeVCCKbZjX/7+2XZIZgJPI9s
p4ZsBJtf1kgiXLKFMTXE5+HaW8zmtLs9fw6KsD6bPkP/QrtwcNeMSAqSydB/HQJv
BUSy8sYbX1T7q695cWa0rMLQ0JcUle7XoeLzHnhR1SgoPr6stZ5XvylaH2oeoFpI
vZpE2c+pmtHJtKSsX8fmTCWerNIHcANlzuEbY6Mlq0PdA5xDNmIsWQ==
-----END CERTIFICATE-----
"""
    client_key = """
-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDIcHovpNtzoNZw
1bR7oEWKfsIF/SGWyGltVQgZH4TSPU1/FwV38r5/e1pj+m4fzzxDYm5uh60yEpcK
7ie6YSxcYgXGPVuENDpE/clmgXFtHGksxxsQBrtYexLw3wRxsST7YilXPaZNwQzT
nCUxZnogLls4S6TAMz1L2QFTiqpySRWWHzhGWyRAwVrAZdBGLI2i/KKRt5aPEgJX
8ST7k8IquHxceEbnO+eI+LNstdp44kgR3vEdGglOVIktmIfuNAXuSWf8BV/LwMuQ
x0JSv0IfHyKK3ftSGqsIB8NZR//4QBDhRu3VHaE6yo6Dl9z7+56isOuC9W/uiqhQ
frnB3gAJAgMBAAECggEAMnqpOwCAyCEUgHxBpPTbLqL3yDxUzj2Q15kXngQjFjOc
JcqtdOTsdvyg6hbyzw4I9kt+1lVTGA44fec2mCjK3EVPgPqI5sWkeyWTKD6nSzxh
fIZ0WVforMLqJUF1RBDB6JFzKeZ70I8y5MQpVcSiMyKZlJbwWftNj6GKRPjiLj+p
b6+EpWI5BrdCTRDDUcppIg11AEXwXoATNlVbY785Ioj5Mz2dt8jjX96crdW/vmPY
85Jh+4CSGdNgEwxqb5OuMnHZpKZXmwYN89NSgyipfLMup9k1bj86uxsw3GPYW7io
AnGoydCYvmBl+Dnm/UNJeTv3ekg/9PSc54hUvy+69QKBgQDW7FB39E18qxiVNwJx
oh4s0A3U9n23JYQtz9elcTTrVdlt0C1maLOsi1MIRT66/Tgvy14ekuVX0SytqwIL
0h+rDm9kkt7TWvzxldl6IeoswgPXsU83WFAyfJosuv0MRmz3YmdEFmPHetHbZeP4
zjsYtIyjOjTPnEk9m4EpCF475wKBgQDuv4HoRudCiPOn5COJyEb6H0Anli+kL8VM
zxEMnzj26TA2LISvXc+rSkzM+bvfMKmH+aTMTnN0awg3uoq8YpaVkGQOEv2BBOUn
HZMuIiwaQmcaGZo5MljnWJn+ywOTpSb/8sXdls/GEUpjSiBqI9Ixk4nReGOnJImS
2IVytC/mjwKBgQDIvIFP+GsjJL7aJz3uGmgqnMw+e1bh53V0QGr+yCSQJWfmDAlL
XEFsx9huohY8GeQPp9epoLP6eJFNR6qqlcAkSWA7RH5AU/xqO/aa/vA0i6WBIMeH
PsHw1bY+TwuoGmMOD+e5jVv2Zb16OMlbimth4Mh2cBgBTMyPcR2K0JkLQwKBgQDq
ZuvMeZCkKoHO3JZnuFK3T+dU9odf0+cKOmKq2ci74saK6bp70scJjbFJrgdeYhCQ
h/HdBUkli9BNpsB/pOvjfBhAGTT7byrs9ISETtiuD3PalDhu35eZy8ul9PxWAVgB
AwJxRJhKBr/aK/UXQIQmWIx/NIvilmTb3+llbLN60QKBgQCVnhbpK9WIjVHcVg6f
RMiYJvUU+4c768HbJIWFKZ7amvQdS+cMLbpKAEyRwOGc9JldprXVfJwwkrAolVxb
iS6s96Kcmo3XxhXq5KJ0gza5ElWwNsGrE/4SN5IW1ODljH2dvjePUTy8UK+3HWDS
SJob7FjDAWWJeLCfsmu6Vy2OVg==
-----END PRIVATE KEY-----
"""

    @staticmethod
    def __create_keyfile(content):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.write(content.encode())
        tmp.close()
        os.chmod(tmp.name, 0o666)
        return tmp.name

    @defer.inlineCallbacks
    def setUp(self):
        self.dbpath = tempfile.mkdtemp()
        os.chmod(self.dbpath, 0o777)

        self.server_keyfile = self.__create_keyfile(self.server_keycert)
        self.ca_certfile = self.__create_keyfile(self.ca_cert)
        self.client_keyfile = self.__create_keyfile(self.client_key)
        self.client_certfile = self.__create_keyfile(self.client_cert)

        self.ssl_factory = ssl.DefaultOpenSSLContextFactory(
            privateKeyFileName=self.client_keyfile,
            certificateFileName=self.client_certfile,
        )
        self.mongod = None

        self.mongod_noauth = create_mongod(
            port=self.port, auth=False, dbpath=self.dbpath
        )

        try:
            yield self.mongod_noauth.start()
        except TimeoutError as e:
            yield self.tearDown()
            raise e

        try:
            conn = connection.MongoConnection("localhost", self.port)

            yield conn["$external"].command(
                "createUser",
                self.client_subject,
                roles=[{"role": "root", "db": "admin"}],
            )
        finally:
            yield conn.disconnect()
            yield self.mongod_noauth.stop()

        self.mongod = create_mongod(
            port=self.port,
            auth=True,
            dbpath=self.dbpath,
            args=["--clusterAuthMode", "x509"],
            tlsCertificateKeyFile=self.server_keyfile,
            tlsCAFile=self.ca_certfile,
        )
        try:
            yield self.mongod.start()
        except:
            print(self.mongod.output())
            raise

    @defer.inlineCallbacks
    def tearDown(self):
        if self.mongod_noauth:
            yield self.mongod_noauth.stop()
        if self.mongod:
            yield self.mongod.stop()
        shutil.rmtree(self.dbpath)
        os.unlink(self.server_keyfile)
        os.unlink(self.ca_certfile)
        os.unlink(self.client_keyfile)
        os.unlink(self.client_certfile)

    @defer.inlineCallbacks
    def test_auth(self):
        conn = connection.MongoConnection(
            port=self.port, ssl_context_factory=self.ssl_factory
        )
        yield self.assertFailure(conn.db.coll.find(), OperationFailure)
        try:
            yield conn.db.authenticate(
                self.client_subject, "", mechanism="MONGODB-X509"
            )
            yield conn.db.coll.insert_one({"x": 42})
            cnt = yield conn.db.coll.count()
            self.assertEqual(cnt, 1)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_fail(self):
        conn = connection.MongoConnection(
            port=self.port, ssl_context_factory=self.ssl_factory
        )
        yield self.assertFailure(conn.db.coll.find(), OperationFailure)
        try:
            auth = conn.db.authenticate(
                "DC=another,O=txmongo", "", mechanism="MONGODB-X509"
            )
            yield self.assertFailure(auth, MongoAuthenticationError)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_lazy_fail(self):
        conn = connection.MongoConnection(
            port=self.port, ssl_context_factory=self.ssl_factory
        )
        try:
            yield conn.db.authenticate(
                "DC=another,O=txmongo", "", mechanism="MONGODB-X509"
            )
            yield self.assertFailure(conn.db.coll.find(), OperationFailure)
        finally:
            yield conn.disconnect()
