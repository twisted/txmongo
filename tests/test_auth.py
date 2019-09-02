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

from __future__ import absolute_import, division
from mock import patch
from pymongo.errors import OperationFailure
import os
import shutil
import tempfile
from twisted.trial import unittest
from twisted.internet import defer, ssl
from txmongo import connection
from txmongo.protocol import MongoAuthenticationError

from .mongod import Mongod


mongo_host = "localhost"
mongo_port = 27018
mongo_uri = "mongodb://{0}:{1}/".format(mongo_host, mongo_port)


class TestMongoAuth(unittest.TestCase):
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
        return connection.ConnectionPool(mongo_uri, pool_size)

    @defer.inlineCallbacks
    def createUserAdmin(self):
        conn = self.__get_connection()

        try:
            self.ismaster = yield conn.admin.command("ismaster")

            yield conn.admin.command("createUser", self.ua_login,
                                     pwd=self.ua_password,
                                     roles=[{"role": "userAdminAnyDatabase",
                                             "db": "admin"}])

            try:
                # This should fail if authentication enabled in MongoDB since
                # we've created user but didn't authenticated
                yield conn[self.db1][self.coll].find_one()

                yield conn.admin.command("dropUser", self.ua_login)
                raise unittest.SkipTest("Authentication tests require authorization enabled "
                                        "in MongoDB configuration file")
            except OperationFailure:
                pass
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def createDBUsers(self):
        conn = self.__get_connection()
        yield conn["admin"].authenticate(self.ua_login, self.ua_password)

        yield conn[self.db1].command("createUser", self.login1,
                                     pwd=self.password1,
                                     roles=[{"role": "readWrite",
                                             "db": self.db1}])

        yield conn[self.db2].command("createUser", self.login2,
                                     pwd=self.password2,
                                     roles=[{"role": "readWrite",
                                             "db": self.db2}])

        yield conn.disconnect()

    @defer.inlineCallbacks
    def setUp(self):
        self.__mongod = Mongod(port=mongo_port, auth=True)
        yield self.__mongod.start()
        self.addCleanup(self.clean)

        yield self.createUserAdmin()
        yield self.createDBUsers()

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
            yield conn["admin"].command("dropUser", self.ua_login)
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

            yield defer.gatherResults([coll.insert({'x': 42}) for _ in range(n)])

            cnt = yield coll.count()
            self.assertEqual(cnt, n)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthConnectionPoolUri(self):
        pool_size = 5

        conn = connection.ConnectionPool(
            "mongodb://{0}:{1}@{2}:{3}/{4}".format(self.login1, self.password1, mongo_host,
                                                   mongo_port, self.db1)
        )
        db = conn.get_default_database()
        coll = db[self.coll]

        n = pool_size + 1

        try:
            yield defer.gatherResults([coll.insert({'x': 42}) for _ in range(n)])

            cnt = yield coll.count()
            self.assertEqual(cnt, n)
        finally:
            yield conn.disconnect()
    
    @defer.inlineCallbacks
    def test_AuthConnectionPoolUriAuthSource(self):
        def authenticate(database, username, password, mechanism):
            self.assertEqual(database, self.db2)

        with patch('txmongo.connection.ConnectionPool.authenticate', side_effect=authenticate):
            conn = connection.ConnectionPool(
                "mongodb://{0}:{1}@{2}:{3}/{4}?authSource={5}".format(self.login1, self.password1, mongo_host,
                                                                      mongo_port, self.db1, self.db2)
            )
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthFailAtStartup(self):
        conn = self.__get_connection()
        db = conn[self.db1]

        try:
            db.authenticate(self.login1, self.password1 + 'x')
            yield self.assertFailure(conn[self.db1][self.coll].find_one(), OperationFailure)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthFailAtRuntime(self):
        conn = self.__get_connection()
        db = conn[self.db1]

        try:
            yield self.assertFailure(conn[self.db1][self.coll].find_one(), OperationFailure)
            yield self.assertFailure(db.authenticate(self.login1, self.password1 + 'x'),
                                     MongoAuthenticationError)
            yield self.assertFailure(conn[self.db1][self.coll].find_one(), OperationFailure)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthOnTwoDBsParallel(self):
        conn = self.__get_connection()

        try:
            yield self.assertFailure(conn[self.db1][self.coll].find_one(), OperationFailure)

            yield defer.gatherResults([
                conn[self.db1].authenticate(self.login1, self.password1),
                conn[self.db2].authenticate(self.login2, self.password2),
            ])

            yield defer.gatherResults([
                conn[self.db1][self.coll].insert({'x': 42}),
                conn[self.db2][self.coll].insert({'x': 42}),
            ])
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_AuthOnTwoDBsSequential(self):
        conn = self.__get_connection()

        try:
            yield self.assertFailure(conn[self.db1][self.coll].find_one(), OperationFailure)

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
            yield self.assertFailure(conn[self.db1][self.coll].find_one(), OperationFailure)

            auth = conn[self.db1].authenticate(self.login1, self.password1, mechanism="XXX")
            yield self.assertFailure(auth, MongoAuthenticationError)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_Explicit_SCRAM_SHA_1(self):
        if self.ismaster["maxWireVersion"] < 3:
            raise unittest.SkipTest("This test is only applicable to MongoDB >= 3")

        conn = self.__get_connection()
        try:
            yield conn[self.db1].authenticate(self.login1, self.password1, mechanism="SCRAM-SHA-1")
            yield conn[self.db1][self.coll].find_one()
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_InvalidArgs(self):
        conn = self.__get_connection()
        try:
            yield self.assertRaises(TypeError, conn[self.db1].authenticate, self.login1, 123)
            yield self.assertRaises(TypeError, conn[self.db1].authenticate, 123, self.password1)
        finally:
            yield conn.disconnect()


class TestMongoDBCR(unittest.TestCase):

    ua_login = "useradmin"
    ua_password = "useradminpwd"

    db1 = "db1"
    coll = "coll"
    login1 = "user1"
    password1 = "pwd1"

    @defer.inlineCallbacks
    def setUp(self):
        self.dbpath = tempfile.mkdtemp()

        mongod_noauth = Mongod(port=mongo_port, auth=False, dbpath=self.dbpath)
        yield mongod_noauth.start()

        try:
            try:
                conn = connection.MongoConnection(mongo_host, mongo_port)

                server_status = yield conn.admin.command("serverStatus")
                major_version = int(server_status['version'].split('.')[0])
                if major_version != 3:
                    raise unittest.SkipTest("This test is only for MongoDB 3.x")

                # Force MongoDB 3.x to use MONGODB-CR auth schema
                yield conn.admin.system.version.update_one({"_id": "authSchema"},
                                                           {"$set": {"currentVersion": 3}},
                                                           upsert=True)
            finally:
                yield conn.disconnect()
                yield mongod_noauth.stop()
        except unittest.SkipTest:
            shutil.rmtree(self.dbpath)
            raise

        self.mongod = Mongod(port=mongo_port, auth=True, dbpath=self.dbpath)
        yield self.mongod.start()

        try:
            conn = connection.MongoConnection(mongo_host, mongo_port)
            try:
                yield conn.admin.command("createUser", self.ua_login, pwd=self.ua_password,
                                         roles=[{"role": "userAdminAnyDatabase", "db": "admin"}])
                yield conn.admin.authenticate(self.ua_login, self.ua_password)

                yield conn[self.db1].command("createUser", self.login1, pwd=self.password1,
                                             roles=[{"role": "readWrite", "db": self.db1}])
            finally:
                yield conn.disconnect()
        except:
            yield self.mongod.stop()
            raise

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.mongod.stop()
        shutil.rmtree(self.dbpath)

    @defer.inlineCallbacks
    def test_ConnectionPool(self):
        conn = connection.ConnectionPool(mongo_uri)

        try:
            yield conn[self.db1].authenticate(self.login1, self.password1,
                                              mechanism="MONGODB-CR")
            yield conn[self.db1][self.coll].insert_one({'x': 42})
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_ByURI(self):
        uri = "mongodb://{0}:{1}@{2}:{3}/{4}?authMechanism=MONGODB-CR".format(
            self.login1, self.password1, mongo_host, mongo_port, self.db1
        )
        conn = connection.ConnectionPool(uri)
        try:
            yield conn[self.db1][self.coll].insert_one({'x': 42})
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_Fail(self):
        conn = connection.ConnectionPool(mongo_uri)

        try:
            yield conn[self.db1].authenticate(self.login1, self.password1+'x',
                                              mechanism="MONGODB-CR")
            query = conn[self.db1][self.coll].insert_one({'x': 42})
            yield self.assertFailure(query, OperationFailure)
        finally:
            yield conn.disconnect()


class TestX509(unittest.TestCase):

    ca_subject = "CN=testing,O=txmongo"
    ca_cert = """
-----BEGIN CERTIFICATE-----
MIICFjCCAX+gAwIBAgIJAMrkIW39mCd8MA0GCSqGSIb3DQEBCwUAMCQxEDAOBgNV
BAoMB3R4bW9uZ28xEDAOBgNVBAMMB3Rlc3RpbmcwHhcNMTYxMDAzMjA0MjMzWhcN
NDQwMjE5MjA0MjMzWjAkMRAwDgYDVQQKDAd0eG1vbmdvMRAwDgYDVQQDDAd0ZXN0
aW5nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDNO105VaGOMGj0DTAob8gE
zjYDT5jUh7WLjOLRHLzxsOUxcREPkfY57x4+c2fscYzKTGMETcJnjPK1SUbtB5dx
f/f8uplSCbWwQDYc/tOW9FGsUYeocVnt079b72J8zkMnHOZ1e0ro9L8ThZvpbA8E
fEYqOaTqlxCrkhjjmXqVkQIDAQABo1AwTjAdBgNVHQ4EFgQUGLfQGshMNtsO6zSH
mnLMeBC2ZcAwHwYDVR0jBBgwFoAUGLfQGshMNtsO6zSHmnLMeBC2ZcAwDAYDVR0T
BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOBgQAL9VqemuxEQGO3wpIfo+BNjLBZpXsi
n669J7l7vIZeLZOBIaghqGXs0pbxl16upbbFMeYD0zetpcc0J+vLUWorTRRAOSyD
dhIitOrvlbltKtvaGU8vS7Tssl0QWD9uE7ZjSZ+cR4wkvu9yB/JIC+VE0uK7C5uT
qGrjagfQugjW8w==
-----END CERTIFICATE-----
"""
    # CA private key, just for reference
    #
    # -----BEGIN RSA PRIVATE KEY-----
    # MIICXgIBAAKBgQDNO105VaGOMGj0DTAob8gEzjYDT5jUh7WLjOLRHLzxsOUxcREP
    # kfY57x4+c2fscYzKTGMETcJnjPK1SUbtB5dxf/f8uplSCbWwQDYc/tOW9FGsUYeo
    # cVnt079b72J8zkMnHOZ1e0ro9L8ThZvpbA8EfEYqOaTqlxCrkhjjmXqVkQIDAQAB
    # AoGAAInjWL8syV6/J8TRF4oTkE+qPJ/82rHwfAlGnx3gMRIxx8twLAZKCyThg3By
    # GWDC6dUBfYVmuTbZfDhRA1Y9w4FJGZ0ESVP9tnx77eUuZ4Ai0a81sAkpyJNn2Zlh
    # A7DA/oyzm1HenD4Lfqbj8fZh/wYdb9Eg+mXYrqnyl9ViLDECQQD72T2IBzuwhDfN
    # BP5OfF0kzyay29ZuvkeE3tEoSpDDH57mcwSP7xz6IQTyZORaEoqaEMsjVEfTUSnS
    # NyibMMM/AkEA0J1osrhFkDO8nzl8Sq+DbM01NHFojsiwcfbcTCI8wq1/vCv3pv9Z
    # 4zHbZazR+jve7W4H63UPrU2hjmIOYIMDLwJBAIqYmNYdNOoFOTgogVLr+c5h+agA
    # d1dme7FRdcU4k8XtxuKHdYFIU6gLN8+1Wj1/aqsyhrggj45pYhx/omcVRL0CQQCj
    # IKORNTT4OOyjGYGOqUY82w5irtfS5y3KP/4t7ovSs3bx/vON+5kfZoooLIaZhR2i
    # TesVfJlArDbLrvONFoVzAkEAlFO+Jx429C9hg3+Plu7YwJ6W9BQ1x0l97GRHLlC0
    # GbNoaMtCG4bKJ70OjHaCFHHiFHxtvFp5zxSALaxbUBm5Kg==
    # -----END RSA PRIVATE KEY-----


    server_subject = "DC=server,O=txmongo"
    server_keycert = """
-----BEGIN RSA PRIVATE KEY-----
MIICWwIBAAKBgQDMzYRDPuQYiRrLvpF7QeKyi6djwARUHj6Yl/dZ7dE8Qah084sa
4x4a9UIE2YPg+Jq4Xr/cVCP4wHXq0Ok2tgTExsAX2uOGheU8AihEAAOL33LiBUq0
C1J9xxL5ZS6aFgGvyMAOReaBpce472kQJlCneCGH3P8vVph/1TPWiaWcQQIDAQAB
AoGAVVpdge0HANa7DSi51uWphgG/3EmdRDVqnwvOcXM0nWk7vKn3UlhPJqsKPZ0t
YigZyzbpvPhwGW6Udi1k1IFdUKoct8ciWu6GTwHh96/jIHDQdgGzEPI8eyqccaBA
Iy3t/jf2To/Wr9n8+oEfjHHHrjChU2YoMPR5ZZ/P3n2228ECQQDq9Xl0OdT8vnLD
ZXF3qNo4cL63JkbwyCTokUhhb1Bo7z22N/0Az9QJtrW7Z2hjdIvuTSn18mFtQWFR
kPLyGU25AkEA3ySzt1ou5f8YuZUwrLaVAnNYNp2l9NbCkM89cPFfRTqY7rtU9+t2
hOrLfXhdwBDGFVulABHm8Kg9dhjoqD9GyQJAYK/9d+eojw1sOp5PMDeq/VjgEoxM
2x7xmUbX60icZWI2Gfs2QRRFJG4soN7v5SV7w+e7IbvJfeVOv/sPDrN8+QJAdRl1
lkqlQd1UxE8edAR8vgR5zm98n7fz8rpOq+5+6H2Ps/hq5o+Sar4se3Om/xvOV3b4
Z8j9QF2Jo2f+8AwEwQJAPaAkfH4UJ/dxQ/6xmn5JGJj8w91hAB1vg4M37tPKBoHx
xm9+lxWGOq3vlPWI9U4mzzPZ0IaCc9Vh4kYoeUhQTA==
-----END RSA PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIIB0DCCATkCFDy1RlQvWM+OjvInm61kkiDaKnN0MA0GCSqGSIb3DQEBCwUAMCQx
EDAOBgNVBAoMB3R4bW9uZ28xEDAOBgNVBAMMB3Rlc3RpbmcwHhcNMTkwODIwMTAz
NDU1WhcNMjkwODE3MTAzNDU1WjAqMRYwFAYKCZImiZPyLGQBGRYGc2VydmVyMRAw
DgYDVQQKDAd0eG1vbmdvMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDMzYRD
PuQYiRrLvpF7QeKyi6djwARUHj6Yl/dZ7dE8Qah084sa4x4a9UIE2YPg+Jq4Xr/c
VCP4wHXq0Ok2tgTExsAX2uOGheU8AihEAAOL33LiBUq0C1J9xxL5ZS6aFgGvyMAO
ReaBpce472kQJlCneCGH3P8vVph/1TPWiaWcQQIDAQABMA0GCSqGSIb3DQEBCwUA
A4GBALJdbrXT41YCidOl6+MKk+NyM+53puPIvnkUH7ymrd/CNae4eYa6qNX+dxIG
oP9y4WM9lqctPGE3JBoohQbmkMxopT2XI/KgCvxNOe/TQje+qnKWkjjKBI/y1WNG
19A8FRZrVO0+DGFUPlMSeqExse0/JgFZNqzSgt7LhE+COTI9
-----END CERTIFICATE-----
"""


    client_subject = "O=txmongo,DC=client"
    client_cert = """
-----BEGIN CERTIFICATE-----
MIIB0DCCATkCFDy1RlQvWM+OjvInm61kkiDaKnN1MA0GCSqGSIb3DQEBCwUAMCQx
EDAOBgNVBAoMB3R4bW9uZ28xEDAOBgNVBAMMB3Rlc3RpbmcwHhcNMTkwODIwMTAz
NTA0WhcNMjkwODE3MTAzNTA0WjAqMRYwFAYKCZImiZPyLGQBGRYGY2xpZW50MRAw
DgYDVQQKDAd0eG1vbmdvMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCSImD6
FUzHJ8eBu0K/hAyv/3muiXYikX0bOwkNdGM3iNp/eLBqi9L+B8nJo0MajxoJvfSj
uG+tQgUcSTUHErd5sQhli4I3cDZJ/9yrH0ZGJj9+RaBdAkETTozMMX/blp/C83R5
rngD5tLex7e/n2JxKPGZXkJ5fnYAGrvRiMfAAQIDAQABMA0GCSqGSIb3DQEBCwUA
A4GBAIZbR0xP5Oi+vHiy7DXnHvQPGYQYgG5/uQ5Jb3+PAgYUmijRgwl6/bmeUYv5
EwDx3sXWmbrCNlkePy1tZN+KXNzusEJQWIS7Tx4Crs2mNjcqvAi+59TOnxTo2ZA6
k+zy+UlhXUuPcigJhyj6dx10SgRu6GQO1rOCtWYWUWmj/eFd
-----END CERTIFICATE-----
"""
    client_key = """
-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQCSImD6FUzHJ8eBu0K/hAyv/3muiXYikX0bOwkNdGM3iNp/eLBq
i9L+B8nJo0MajxoJvfSjuG+tQgUcSTUHErd5sQhli4I3cDZJ/9yrH0ZGJj9+RaBd
AkETTozMMX/blp/C83R5rngD5tLex7e/n2JxKPGZXkJ5fnYAGrvRiMfAAQIDAQAB
AoGAN0Gyo72cG45KHR7+3UYEOiSDEWE+/1E+GibXhHPm9F/WJu8u3grjDFVLkugd
/pPvx5FBSQr7h2r4Xbq8x2DnaRUmzkWbqSOdvKEXER35zXmqF+J0p2nfoaSq2qh4
4rAuUVgn3P9p1dxZqllDBoiuTSMMERwnbaPklvRj5MuDUXECQQDByrJbthPlj1kK
XdANGIUtBTDUg9c5cH5uFr6Lz2ehd/dfyvE9COqIwPDfbwg4sHHHLZ4g3ZCi+fDu
ZYnQOPKFAkEAwQtQUdEB0expSlUriOhueCm6CmkQBYwbDnfRA6ERtsT/0C+8H6t6
Xm++YKQKQ9zRwkK23ChyhKotGdDmtp/2TQJAFqsRJe0scqPL9Ix4s690lImQ5qrt
WAiyoUoDy/Lc2mRgCVKB2XPbi1eWVWx1d7wb8wKBBrMkIgw+hIRYFIU0yQJBAIn2
QPfH7IoPcAwspEla+6ArCgdooIemYqvLW3hBg3xgfAZYJxVnIrQdHizI74EiblJs
BW2ABp/jUwoxLsFzvr0CQQCdvd/scIfqXlf/kxiOwtwoTQDS0MZYNWE1r1E5sAPV
V37FR1u/s35pM/csU6V/hNpOBhrZ4SjxhJy8vAOs9sHA
-----END RSA PRIVATE KEY-----
"""

    @staticmethod
    def __create_keyfile(content):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.write(content.encode())
        tmp.close()
        return tmp.name

    @defer.inlineCallbacks
    def setUp(self):
        self.dbpath = tempfile.mkdtemp()

        self.server_keyfile = self.__create_keyfile(self.server_keycert)
        self.ca_certfile = self.__create_keyfile(self.ca_cert)
        self.client_keyfile = self.__create_keyfile(self.client_key)
        self.client_certfile = self.__create_keyfile(self.client_cert)

        self.ssl_factory = ssl.DefaultOpenSSLContextFactory(
            privateKeyFileName = self.client_keyfile,
            certificateFileName = self.client_certfile,
        )

        mongod_noauth = Mongod(port=mongo_port, auth=False, dbpath=self.dbpath)
        yield mongod_noauth.start()

        try:
            conn = connection.MongoConnection("localhost", mongo_port)

            yield conn["$external"].command("createUser", self.client_subject,
                                            roles=[{"role": "root", "db": "admin"}])
        finally:
            yield conn.disconnect()
            yield mongod_noauth.stop()

        self.mongod = Mongod(port=mongo_port, auth=True, dbpath=self.dbpath,
                             args=["--clusterAuthMode", "x509", "--sslMode", "requireSSL",
                                   "--sslPEMKeyFile", self.server_keyfile,
                                   "--sslCAFile", self.ca_certfile])
        try:
            yield self.mongod.start()
        except:
            print(self.mongod.output())
            raise


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.mongod.stop()
        shutil.rmtree(self.dbpath)
        os.unlink(self.server_keyfile)
        os.unlink(self.ca_certfile)
        os.unlink(self.client_keyfile)
        os.unlink(self.client_certfile)

    @defer.inlineCallbacks
    def test_auth(self):
        conn = connection.MongoConnection(port=mongo_port, ssl_context_factory=self.ssl_factory)
        yield self.assertFailure(conn.db.coll.find(), OperationFailure)
        try:
            yield conn.db.authenticate(self.client_subject, '', mechanism="MONGODB-X509")
            yield conn.db.coll.insert_one({'x': 42})
            cnt = yield conn.db.coll.count()
            self.assertEqual(cnt, 1)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_fail(self):
        conn = connection.MongoConnection(port=mongo_port, ssl_context_factory=self.ssl_factory)
        yield self.assertFailure(conn.db.coll.find(), OperationFailure)
        try:
            auth = conn.db.authenticate("DC=another,O=txmongo", '', mechanism="MONGODB-X509")
            yield self.assertFailure(auth, MongoAuthenticationError)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_lazy_fail(self):
        conn = connection.MongoConnection(port=mongo_port, ssl_context_factory=self.ssl_factory)
        try:
            yield conn.db.authenticate("DC=another,O=txmongo", '', mechanism="MONGODB-X509")
            yield self.assertFailure(conn.db.coll.find(), OperationFailure)
        finally:
            yield conn.disconnect()
