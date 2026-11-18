import datetime
import os
import tempfile

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from twisted.internet import defer
from twisted.trial import unittest

from tests.conf import MongoConf
from tests.mongod import create_mongod
from txmongo.connection import ConnectionPool
from txmongo.errors import TimeExceeded


def _make_cert(subject, issuer_name, issuer_key, *, ca=False, san=None):
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject)]))
        .issuer_name(
            issuer_name or x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject)])
        )
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
    )
    if san:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(san), critical=False
        )
    cert = builder.sign(issuer_key or key, hashes.SHA256())
    return key, cert


def _pem(key, cert):
    return cert.public_bytes(serialization.Encoding.PEM) + (
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        if key
        else b""
    )


def _write(content):
    fd, path = tempfile.mkstemp(suffix=".pem")
    os.write(fd, content)
    os.close(fd)
    os.chmod(path, 0o644)
    return path


class TestTLSFromURI(unittest.TestCase):
    """Server certificate is issued to "localhost" only"""

    port = MongoConf().auth_test_port

    @defer.inlineCallbacks
    def setUp(self):
        ca_key, ca_cert = _make_cert("txmongo test CA", None, None, ca=True)
        server_key, server_cert = _make_cert(
            "localhost", ca_cert.subject, ca_key, san=[x509.DNSName("localhost")]
        )
        client_key, client_cert = _make_cert("client", ca_cert.subject, ca_key)
        self.ca_file = _write(_pem(None, ca_cert))
        self.client_file = _write(_pem(client_key, client_cert))
        self.addCleanup(os.unlink, self.client_file)
        self.server_file = _write(_pem(server_key, server_cert))
        self.addCleanup(os.unlink, self.ca_file)
        self.addCleanup(os.unlink, self.server_file)

        self.mongod = create_mongod(
            port=self.port,
            tlsCertificateKeyFile=self.server_file,
            tlsCAFile=self.ca_file,
        )
        yield self.mongod.start()
        self.addCleanup(self.mongod.stop)

    def uri(self, host):
        return (
            f"mongodb://{host}:{self.port}/?tls=true&tlsCAFile={self.ca_file}"
            f"&tlsCertificateKeyFile={self.client_file}"
        )

    @defer.inlineCallbacks
    def test_hostname_matches(self):
        conn = ConnectionPool(self.uri("localhost"))
        try:
            yield conn.db.coll.insert_one({"x": 1})
            self.assertEqual((yield conn.db.coll.count_documents({})), 1)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_hostname_mismatch(self):
        conn = ConnectionPool(self.uri("127.0.0.1"), retry_delay=0.1)
        try:
            yield self.assertFailure(conn.db.coll.find_one(timeout=2), TimeExceeded)
        finally:
            yield conn.disconnect()
        self.flushLoggedErrors()

    @defer.inlineCallbacks
    def test_allow_invalid_hostnames(self):
        conn = ConnectionPool(self.uri("127.0.0.1") + "&tlsAllowInvalidHostnames=true")
        try:
            yield conn.db.coll.find_one(timeout=5)
        finally:
            yield conn.disconnect()

    @defer.inlineCallbacks
    def test_untrusted_ca(self):
        # No tlsCAFile: platform trust store doesn't know our test CA
        conn = ConnectionPool(
            f"mongodb://localhost:{self.port}/?tls=true&tlsCertificateKeyFile={self.client_file}",
            retry_delay=0.1,
        )
        try:
            yield self.assertFailure(conn.db.coll.find_one(timeout=2), TimeExceeded)
        finally:
            yield conn.disconnect()
        self.flushLoggedErrors()
