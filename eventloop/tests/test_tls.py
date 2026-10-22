"""
Tests for TLS/SSL support in the stream layer.
"""

import asyncio
import os
import socket
import ssl
import tempfile

import pytest

from eventloop import new_event_loop, run, sleep
from eventloop.streams import open_connection, start_server


@pytest.fixture(scope="module")
def certfiles():
    """Create a self-signed certificate for the tests."""
    pytest.importorskip("cryptography")
    tmpdir = tempfile.mkdtemp(prefix="eventloop-tls-")
    certfile = os.path.join(tmpdir, "cert.pem")
    keyfile = os.path.join(tmpdir, "key.pem")
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
        import datetime
    except ImportError as exc:  # pragma: no cover
        pytest.skip(f"cryptography is required to generate test certificates: {exc}")
    
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
    ])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
            ]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    
    with open(certfile, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(keyfile, "wb") as f:
        f.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
    
    yield certfile, keyfile
    try:
        os.unlink(certfile)
        os.unlink(keyfile)
        os.rmdir(tmpdir)
    except OSError:
        pass


def _server_context(certfile, keyfile):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile, keyfile)
    return ctx


def _client_context():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def test_tls_client_and_server(certfiles):
    """A TLS connection must carry encrypted application data."""
    certfile, keyfile = certfiles
    server_ctx = _server_context(certfile, keyfile)
    client_ctx = _client_context()
    
    received: list[bytes] = []
    server_ready = None
    
    async def handle(reader, writer):
        data = await reader.read(1024)
        received.append(data)
        writer.write(b"pong:" + data)
        await writer.drain()
        writer.close()
    
    async def scenario():
        server = await start_server(handle, "127.0.0.1", 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        
        reader, writer = await open_connection(
            "127.0.0.1", port, ssl=client_ctx, server_hostname="localhost"
        )
        writer.write(b"ping")
        await writer.drain()
        
        reply = await reader.read(1024)
        writer.close()
        
        server.close()
        await server.wait_closed()
        return reply
    
    reply = run(scenario())
    assert reply == b"pong:ping"
    assert received == [b"ping"]


def test_tls_handshake_failure_is_reported(certfiles):
    """A client that does not trust the server must fail loudly."""
    certfile, keyfile = certfiles
    server_ctx = _server_context(certfile, keyfile)
    
    # A client context that verifies the certificate against the CA bundle
    # (which does not contain our self-signed certificate).
    client_ctx = ssl.create_default_context()
    
    async def handle(reader, writer):
        await reader.read(10)
    
    async def scenario():
        server = await start_server(handle, "127.0.0.1", 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        try:
            await open_connection(
                "127.0.0.1", port, ssl=client_ctx, server_hostname="localhost"
            )
        finally:
            server.close()
            await server.wait_closed()
    
    with pytest.raises((ConnectionError, ssl.SSLError, OSError)):
        run(scenario())


def test_plain_connection_still_works():
    """Plain TCP must be unaffected by the TLS support."""
    async def handle(reader, writer):
        data = await reader.read(1024)
        writer.write(b"echo:" + data)
        await writer.drain()
        writer.close()
    
    async def scenario():
        server = await start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        
        reader, writer = await open_connection("127.0.0.1", port)
        writer.write(b"hello")
        await writer.drain()
        reply = await reader.read(1024)
        writer.close()
        
        server.close()
        await server.wait_closed()
        return reply
    
    assert run(scenario()) == b"echo:hello"


def test_ssl_argument_validation():
    """An invalid ssl argument must be rejected."""
    async def scenario():
        with pytest.raises(TypeError):
            await open_connection("127.0.0.1", 1, ssl="not-a-context")
    
    run(scenario())
