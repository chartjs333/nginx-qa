"""Loopback HTTPS harness. Each service owns its state; peers receive only URLs."""

from __future__ import annotations

import datetime
import ipaddress
import socket
import ssl
import threading
import time
from contextlib import nullcontext
from pathlib import Path

import httpx
import uvicorn
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


def certificate(root: Path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = root / "tls.pem", root / "tls.key"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def client(url, cert):
    return httpx.Client(
        base_url=url,
        verify=ssl.create_default_context(cafile=str(cert)),
        trust_env=False,
        follow_redirects=False,
        timeout=8,
    )


class Server:
    def __init__(self, app, cert, key, port=None):
        self.port = port or free_port()
        self.url = f"https://127.0.0.1:{self.port}"
        self.server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=self.port,
                ssl_certfile=str(cert),
                ssl_keyfile=str(key),
                log_level="error",
                lifespan="off",
                timeout_keep_alive=1,
                timeout_graceful_shutdown=1,
            )
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        deadline = time.monotonic() + 5
        while not self.server.started:
            if not self.thread.is_alive() or time.monotonic() > deadline:
                raise RuntimeError("isolated server failed to start")
            time.sleep(0.01)

    def close(self):
        self.server.should_exit = True
        self.thread.join(timeout=10)
        assert not self.thread.is_alive()


def restore_runtime(database, state):
    """Reopen nginx-owned storage; no external service path enters this function."""
    from nginx_qa.branch_leases import BranchLeaseStore
    from nginx_qa.git_provider import ManagedGitProvider
    from nginx_qa.managed_continuity import ManagedContinuityRuntime
    from nginx_qa.managed_import import ManagedImportStore, TransactionalSprintImporter

    path = Path(database)
    store = ManagedImportStore(path)
    importer = TransactionalSprintImporter(
        state["runtime_config"],
        {},
        store=store,
        branch_leases=BranchLeaseStore(path.parent, database_name=path.name),
        git_provider=ManagedGitProvider(path.parent / "git", {}),
    )
    return ManagedContinuityRuntime(importer, result_verifier=lambda *_: nullcontext())
