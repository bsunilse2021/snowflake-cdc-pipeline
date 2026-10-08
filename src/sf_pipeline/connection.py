"""Snowflake connection helper (password or key-pair auth)."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from .config import Settings


def _load_private_key(path: str, passphrase: str | None) -> bytes:
    from cryptography.hazmat.primitives import serialization

    pem = Path(path).expanduser().read_bytes()
    key = serialization.load_pem_private_key(
        pem, password=passphrase.encode() if passphrase else None
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@contextmanager
def connect(settings: Settings, bootstrap: bool = False):
    """Yield a connection. bootstrap=True connects without warehouse/database
    (they may not exist yet on first run)."""
    import snowflake.connector

    params: dict = {
        "account": settings.account,
        "user": settings.user,
        "role": settings.role,
        "application": "sf_pipeline",
    }
    if not bootstrap:
        params["warehouse"] = settings.warehouse
        params["database"] = settings.database
    if settings.private_key_path:
        params["private_key"] = _load_private_key(
            settings.private_key_path, settings.private_key_passphrase
        )
    else:
        params["password"] = settings.password

    conn = snowflake.connector.connect(**params)
    try:
        yield conn
    finally:
        conn.close()
