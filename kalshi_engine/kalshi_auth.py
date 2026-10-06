"""
kalshi_auth.py

Generates the signed authentication headers Kalshi requires on every
WebSocket handshake and REST request. Supports both RSA and Ed25519
private keys (Kalshi issues either depending on account/key settings).

Credentials are loaded from .env (never hardcoded, never committed) via
python-dotenv.
"""

import os
import time
import base64
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, ed25519, rsa

load_dotenv()

# Which Kalshi environment to authenticate against. Demo and production
# are entirely separate accounts with separate keypairs — a demo key is
# rejected with HTTP 401 by production and vice versa — so credentials are
# looked up per environment, falling back to the unprefixed names.
KALSHI_ENV = os.getenv("KALSHI_ENV", "demo").lower()


def _cred(name: str) -> str | None:
    """Prefers e.g. KALSHI_PROD_API_KEY_ID, falls back to KALSHI_API_KEY_ID."""
    return os.getenv(f"KALSHI_{KALSHI_ENV.upper()}_{name}") or os.getenv(f"KALSHI_{name}")


KALSHI_API_KEY_ID = _cred("API_KEY_ID")
KALSHI_PRIVATE_KEY_PATH = _cred("PRIVATE_KEY_PATH")


def _load_private_key():
    """Reads and parses the PEM private key file from disk."""
    if not KALSHI_PRIVATE_KEY_PATH:
        raise RuntimeError(
            f"No private key path set for KALSHI_ENV={KALSHI_ENV!r}. Set "
            f"KALSHI_{KALSHI_ENV.upper()}_PRIVATE_KEY_PATH or KALSHI_PRIVATE_KEY_PATH."
        )
    with open(KALSHI_PRIVATE_KEY_PATH, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


def generate_auth_headers(method: str, path: str) -> dict:
    """
    Builds the KALSHI-ACCESS-* headers required on the WebSocket handshake
    (and REST calls). Kalshi verifies the signature server-side using your
    public key, which proves the request came from you without ever
    sending your private key over the wire.

    method: HTTP method, e.g. "GET"
    path: the request path to sign, e.g. "/trade-api/ws/v2"
          (per Kalshi's docs: sign the path WITHOUT the query string)
    """
    if not KALSHI_API_KEY_ID:
        raise RuntimeError(
            f"No API key id set for KALSHI_ENV={KALSHI_ENV!r}. Set "
            f"KALSHI_{KALSHI_ENV.upper()}_API_KEY_ID or KALSHI_API_KEY_ID."
        )

    private_key = _load_private_key()

    timestamp_ms = str(int(time.time() * 1000))
    message = f"{timestamp_ms}{method}{path}".encode("utf-8")

    if isinstance(private_key, ed25519.Ed25519PrivateKey):
        # Ed25519 signing takes just the message — no padding/hash params
        signature = private_key.sign(message)
    elif isinstance(private_key, rsa.RSAPrivateKey):
        # RSA-PSS signing, Kalshi's other supported key type
        signature = private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
    else:
        raise TypeError(f"Unsupported private key type: {type(private_key)}")

    signature_b64 = base64.b64encode(signature).decode("utf-8")

    return {
        "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature_b64,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }