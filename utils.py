# utils.py
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import random
from typing import Optional

from sqlalchemy.orm import Session

import models


# ---------------------------------------------------------------------------
# Webhook HMAC verification (Base64-encoded SHA256 HMAC, e.g. from Shopify)
# ---------------------------------------------------------------------------

def verify_hmac(secret: str, data: bytes | str, hmac_header: str) -> bool:
    """
    Verifies an HMAC header (base64 encoded SHA256 digest) against a secret.

    Args:
        secret: The shared secret string.
        data:   The raw request body as bytes or str.
        hmac_header: The header value you received (base64-encoded digest).

    Returns:
        True if valid, False otherwise.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")

    digest = hmac.new(secret.encode("utf-8"), data, hashlib.sha256).digest()
    computed_b64 = base64.b64encode(digest).decode("utf-8")
    # Use constant-time comparison
    return hmac.compare_digest(computed_b64, (hmac_header or "").strip())


# ---------------------------------------------------------------------------
# EAN-13 barcode helpers
# ---------------------------------------------------------------------------

def calculate_ean13_check_digit(base12: str) -> str:
    """
    Compute EAN-13 check digit for a 12-digit numeric string.

    Formula:
      sum_odd = d1 + d3 + d5 + d7 + d9 + d11
      sum_even = d2 + d4 + d6 + d8 + d10 + d12
      total = sum_odd + 3 * sum_even
      check = (10 - (total % 10)) % 10
    """
    if not base12.isdigit() or len(base12) != 12:
        raise ValueError("base12 must be a 12-digit numeric string")

    digits = [int(c) for c in base12]
    sum_odd = sum(digits[0::2])          # positions 1,3,5,7,9,11 (0-based index even)
    sum_even = sum(digits[1::2])         # positions 2,4,6,8,10,12 (0-based index odd)
    total = sum_odd + 3 * sum_even
    check = (10 - (total % 10)) % 10
    return str(check)


def generate_ean13(
    db: Session,
    prefix: str = "594",
    max_tries: int = 100,
    ensure_unique_in_products: bool = False,
) -> str:
    """
    Generate a unique EAN-13 barcode that doesn't already exist in your DB.

    Args:
        db: SQLAlchemy Session.
        prefix: Numeric string used as a prefix (e.g., a GS1 country/company code).
                Must be 1..11 digits so we can build a 12-digit base.
                Default "594" (Romania) as often used in your examples.
        max_tries: Number of attempts to find an unused code before failing.
        ensure_unique_in_products: If True, also check the products table (if it
                                   has a 'barcode' column in your schema).

    Returns:
        A unique 13-digit EAN string.

    Raises:
        RuntimeError if a unique EAN cannot be found within max_tries.
        ValueError for invalid prefix.
    """
    prefix = (prefix or "").strip()
    if not prefix.isdigit():
        raise ValueError("prefix must be numeric")
    if not (1 <= len(prefix) <= 11):
        raise ValueError("prefix length must be between 1 and 11 digits")

    for _ in range(max_tries):
        # Build 12-digit base (prefix + random)
        remaining = 12 - len(prefix)
        body = "".join(str(random.randint(0, 9)) for _ in range(remaining))
        base12 = prefix + body

        check = calculate_ean13_check_digit(base12)
        ean13 = base12 + check

        # Check uniqueness in ProductVariant.barcode
        exists_variant = (
            db.query(models.ProductVariant.id)
              .filter(models.ProductVariant.barcode == ean13)
              .first()
              is not None
        )

        exists_product = False
        if ensure_unique_in_products and hasattr(models, "Product") and hasattr(models.Product, "barcode"):
            exists_product = (
                db.query(models.Product.id)
                  .filter(models.Product.barcode == ean13)  # only if your schema has this column
                  .first()
                  is not None
            )

        if not exists_variant and not exists_product:
            return ean13

    raise RuntimeError("Could not generate a unique EAN-13 after max_tries attempts")


# Optional convenience: deterministic EAN from a seed (e.g., SKU) + fallback
def generate_ean13_from_seed(
    db: Session,
    seed: str,
    prefix: str = "594",
    ensure_unique_in_products: bool = False,
) -> str:
    """
    Generate a deterministic EAN-13 candidate from a seed (like SKU).
    If it collides, fall back to random generation.

    Not cryptographically secure—just a convenience to get stable numbers
    when possible.

    Args are the same as generate_ean13.
    """
    # Hash the seed to an integer, then use part of it for digits
    h = int(hashlib.sha256(seed.encode("utf-8")).hexdigest(), 16)
    body_len = 12 - len(prefix)
    fixed_body = str(h % (10 ** body_len)).zfill(body_len)
    base12 = prefix + fixed_body
    ean13 = base12 + calculate_ean13_check_digit(base12)

    exists_variant = (
        db.query(models.ProductVariant.id)
          .filter(models.ProductVariant.barcode == ean13)
          .first()
          is not None
    )
    exists_product = False
    if ensure_unique_in_products and hasattr(models, "Product") and hasattr(models.Product, "barcode"):
        exists_product = (
            db.query(models.Product.id)
              .filter(models.Product.barcode == ean13)
              .first()
              is not None
        )

    if not exists_variant and not exists_product:
        return ean13

    # fallback to random if the deterministic one is taken
    return generate_ean13(
        db=db,
        prefix=prefix,
        ensure_unique_in_products=ensure_unique_in_products,
    )


__all__ = [
    "verify_hmac",
    "calculate_ean13_check_digit",
    "generate_ean13",
    "generate_ean13_from_seed",
]
