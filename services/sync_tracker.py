# services/sync_tracker.py
"""
Lightweight helpers to ensure each variant belongs to exactly one barcode group
(by normalized barcode) and to avoid noisy duplicate "added to group" logs.
"""

from typing import Optional
from sqlalchemy.orm import Session

import models

def _norm_barcode(b: Optional[str]) -> Optional[str]:
    if not b:
        return None
    return b.strip().replace(" ", "").upper() or None

def ensure_variant_in_group(db: Session, variant: models.ProductVariant) -> None:
    """
    Create barcode group + membership if missing, otherwise no-op.
    This avoids duplicate inserts and duplicate 'added' logs.
    """
    bnorm = _norm_barcode(variant.barcode)
    if not bnorm:
        return

    grp = db.query(models.BarcodeGroup).filter(models.BarcodeGroup.id == bnorm).first()
    if not grp:
        grp = models.BarcodeGroup(id=bnorm, status="active", pool_available=0)
        db.add(grp)
        db.flush()

    exists = (
        db.query(models.GroupMembership)
        .filter(
            models.GroupMembership.variant_id == variant.id,
            models.GroupMembership.group_id == bnorm,
        )
        .first()
    )
    if exists:
        # already a member; no spammy log
        return

    db.add(models.GroupMembership(variant_id=variant.id, group_id=bnorm))
    # Let caller commit; single log line here if you want visibility:
    print(f"Variant {variant.id} added to group '{bnorm}'")
