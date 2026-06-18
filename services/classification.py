# services/classification.py
"""
P4 — duplicate/group classification workflow.

Operator-driven review of sync_groups. These functions write ONLY classification metadata
(sync_groups / sync_group_members) — never inventory. Every change is audited. The propagation
engine already enforces the result (services.inventory_sync_service._resolve_group_targets:
QUARANTINED / CONFIRMED_ERROR / sync_enabled=false groups and excluded members never sync).

No automatic destructive actions: nothing here deletes or merges products/variants.
"""
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

import models
from services import audit_logger

VALID_CLASSIFICATIONS = {"ACTIVE", "VALID_SHARED", "SUSPECT_DUPLICATE", "CONFIRMED_ERROR", "QUARANTINED"}
# Classifications that STOP propagation for the whole group.
NON_SYNCING = {"CONFIRMED_ERROR", "QUARANTINED"}


def list_groups(db: Session, classification: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
    rows = db.execute(text("""
        SELECT g.id, g.barcode_key, g.classification, g.sync_enabled, g.authoritative_variant_id,
               g.notes, g.updated_at,
               count(m.variant_id) AS members,
               count(m.variant_id) FILTER (WHERE m.excluded) AS excluded_members,
               count(DISTINCT m.store_id) AS stores
        FROM sync_groups g
        LEFT JOIN sync_group_members m ON m.sync_group_id = g.id
        WHERE (:cls IS NULL OR g.classification = :cls)
        GROUP BY g.id
        ORDER BY (g.classification IN ('SUSPECT_DUPLICATE','CONFIRMED_ERROR','QUARANTINED')) DESC, g.id
        LIMIT :limit
    """), {"cls": classification, "limit": limit}).mappings().all()
    return [dict(r) for r in rows]


def group_detail(db: Session, group_id: int) -> Dict[str, Any]:
    g = db.query(models.SyncGroup).filter(models.SyncGroup.id == group_id).first()
    if not g:
        return {}
    members = db.execute(text("""
        SELECT m.variant_id, m.store_id, m.excluded, s.name AS store, pv.sku, pv.title,
               il.available
        FROM sync_group_members m
        JOIN product_variants pv ON pv.id = m.variant_id
        JOIN stores s ON s.id = m.store_id
        LEFT JOIN inventory_levels il ON il.variant_id = pv.id AND il.location_id = s.sync_location_id
        WHERE m.sync_group_id = :g
        ORDER BY m.excluded, s.name
    """), {"g": group_id}).mappings().all()
    return {"id": g.id, "barcode_key": g.barcode_key, "classification": g.classification,
            "sync_enabled": g.sync_enabled, "authoritative_variant_id": g.authoritative_variant_id,
            "notes": g.notes, "members": [dict(m) for m in members]}


def set_group_classification(db: Session, group_id: int, classification: str,
                             actor: str = "operator", notes: Optional[str] = None) -> Dict[str, Any]:
    classification = (classification or "").upper()
    if classification not in VALID_CLASSIFICATIONS:
        return {"error": f"invalid classification; must be one of {sorted(VALID_CLASSIFICATIONS)}"}
    g = db.query(models.SyncGroup).filter(models.SyncGroup.id == group_id).first()
    if not g:
        return {"error": "group not found"}
    old = g.classification
    g.classification = classification
    # CONFIRMED_ERROR / QUARANTINED also disable sync for the whole group.
    g.sync_enabled = classification not in NON_SYNCING
    if notes is not None:
        g.notes = notes
    db.commit()
    audit_logger.log_config_change(
        actor=actor, action="group_classified",
        message=f"sync_group {group_id} ({g.barcode_key}): {old} -> {classification}",
        details={"group_id": group_id, "barcode_key": g.barcode_key, "from": old,
                 "to": classification, "sync_enabled": g.sync_enabled, "notes": notes})
    return {"group_id": group_id, "classification": classification, "sync_enabled": g.sync_enabled}


def set_member_excluded(db: Session, variant_id: int, excluded: bool,
                        actor: str = "operator") -> Dict[str, Any]:
    m = db.query(models.SyncGroupMember).filter(models.SyncGroupMember.variant_id == variant_id).first()
    if not m:
        return {"error": "member not found"}
    old = m.excluded
    m.excluded = bool(excluded)
    db.commit()
    audit_logger.log_config_change(
        actor=actor, action="member_exclusion_changed",
        message=f"variant {variant_id} in group {m.sync_group_id}: excluded {old} -> {m.excluded}",
        details={"variant_id": variant_id, "group_id": m.sync_group_id, "excluded": m.excluded})
    return {"variant_id": variant_id, "excluded": m.excluded}


def set_authoritative_variant(db: Session, group_id: int, variant_id: int,
                              actor: str = "operator") -> Dict[str, Any]:
    g = db.query(models.SyncGroup).filter(models.SyncGroup.id == group_id).first()
    if not g:
        return {"error": "group not found"}
    g.authoritative_variant_id = variant_id
    db.commit()
    audit_logger.log_config_change(
        actor=actor, action="group_authoritative_set",
        message=f"sync_group {group_id}: authoritative variant -> {variant_id}",
        details={"group_id": group_id, "authoritative_variant_id": variant_id})
    return {"group_id": group_id, "authoritative_variant_id": variant_id}
