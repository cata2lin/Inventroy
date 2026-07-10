# routes/trendyol.py
"""
Trendyol sync management API — everything the UI needs to run the Trendyol<->Shopify stock sync:
overview stats, mapping CRUD (edit/assign EAN, re-resolve by SKU, activate/deactivate), Shopify
variant candidate search, WRITING a barcode onto a Shopify variant (productVariantsBulkUpdate),
manual pushes, run-now controls, problem lists, and activity feeds.
"""
import json
from typing import Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

import models
from database import get_db
from services import trendyol_sync as ts
from services import trendyol_client as ty
from services import audit_logger

router = APIRouter(prefix="/api/trendyol-sync", tags=["Trendyol Sync"])


@router.get("/overview")
def overview(db: Session = Depends(get_db)):
    maps = dict(db.execute(text(
        "SELECT active, count(*) FROM trendyol_mappings GROUP BY 1")).fetchall())
    pushes = dict(db.execute(text(
        "SELECT status, count(*) FROM trendyol_pushes GROUP BY 1")).fetchall())
    pushes_24h = dict(db.execute(text(
        "SELECT status, count(*) FROM trendyol_pushes WHERE created_at >= now() - interval '24 hours' GROUP BY 1")).fetchall())
    orders = db.execute(text("SELECT count(*) FROM trendyol_order_lines")).scalar()
    orders_applied = db.execute(text(
        "SELECT count(*) FROM trendyol_order_lines WHERE applied")).scalar()
    last_rec = db.execute(text("""SELECT to_char(timestamp,'YYYY-MM-DD HH24:MI') ts, details
        FROM audit_logs WHERE action='trendyol_reconcile' ORDER BY timestamp DESC LIMIT 1""")).first()
    rec = {}
    if last_rec:
        d = last_rec[1] or {}
        rec = {"at": last_rec[0], "drift": len(d.get("drift") or []),
               "unapproved": len(d.get("unapproved") or []),
               "not_on_trendyol": len(d.get("not_on_trendyol") or []),
               "unmapped_count": d.get("unmapped_count", 0)}
    return {
        "flags": {"sync_enabled": ts.sync_enabled(), "push_enabled": ts.push_enabled(),
                  "inbound_apply": ts.inbound_apply(), "configured": ty.configured(),
                  "allowlist": sorted(ts.push_allowlist())},
        "mappings": {"active": maps.get(True, 0), "inactive": maps.get(False, 0),
                     "total": sum(maps.values())},
        "pushes": pushes, "pushes_24h": pushes_24h,
        "orders": {"seen": orders, "applied": orders_applied},
        "last_reconcile": rec,
    }


@router.get("/mappings")
def mappings(q: Optional[str] = None, status: str = Query("all"), limit: int = 200,
             db: Session = Depends(get_db)):
    where, params = [], {"limit": min(limit, 500)}
    if q:
        where.append("(m.trendyol_barcode ILIKE :q OR m.trendyol_sku ILIKE :q "
                     "OR m.shopify_sku ILIKE :q OR m.ean_barcode ILIKE :q)")
        params["q"] = f"%{q.strip()}%"
    if status == "active":
        where.append("m.active")
    elif status == "inactive":
        where.append("NOT m.active")
    elif status == "problem":
        where.append("(NOT m.active OR lp.status IN ('failed','rejected'))")
    sql = f"""
        SELECT m.trendyol_barcode, m.trendyol_sku, m.shopify_store, m.shopify_sku,
               m.ean_barcode, m.active, m.note,
               ps.quantity AS pool_q, (ps.backfilled_at IS NOT NULL) AS authoritative,
               lp.quantity AS last_push_q, lp.status AS last_push_status,
               to_char(lp.created_at,'MM-DD HH24:MI') AS last_push_at,
               lp.failure_reasons
        FROM trendyol_mappings m
        LEFT JOIN pool_states ps ON ps.barcode = m.ean_barcode
        LEFT JOIN LATERAL (
            SELECT quantity, status, created_at, failure_reasons FROM trendyol_pushes p
            WHERE p.trendyol_barcode = m.trendyol_barcode ORDER BY p.id DESC LIMIT 1) lp ON true
        {("WHERE " + " AND ".join(where)) if where else ""}
        ORDER BY m.active DESC, m.trendyol_sku NULLS LAST LIMIT :limit"""
    rows = [dict(r) for r in db.execute(text(sql), params).mappings().all()]
    return {"rows": rows, "count": len(rows)}


class MappingPatch(BaseModel):
    ean_barcode: Optional[str] = None
    shopify_sku: Optional[str] = None
    active: Optional[bool] = None
    note: Optional[str] = None


def _resolve_ean_by_sku(db: Session, sku: str):
    hits = db.execute(text("""
        SELECT DISTINCT pv.barcode FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        WHERE btrim(pv.sku) = :sku AND pv.barcode IS NOT NULL AND btrim(pv.barcode) <> ''
          AND pv.inventory_item_id IS NOT NULL"""), {"sku": sku.strip()}).fetchall()
    return sorted({h[0] for h in hits})


@router.post("/mappings/{trendyol_barcode}")
def update_mapping(trendyol_barcode: str, patch: MappingPatch, db: Session = Depends(get_db)):
    m = db.query(models.TrendyolMapping).filter_by(trendyol_barcode=trendyol_barcode).first()
    if not m:
        raise HTTPException(404, "mapping not found")
    if patch.ean_barcode is not None:
        ean = patch.ean_barcode.strip()
        if ean:
            exists = db.execute(text("""
                SELECT 1 FROM product_variants pv
                JOIN products p ON p.id=pv.product_id AND p.deleted_at IS NULL
                JOIN stores s ON s.id=pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
                WHERE pv.barcode=:b AND pv.inventory_item_id IS NOT NULL LIMIT 1"""),
                {"b": ean}).first()
            if not exists:
                raise HTTPException(400, f"EAN {ean} not found on any synced store variant")
            m.ean_barcode, m.active, m.note = ean, True, None
        else:
            m.ean_barcode, m.active = None, False
    if patch.shopify_sku is not None:
        m.shopify_sku = patch.shopify_sku.strip()
    if patch.active is not None:
        m.active = patch.active and bool(m.ean_barcode)
    if patch.note is not None:
        m.note = patch.note[:200]
    db.commit()
    audit_logger.log(category="CONFIG", action="trendyol_mapping_updated",
                     message=f"[{trendyol_barcode}] mapping updated (ean={m.ean_barcode}, active={m.active})",
                     target=trendyol_barcode, severity="INFO")
    return {"ok": True, "active": m.active, "ean_barcode": m.ean_barcode}


@router.post("/mappings/{trendyol_barcode}/resolve")
def resolve_mapping(trendyol_barcode: str, db: Session = Depends(get_db)):
    m = db.query(models.TrendyolMapping).filter_by(trendyol_barcode=trendyol_barcode).first()
    if not m:
        raise HTTPException(404, "mapping not found")
    if not m.shopify_sku:
        raise HTTPException(400, "no shopify_sku to resolve by")
    eans = _resolve_ean_by_sku(db, m.shopify_sku)
    if len(eans) == 1:
        m.ean_barcode, m.active, m.note = eans[0], True, None
        db.commit()
        return {"ok": True, "ean_barcode": eans[0], "active": True}
    m.note = ("sku not found on synced stores" if not eans
              else f"sku maps to {len(eans)} EANs — resolve manually")
    db.commit()
    return {"ok": False, "candidates": eans, "note": m.note}


@router.get("/candidates")
def candidates(q: str, limit: int = 25, db: Session = Depends(get_db)):
    """Search Shopify variants (SKU / barcode / title) to assign to a Trendyol mapping."""
    rows = db.execute(text("""
        SELECT pv.id AS variant_id, s.name AS store, pv.sku,
               coalesce(nullif(btrim(pv.barcode),''), NULL) AS barcode,
               p.title, p.shopify_gid AS product_gid, il.available
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
        JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
        LEFT JOIN inventory_levels il ON il.variant_id = pv.id AND il.location_id = s.sync_location_id
        WHERE pv.inventory_item_id IS NOT NULL AND
              (pv.sku ILIKE :q OR pv.barcode ILIKE :q OR p.title ILIKE :q)
        ORDER BY s.name, pv.sku LIMIT :limit"""),
        {"q": f"%{q.strip()}%", "limit": min(limit, 100)}).mappings().all()
    return {"rows": [dict(r) for r in rows]}


class AssignBarcode(BaseModel):
    variant_id: int
    barcode: str


@router.post("/assign-barcode")
def assign_barcode(payload: AssignBarcode, db: Session = Depends(get_db)):
    """WRITE a barcode onto a Shopify variant (productVariantsBulkUpdate) + mirror locally. Used to
    give EAN-less products an identity so they can join the pool + Trendyol sync."""
    from shopify_service import ShopifyService
    bc = payload.barcode.strip()
    if not bc or len(bc) < 6:
        raise HTTPException(400, "barcode too short")
    row = db.execute(text("""
        SELECT pv.id, pv.sku, p.shopify_gid, s.shopify_url, s.api_token, s.name AS store
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id
        JOIN stores s ON s.id = pv.store_id
        WHERE pv.id = :vid"""), {"vid": payload.variant_id}).mappings().first()
    if not row:
        raise HTTPException(404, "variant not found")
    svc = ShopifyService(store_url=row["shopify_url"], token=row["api_token"])
    res = svc.execute_mutation("updateVariantBarcode", {
        "productId": row["shopify_gid"],
        "variants": [{"id": f"gid://shopify/ProductVariant/{payload.variant_id}", "barcode": bc}],
    })
    ue = (res.get("productVariantsBulkUpdate") or {}).get("userErrors") or []
    if ue:
        raise HTTPException(400, f"Shopify: {ue}")
    db.execute(text("UPDATE product_variants SET barcode=:b WHERE id=:vid"),
               {"b": bc, "vid": payload.variant_id})
    db.commit()
    audit_logger.log(category="CONFIG", action="trendyol_barcode_assigned",
                     message=f"variant {payload.variant_id} ({row['store']} {row['sku']}) barcode set to {bc}",
                     target=bc, severity="INFO")
    return {"ok": True, "barcode": bc}


@router.post("/push/{trendyol_barcode}")
def manual_push(trendyol_barcode: str, db: Session = Depends(get_db)):
    if not ts.push_enabled():
        raise HTTPException(400, "push disabled (TRENDYOL_PUSH_ENABLED)")
    m = db.query(models.TrendyolMapping).filter_by(trendyol_barcode=trendyol_barcode).first()
    if not m or not m.active or not m.ean_barcode:
        raise HTTPException(400, "mapping missing/inactive/unresolved")
    q = ts._authoritative_pool_q(db, m.ean_barcode)
    if q is None:
        raise HTTPException(400, "pool not engine-authoritative for this EAN")
    desired = min(max(q, 0), ty.MAX_STOCK_PER_PRODUCT)
    bid = ts._submit_batch(db, [{"tb": m.trendyol_barcode, "ean": m.ean_barcode, "q": desired}])
    if not bid:
        raise HTTPException(502, "Trendyol submit failed")
    return {"ok": True, "batch": bid, "quantity": desired}


@router.post("/run/{job}")
def run_job(job: str):
    if job == "push":
        return ts.push_sweep()
    if job == "orders":
        return ts.orders_poll()
    if job == "reconcile":
        return ts.reconcile()
    raise HTTPException(404, "unknown job")


@router.get("/problems")
def problems(db: Session = Depends(get_db)):
    inactive = [dict(r) for r in db.execute(text("""
        SELECT trendyol_barcode, trendyol_sku, shopify_sku, note FROM trendyol_mappings
        WHERE NOT active ORDER BY trendyol_sku NULLS LAST LIMIT 100""")).mappings().all()]
    bad_pushes = [dict(r) for r in db.execute(text("""
        SELECT DISTINCT ON (p.trendyol_barcode) p.trendyol_barcode, p.ean_barcode, p.quantity,
               p.status, p.failure_reasons, to_char(p.created_at,'MM-DD HH24:MI') at
        FROM trendyol_pushes p WHERE p.status IN ('failed','rejected')
        ORDER BY p.trendyol_barcode, p.id DESC LIMIT 100""")).mappings().all()]
    # keep only those whose LATEST row is still bad
    bad_pushes = [b for b in bad_pushes if db.execute(text(
        "SELECT status FROM trendyol_pushes WHERE trendyol_barcode=:tb ORDER BY id DESC LIMIT 1"),
        {"tb": b["trendyol_barcode"]}).scalar() in ("failed", "rejected")]
    skipped = [dict(r) for r in db.execute(text("""
        SELECT skip_reason, count(*) AS n FROM trendyol_order_lines
        WHERE skip_reason IS NOT NULL GROUP BY 1 ORDER BY 2 DESC""")).mappings().all()]
    last = db.execute(text("""SELECT details FROM audit_logs WHERE action='trendyol_reconcile'
        ORDER BY timestamp DESC LIMIT 1""")).scalar() or {}
    return {"inactive_mappings": inactive, "bad_pushes": bad_pushes,
            "skipped_order_lines": skipped,
            "unmapped_on_trendyol": (last.get("unmapped_sample") or []),
            "unmapped_count": last.get("unmapped_count", 0),
            "not_on_trendyol": (last.get("not_on_trendyol") or []),
            "drift": (last.get("drift") or [])[:25]}


@router.get("/activity")
def activity(db: Session = Depends(get_db)):
    pushes = [dict(r) for r in db.execute(text("""
        SELECT trendyol_barcode, ean_barcode, quantity, status,
               to_char(created_at,'MM-DD HH24:MI') at, batch_request_id
        FROM trendyol_pushes ORDER BY id DESC LIMIT 60""")).mappings().all()]
    orders = [dict(r) for r in db.execute(text("""
        SELECT order_number, trendyol_barcode, ean_barcode, quantity, order_status,
               applied, skip_reason, to_char(created_at,'MM-DD HH24:MI') at
        FROM trendyol_order_lines ORDER BY id DESC LIMIT 60""")).mappings().all()]
    return {"pushes": pushes, "orders": orders}
