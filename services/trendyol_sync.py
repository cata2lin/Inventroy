# services/trendyol_sync.py
"""
TRENDYOL <-> SHOPIFY STOCK SYNC — Trendyol as one more (webhook-less) replica of the barcode pool.

Trendyol sends NO webhooks, so the sync is three poll/push loops built on the existing pool engine:

  OUTBOUND  push_sweep (1 min): for every ACTIVE mapping whose pool is engine-authoritative, push the
            pool quantity to Trendyol when it differs from the last pushed value. Quantity-only,
            coalesced <=1000-item async batches; batchRequestId persisted + polled; item-level
            SUCCESS/FAILED recorded (results expire server-side in ~4h); FAILED retried. The per-
            barcode last-push store also satisfies Trendyol's 15-minute identical-request rejection.
  INBOUND   orders_poll (3 min): page recent orders; each UNSEEN line (UNIQUE order_id+line_id) folds
            a sale into the pool through the ENGINE (virtual "Trendyol listing" = the NULL-variant
            per-source stream; webhook_id 'trendyol:{order}:{line}' -> structurally idempotent), then
            converges every Shopify listing. Cancelled lines never fold.
  RECONCILE reconcile (hourly): full approved-products read; Trendyol quantity vs pool Q drift ->
            re-push (if allowed) + report; also flags unmapped/unapproved items.

OVER-CORRECTION SAFETY (the design contract):
  • Only ENGINE-AUTHORITATIVE pools (backfilled_at set, not rolled back) are pushed or folded — a
    stale pool Q can never be exported.
  • Inbound folds are delta-per-line against the virtual listing's OWN baseline (never another
    listing's), idempotent per order line, floored at 0, qty sanity-capped.
  • Pushes are absolute quantity SETs (idempotent) and never read-modify-write.
  • Everything is flag-gated and dormant by default:
      TRENDYOL_SYNC_ENABLED   master (jobs no-op when off)
      TRENDYOL_PUSH_ENABLED   outbound writes to Trendyol
      TRENDYOL_INBOUND_APPLY  inbound folds mutate pools (off = DRY-RUN: record + log only)
      TRENDYOL_PUSH_BARCODES  optional canary allowlist of TRENDYOL barcodes (empty = all)
"""
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from sqlalchemy import text
from database import SessionLocal
import models
from services import trendyol_client as ty
from services import audit_logger, alerting, dist_lock, pool_engine, pool_canary


ORDERS_WINDOW_HOURS = int(os.getenv("TRENDYOL_ORDERS_WINDOW_HOURS", "24"))
MAX_LINE_QTY = int(os.getenv("TRENDYOL_MAX_LINE_QTY", "50"))      # sanity cap per order line
RETRY_MINUTES = 16                                                # > Trendyol's 15-min identical window
CANCEL_STATUSES = {"cancelled", "unsupplied", "returned", "undelivered"}


def sync_enabled() -> bool:
    return os.getenv("TRENDYOL_SYNC_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")


def push_enabled() -> bool:
    return os.getenv("TRENDYOL_PUSH_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")


def inbound_apply() -> bool:
    return os.getenv("TRENDYOL_INBOUND_APPLY", "false").strip().lower() in ("1", "true", "yes", "on")


def push_allowlist() -> set:
    raw = os.getenv("TRENDYOL_PUSH_BARCODES", "").strip()
    return {b.strip() for b in raw.split(",") if b.strip()}


def _authoritative_pool_q(db, ean: str) -> Optional[int]:
    """Pool Q for an EAN, ONLY when the engine is authoritative for it (backfilled, not rolled back).
    Returns None otherwise — a non-authoritative Q must never be exported or folded against."""
    row = db.execute(text("""SELECT quantity FROM pool_states
        WHERE barcode=:b AND backfilled_at IS NOT NULL"""), {"b": ean}).first()
    if row is None:
        return None
    if pool_canary.is_rolled_back(db, ean):
        return None
    return int(row[0])


# ---------------------------------------------------------------------------------------------------
# OUTBOUND — push pool Q to Trendyol
# ---------------------------------------------------------------------------------------------------

def _poll_submitted_batches(db) -> Dict[str, int]:
    """Resolve pending batches to item-level outcomes (results expire in ~4h — never leave them)."""
    done = failed = 0
    batch_ids = [r[0] for r in db.execute(text(
        "SELECT DISTINCT batch_request_id FROM trendyol_pushes WHERE status='submitted' "
        "AND batch_request_id IS NOT NULL LIMIT 20")).fetchall()]
    for bid in batch_ids:
        res = ty.get_batch(bid)
        if not res.get("ok"):
            continue
        if res.get("status") not in ("COMPLETED", "FAILED", "DONE"):
            continue                       # still processing — next sweep
        outcomes = {i["barcode"]: i for i in res.get("items", []) if i.get("barcode")}
        rows = db.query(models.TrendyolPush).filter_by(batch_request_id=bid, status="submitted").all()
        for row in rows:
            it = outcomes.get(row.trendyol_barcode)
            if it is None:
                row.status = "success" if not res.get("failed_count") else "failed"
                continue
            if (it.get("status") or "").upper() in ("SUCCESS", "COMPLETED", "OK"):
                row.status = "success"; done += 1
            else:
                row.status = "failed"; row.failure_reasons = it.get("failureReasons"); failed += 1
        db.commit()
        if failed:
            alerting.warning("trendyol.push_failed",
                             f"Trendyol batch {bid}: {failed} item(s) FAILED",
                             {"batch": bid, "failed": failed})
    return {"resolved": done, "failed": failed}


def _push_candidates(db) -> List[Dict[str, Any]]:
    """Active mappings whose authoritative pool Q differs from the last pushed quantity (or whose
    last push FAILED and is past the 15-min identical-request window)."""
    rows = db.execute(text("""
        SELECT m.trendyol_barcode, m.ean_barcode,
               ps.quantity AS pool_q,
               lp.quantity AS last_q, lp.status AS last_status, lp.created_at AS last_at
        FROM trendyol_mappings m
        JOIN pool_states ps ON ps.barcode = m.ean_barcode AND ps.backfilled_at IS NOT NULL
        LEFT JOIN LATERAL (
            SELECT quantity, status, created_at FROM trendyol_pushes p
            WHERE p.trendyol_barcode = m.trendyol_barcode
            ORDER BY p.id DESC LIMIT 1) lp ON true
        WHERE m.active AND m.ean_barcode IS NOT NULL
    """)).mappings().all()
    allow = push_allowlist()
    out = []
    now = datetime.now(timezone.utc)
    for r in rows:
        if allow and r["trendyol_barcode"] not in allow:
            continue
        if pool_canary.is_rolled_back(db, r["ean_barcode"]):
            continue
        desired = min(max(int(r["pool_q"]), 0), ty.MAX_STOCK_PER_PRODUCT)
        if r["last_q"] is None:
            out.append({"tb": r["trendyol_barcode"], "ean": r["ean_barcode"], "q": desired})
        elif int(r["last_q"]) != desired:
            out.append({"tb": r["trendyol_barcode"], "ean": r["ean_barcode"], "q": desired})
        elif r["last_status"] == "failed":
            last_at = r["last_at"]
            if last_at is not None and last_at.tzinfo is None:
                last_at = last_at.replace(tzinfo=timezone.utc)
            if last_at is None or (now - last_at) > timedelta(minutes=RETRY_MINUTES):
                out.append({"tb": r["trendyol_barcode"], "ean": r["ean_barcode"], "q": desired})
    return out


def _submit_batch(db, cands: List[Dict[str, Any]]) -> Optional[str]:
    items = [{"barcode": c["tb"], "quantity": c["q"]} for c in cands]
    res = ty.push_inventory(items)
    if not res.get("ok"):
        alerting.warning("trendyol.push_submit", f"Trendyol push submit failed: {res.get('error')}",
                         {"items": len(items)})
        return None
    bid = res.get("batch_request_id")
    for c in cands:
        db.add(models.TrendyolPush(trendyol_barcode=c["tb"], ean_barcode=c["ean"],
                                   quantity=c["q"], batch_request_id=bid, status="submitted"))
    db.commit()
    audit_logger.log(category="STOCK", action="trendyol_push_submitted",
                     message=f"Trendyol push: {len(cands)} item(s), batch {bid}",
                     severity="INFO", details={"batch": bid, "count": len(cands),
                                               "sample": cands[:10]})
    return bid


def push_sweep() -> Dict[str, Any]:
    """Scheduled ~1 min. Poll pending batches, then push every changed pool quantity."""
    if not (sync_enabled() and ty.configured()):
        return {"disabled": True}
    db = SessionLocal()
    try:
        polled = _poll_submitted_batches(db)
        if not push_enabled():
            return {"read_only": True, **polled}
        cands = _push_candidates(db)
        submitted = 0
        for i in range(0, len(cands), ty.MAX_BATCH_ITEMS):
            if _submit_batch(db, cands[i:i + ty.MAX_BATCH_ITEMS]):
                submitted += len(cands[i:i + ty.MAX_BATCH_ITEMS])
        return {**polled, "submitted": submitted}
    except Exception as e:
        alerting.warning("trendyol.push_sweep", f"push sweep failed: {e}", {})
        return {"error": str(e)}
    finally:
        db.close()


# ---------------------------------------------------------------------------------------------------
# INBOUND — poll orders, fold sales into the pool through the engine
# ---------------------------------------------------------------------------------------------------

def _fold_sale(db, ean: str, qty: int, order_ref: str, order_ts) -> bool:
    """Fold a Trendyol sale of `qty` units into the pool as the virtual 'Trendyol listing'
    (NULL-variant per-source stream), then converge every Shopify listing. Caller verified the pool
    is engine-authoritative. Runs under the same per-barcode advisory lock as webhooks."""
    handle = dist_lock.acquire(f"barcode:{ean}")
    if handle is None:
        return False
    try:
        q_now = _authoritative_pool_q(db, ean)
        if q_now is None:
            return False
        prev = pool_engine.latest_source_observed(db, ean, None)   # virtual listing's own baseline
        if prev is None:
            # first Trendyol event for this pool: seed the virtual baseline at Q (replica joining),
            # exactly like a live-truth backfill seeds real listings.
            db.execute(text("""INSERT INTO pool_events
                (barcode, source_store_id, source_variant_id, inventory_item_id,
                 observed_quantity, source_timestamp, kind, applied)
                VALUES (:b, NULL, NULL, NULL, :q, now(), 'backfill_baseline', true)"""),
                {"b": ean, "q": q_now})
            db.commit()
            prev = q_now
        observed = max(int(prev) - qty, 0)
        ev_id = pool_engine.ingest_event(
            db, barcode=ean, source_store_id=None, source_variant_id=None, inventory_item_id=None,
            observed_quantity=observed, source_timestamp=order_ts, webhook_id=order_ref)
        if ev_id is None:
            return True    # duplicate (already folded) — idempotent no-op
        res = pool_engine.apply_event(db, ev_id, skip_lock=True)
        if res is None:
            return False
        pool_engine.converge_pool(db, ean)
        audit_logger.log(category="STOCK", action="trendyol_sale_folded",
                         message=f"[{ean}] Trendyol sale -{qty} folded -> Q={res['quantity']} ({order_ref})",
                         target=ean, severity="INFO",
                         details={"qty": qty, "new_Q": res["quantity"], "order_ref": order_ref})
        return True
    finally:
        dist_lock.release(handle)


def orders_poll() -> Dict[str, Any]:
    """Scheduled ~3 min. Page recent orders; process UNSEEN lines exactly once."""
    if not (sync_enabled() and ty.configured()):
        return {"disabled": True}
    db = SessionLocal()
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=ORDERS_WINDOW_HOURS)
        new_lines: List[Dict[str, Any]] = []
        page, total_pages = 0, 1
        while page < total_pages and page < 20:
            res = ty.get_orders(int(start.timestamp() * 1000), int(end.timestamp() * 1000), page=page)
            if not res.get("ok"):
                alerting.warning("trendyol.orders_poll", f"orders fetch failed: {res.get('error')}", {})
                break
            total_pages = res.get("total_pages", 1)
            for order in res.get("content", []):
                oid = str(order.get("id") or order.get("orderNumber") or "")
                onum = str(order.get("orderNumber") or "")
                odate = order.get("orderDate")
                ostatus = (order.get("status") or "").strip()
                for line in order.get("lines", []) or []:
                    lid = str(line.get("id") or "")
                    if not oid or not lid:
                        continue
                    ins = db.execute(text("""
                        INSERT INTO trendyol_order_lines
                            (order_id, line_id, order_number, trendyol_barcode, quantity,
                             order_status, order_date_ms)
                        VALUES (:o,:l,:n,:tb,:q,:st,:dm)
                        ON CONFLICT (order_id, line_id) DO NOTHING RETURNING id"""),
                        {"o": oid, "l": lid, "n": onum, "tb": str(line.get("barcode") or ""),
                         "q": int(line.get("quantity") or 0), "st": ostatus, "dm": odate}).first()
                    db.commit()
                    if ins:
                        new_lines.append({"row_id": ins[0], "oid": oid, "lid": lid,
                                          "tb": str(line.get("barcode") or ""),
                                          "qty": int(line.get("quantity") or 0),
                                          "status": ostatus, "odate": odate,
                                          "line_status": (line.get("orderLineItemStatusName") or "").strip()})
            page += 1

        # oldest first so per-source timestamps stay monotonic on the virtual listing stream
        new_lines.sort(key=lambda x: x["odate"] or 0)
        applied = skipped = 0
        for nl in new_lines:
            row = db.query(models.TrendyolOrderLine).filter_by(id=nl["row_id"]).first()
            reason = None
            m = db.query(models.TrendyolMapping).filter_by(trendyol_barcode=nl["tb"]).first()
            ean = m.ean_barcode if (m and m.active) else None
            status_l = (nl["line_status"] or nl["status"] or "").lower()
            if ean is None:
                reason = "unmapped"
            elif any(s in status_l for s in CANCEL_STATUSES):
                reason = f"status:{status_l[:30]}"
            elif nl["qty"] <= 0 or nl["qty"] > MAX_LINE_QTY:
                reason = f"qty_out_of_range:{nl['qty']}"
            elif _authoritative_pool_q(db, ean) is None:
                reason = "pool_not_authoritative"
            elif not inbound_apply():
                reason = "dry_run"
            if row:
                row.ean_barcode = ean
            if reason:
                if row:
                    row.skip_reason = reason
                    db.commit()
                skipped += 1
                continue
            ts = datetime.fromtimestamp((nl["odate"] or 0) / 1000, tz=timezone.utc) if nl["odate"] else datetime.now(timezone.utc)
            ok = _fold_sale(db, ean, nl["qty"], f"trendyol:{nl['oid']}:{nl['lid']}", ts)
            if row:
                row.applied = bool(ok)
                row.skip_reason = None if ok else "fold_failed"
                db.commit()
            applied += 1 if ok else 0
        if new_lines:
            audit_logger.log(category="STOCK", action="trendyol_orders_polled",
                             message=f"Trendyol orders: {len(new_lines)} new line(s), "
                                     f"{applied} folded, {skipped} skipped (apply={inbound_apply()})",
                             severity="INFO",
                             details={"new": len(new_lines), "applied": applied, "skipped": skipped})
        return {"new": len(new_lines), "applied": applied, "skipped": skipped}
    except Exception as e:
        alerting.warning("trendyol.orders_poll", f"orders poll failed: {e}", {})
        return {"error": str(e)}
    finally:
        db.close()


# ---------------------------------------------------------------------------------------------------
# RECONCILE — full-state compare (the poll-world live-truth sweep)
# ---------------------------------------------------------------------------------------------------

def reconcile() -> Dict[str, Any]:
    """Scheduled hourly. Trendyol approved-products quantity vs pool Q; drift -> re-push + report."""
    if not (sync_enabled() and ty.configured()):
        return {"disabled": True}
    db = SessionLocal()
    try:
        ty_stock: Dict[str, Dict[str, Any]] = {}
        page, total_pages = 0, 1
        while page < total_pages and page < 200:
            res = ty.get_approved_products(page=page)
            if not res.get("ok"):
                alerting.warning("trendyol.reconcile", f"products fetch failed: {res.get('error')}", {})
                return {"error": res.get("error")}
            total_pages = res.get("total_pages", 1)
            for it in res.get("content", []):
                bc = str(it.get("barcode") or "")
                if bc:
                    ty_stock[bc] = {"q": int(it.get("quantity") or 0),
                                    "on_sale": bool(it.get("onSale", True)),
                                    "archived": bool(it.get("archived", False))}
            page += 1

        maps = db.query(models.TrendyolMapping).filter_by(active=True).all()
        drift, not_on_ty, pushed = [], [], 0
        for m in maps:
            if not m.ean_barcode:
                continue
            q = _authoritative_pool_q(db, m.ean_barcode)
            if q is None:
                continue
            desired = min(max(q, 0), ty.MAX_STOCK_PER_PRODUCT)
            t = ty_stock.get(m.trendyol_barcode)
            if t is None:
                not_on_ty.append(m.trendyol_barcode)
                continue
            if t["q"] != desired:
                drift.append({"tb": m.trendyol_barcode, "ean": m.ean_barcode,
                              "trendyol": t["q"], "pool": desired})
        if drift and push_enabled():
            cands = [{"tb": d["tb"], "ean": d["ean"], "q": d["pool"]} for d in drift]
            for i in range(0, len(cands), ty.MAX_BATCH_ITEMS):
                if _submit_batch(db, cands[i:i + ty.MAX_BATCH_ITEMS]):
                    pushed += len(cands[i:i + ty.MAX_BATCH_ITEMS])
        unmapped = [bc for bc in ty_stock if not db.query(models.TrendyolMapping)
                    .filter_by(trendyol_barcode=bc).first()]
        audit_logger.log(category="RECONCILIATION", action="trendyol_reconcile",
                         message=f"Trendyol reconcile: {len(ty_stock)} approved items; drift={len(drift)} "
                                 f"(re-pushed {pushed}), mapped-but-missing={len(not_on_ty)}, "
                                 f"unmapped-on-trendyol={len(unmapped)}",
                         severity="WARN" if drift else "INFO",
                         details={"drift": drift[:25], "not_on_trendyol": not_on_ty[:15],
                                  "unmapped_count": len(unmapped), "unmapped_sample": unmapped[:15]})
        if drift and not push_enabled():
            alerting.warning("trendyol.drift",
                             f"Trendyol stock drift on {len(drift)} item(s) (push disabled — read-only)",
                             {"examples": drift[:10]})
        return {"approved": len(ty_stock), "drift": len(drift), "pushed": pushed,
                "not_on_trendyol": len(not_on_ty), "unmapped": len(unmapped)}
    except Exception as e:
        alerting.warning("trendyol.reconcile", f"reconcile failed: {e}", {})
        return {"error": str(e)}
    finally:
        db.close()
