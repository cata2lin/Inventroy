# services/trendyol_sync.py
"""
TRENDYOL <-> SHOPIFY STOCK SYNC — Trendyol as one more (webhook-less) replica of the barcode pool.

Trendyol sends NO webhooks, so the sync is poll/push loops on the existing pool engine. Inbound is
STOCK-DELTA, not order-line: Trendyol maintains its own quantity (down on a sale, back up on a
cancel), so a quantity BELOW the value we last SET it to (the per-mapping `ty_accounted_qty` anchor)
is exactly the units sold since we last accounted — folded ONCE. This is immune to the package-split
double-count that per-line folding suffers (the split cron re-issues package ids, so a line would
re-fold under a new key) and to cancellations (which per-line folding could never reverse).

  OUTBOUND  push_sweep (1 min): FOLD-BEFORE-PUSH — for each candidate (pool != last pushed), read
            Trendyol's current qty, fold any sale first, then push the post-fold pool Q. Quantity-only
            coalesced <=1000-item async batches; batchRequestId persisted + polled; item-level
            SUCCESS/FAILED recorded (results expire ~4h); FAILED retried; a confirmed success advances
            the anchor. The per-barcode last-push store also satisfies the 15-min identical-request rule.
  INBOUND   reconcile (~5 min): full approved-products read; per approved listing, fold (accounted
            anchor - current qty) into the pool via the ENGINE (virtual "Trendyol listing" = the
            NULL-variant stream, reseeded to Q so the applied delta is exactly -sold), then converge
            every Shopify listing. Idempotent (webhook_id encodes the anchor->qty transition).
            orders_poll (3 min) is RECORD-ONLY now — the activity/audit feed, never a stock signal.
  RECONCILE the same ~5-min pass then re-pushes remaining pool-vs-Trendyol drift and reports
            unmapped/unapproved/missing items.

OVER-CORRECTION SAFETY (the design contract):
  • Only ENGINE-AUTHORITATIVE pools (backfilled_at set, not rolled back) are pushed or folded — a
    stale pool Q can never be exported.
  • Inbound is stock-delta vs the accounted anchor: a sale folds exactly once (idempotent transition
    key), floored at 0, drop sanity-capped (MAX_INBOUND_DROP); splits/cancels can't inflate it.
  • The anchor advances on every confirmed push AND every fold, so our OWN push is never re-read as a
    sale (no echo), and fold-before-push means a push never SETs Trendyol back up over an unfolded sale.
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
MAX_INBOUND_DROP = int(os.getenv("TRENDYOL_MAX_INBOUND_DROP", "500"))  # a bigger 1-cycle drop = glitch
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

def _account_pushed(db, row) -> None:
    """A confirmed-successful push means Trendyol now holds exactly `row.quantity` from us — advance
    the accounted anchor to it (only if this is the newest push for the barcode, so out-of-order batch
    resolution can't rewind it). This is what makes a later Trendyol qty BELOW it read as sales."""
    db.execute(text("""UPDATE trendyol_mappings SET ty_accounted_qty=:q
        WHERE trendyol_barcode=:tb AND NOT EXISTS (
            SELECT 1 FROM trendyol_pushes p2 WHERE p2.trendyol_barcode=:tb AND p2.id > :id)"""),
        {"q": int(row.quantity), "tb": row.trendyol_barcode, "id": row.id})


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
                # a COMPLETED batch reports item-level outcomes for FAILURES; an item absent from the
                # report was applied (verified live: an 'absent' item's quantity had landed).
                row.status = "success"; done += 1
                _account_pushed(db, row); continue
            if (it.get("status") or "").upper() in ("SUCCESS", "COMPLETED", "OK"):
                row.status = "success"; done += 1
                _account_pushed(db, row)
            else:
                reasons = it.get("failureReasons") or []
                row.failure_reasons = reasons
                # PERMANENT rejections (blacklisted/archived/locked/unknown barcode) must not retry
                # every 16 minutes forever — park them as 'rejected' and surface via reconcile.
                blob = " ".join(str(x) for x in reasons).lower()
                if any(w in blob for w in ("blacklist", "archiv", "lock", "not found", "bulunamad")):
                    row.status = "rejected"
                else:
                    # A repeat failure at the SAME quantity (typically the 15-min identical-request
                    # rejection of a retry) must not loop every 16 minutes forever — park it. It
                    # un-parks automatically the moment the pool quantity changes (desired != last_q),
                    # and the hourly reconcile re-pushes if Trendyol truly differs.
                    prior = db.execute(text("""
                        SELECT 1 FROM trendyol_pushes WHERE trendyol_barcode=:tb AND quantity=:q
                          AND status IN ('failed','rejected') AND id < :id LIMIT 1"""),
                        {"tb": row.trendyol_barcode, "q": row.quantity, "id": row.id}).first()
                    row.status = "rejected" if prior else "failed"
                failed += 1
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


def _safe_push_list(db, cands: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """FOLD-BEFORE-PUSH: for each push candidate, read Trendyol's CURRENT quantity and fold any sale
    (qty below the accounted anchor) into the pool FIRST, then recompute the desired quantity from the
    now-current pool Q. This guarantees a push can never SET Trendyol back up over a sale we hadn't yet
    folded (the outbound-erases-inbound race). A candidate whose fresh read fails is skipped this pass
    (retried next sweep / by reconcile) rather than pushed blind."""
    out = []
    for c in cands:
        tb, ean = c["tb"], c["ean"]
        pr = ty.get_product(tb)
        if not pr.get("ok"):
            continue                                   # read failed — never push blind, retry later
        if pr.get("found") and pr.get("approved") and not pr.get("archived"):
            acc, st = _mapping_anchor(db, tb)
            _seed_anchor(db, tb, pr["quantity"]) if acc is None else None
            if acc is not None and st not in ("submitted", "failed"):
                _inbound_fold(db, tb, ean, pr["quantity"], acc)
        q = _authoritative_pool_q(db, ean)             # re-read: the fold may have moved it
        if q is None:
            continue
        desired = min(max(q, 0), ty.MAX_STOCK_PER_PRODUCT)
        if not pr.get("found") or pr.get("quantity") != desired:
            out.append({"tb": tb, "ean": ean, "q": desired})
    return out


def push_sweep() -> Dict[str, Any]:
    """Scheduled ~1 min. Poll pending batches, then fold-before-push every changed pool quantity."""
    if not (sync_enabled() and ty.configured()):
        return {"disabled": True}
    db = SessionLocal()
    try:
        polled = _poll_submitted_batches(db)
        if not push_enabled():
            return {"read_only": True, **polled}
        cands = _safe_push_list(db, _push_candidates(db))
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

def _apply_sale(db, ean: str, sold: int, webhook_id: str, ts) -> Optional[int]:
    """Fold a decrease of `sold` units into the pool via the virtual 'Trendyol listing' (NULL-variant
    per-source stream), then converge every Shopify listing. Caller HOLDS the per-barcode lock and has
    verified the pool is engine-authoritative. Idempotent on `webhook_id`. The NULL stream is reseeded
    to the current pool Q immediately before the fold, so the applied delta is exactly -sold regardless
    of history (no virtual-baseline drift-to-zero) and stays conservation-correct under the engine.
    Returns the new Q (or the unchanged Q on a duplicate), or None if the pool isn't authoritative."""
    q_now = _authoritative_pool_q(db, ean)
    if q_now is None:
        return None
    if db.execute(text("SELECT 1 FROM pool_events WHERE webhook_id=:w LIMIT 1"),
                  {"w": webhook_id}).first() is not None:
        return q_now                                   # already folded — idempotent no-op
    db.execute(text("""INSERT INTO pool_events
        (barcode, source_store_id, source_variant_id, inventory_item_id,
         observed_quantity, source_timestamp, kind, applied)
        VALUES (:b, NULL, NULL, NULL, :q, now(), 'backfill_baseline', true)"""),
        {"b": ean, "q": q_now})
    db.commit()
    observed = max(int(q_now) - sold, 0)
    ev_id = pool_engine.ingest_event(
        db, barcode=ean, source_store_id=None, source_variant_id=None, inventory_item_id=None,
        observed_quantity=observed, source_timestamp=ts, webhook_id=webhook_id)
    if ev_id is None:
        return q_now
    res = pool_engine.apply_event(db, ev_id, skip_lock=True)
    if res is None or res.get("rejected"):
        # "rejected" is unreachable here by construction (the reseed anchors prev to Q, so the fold
        # is exactly -sold, floored at 0) — guard anyway so a future reject can't KeyError the sweep.
        return None
    pool_engine.converge_pool(db, ean)
    return int(res["quantity"])


def select_lines_for_drop(lines: List[Dict[str, Any]], drop: int) -> tuple:
    """PURE: pick whole order lines (oldest first) whose quantities fit inside the observed
    Trendyol qty drop. Returns (fold_qty, [line ids]). A line bigger than the remaining drop is
    left for a later cycle (fold only what BOTH the orders and the qty movement agree on)."""
    fold_qty, ids = 0, []
    for ln in lines:
        q = int(ln.get("quantity") or 0)
        if q <= 0:
            continue
        if fold_qty + q > drop:
            continue
        fold_qty += q
        ids.append(ln["id"])
    return fold_qty, ids


def _inbound_fold(db, tb: str, ean: str, ty_now: int, acc: Optional[int], ts=None) -> Dict[str, Any]:
    """ORDER-VERIFIED inbound (2026-07-15): a Trendyol qty BELOW the accounted anchor is only the
    TRIGGER — the folded amount must be backed by REAL recorded Trendyol order lines
    (trendyol_order_lines, applied=false, non-cancelled). fold_qty = whole order lines that fit
    inside the observed drop; consumed lines are marked applied so they fold exactly once.

    Why: the previous stock-delta-only version trusted the qty drop itself, and Trendyol
    glitch/limbo reads of 0 (batch in flight, listing state flaps) turned entire anchors into
    phantom \"sales\" — 238 units were removed from Grandia pools on Jul 10-15 with ZERO real
    orders behind them. Now: a drop with NO matching orders folds NOTHING, alerts, and leaves the
    anchor untouched (if real orders are merely late, the ~3-min orders_poll records them and the
    next reconcile folds the verified amount; if it was a glitch, the next outbound push re-asserts
    our stock on Trendyol and the drop evaporates). Direction stays one-way: Shopify pushes stock
    TO Trendyol; Trendyol only ever sends back SALES, never stock levels."""
    if acc is None:
        return {"folded": False, "reason": "no_anchor"}
    drop = int(acc) - int(ty_now)
    if drop <= 0:
        return {"folded": False, "reason": "no_decrease"}
    if drop > MAX_INBOUND_DROP:
        alerting.warning("trendyol.inbound_drop",
                         f"[{tb}] Trendyol qty {acc}->{ty_now} dropped {drop} (> {MAX_INBOUND_DROP}) "
                         f"— NOT folded (looks like a glitch/archive, not sales)",
                         {"tb": tb, "ean": ean, "acc": acc, "ty_now": ty_now})
        return {"folded": False, "reason": "suspicious_drop", "sold": drop}
    if not inbound_apply():
        return {"folded": False, "reason": "dry_run", "sold": drop}

    # ORDER CORROBORATION: unconsumed, non-cancelled order lines for this barcode (14-day lookback).
    lines = [dict(r) for r in db.execute(text("""
        SELECT id, quantity FROM trendyol_order_lines
        WHERE trendyol_barcode = :tb
          AND COALESCE(applied, false) = false
          AND COALESCE(order_status, '') NOT IN ('Cancelled')
          AND order_date_ms >= (extract(epoch from now() - interval '14 days') * 1000)::bigint
        ORDER BY order_date_ms, id""" ), {"tb": tb}).mappings()]
    fold_qty, line_ids = select_lines_for_drop(lines, drop)
    if fold_qty <= 0:
        alerting.warning("trendyol.inbound_unverified",
                         f"[{tb}] Trendyol qty dropped {acc}->{ty_now} (-{drop}) but NO recorded "
                         f"orders back it — NOT folded (glitch/limbo read; anchor kept)",
                         {"tb": tb, "ean": ean, "acc": acc, "ty_now": ty_now, "drop": drop,
                          "unapplied_lines": len(lines)})
        audit_logger.log(category="STOCK", action="trendyol_inbound_unverified",
                         message=f"[{ean}] Trendyol drop -{drop} ({acc}->{ty_now}) has no order lines — skipped",
                         target=ean, severity="WARN",
                         details={"tb": tb, "acc": acc, "ty_now": ty_now, "drop": drop})
        return {"folded": False, "reason": "unverified_drop", "sold": drop}

    handle = dist_lock.acquire(f"barcode:{ean}")
    if handle is None:
        return {"folded": False, "reason": "locked", "sold": fold_qty}
    try:
        new_acc = int(acc) - fold_qty
        new_q = _apply_sale(db, ean, fold_qty, f"trendyol-in:{tb}:{acc}:{new_acc}",
                            ts or datetime.now(timezone.utc))
        if new_q is None:
            return {"folded": False, "reason": "not_authoritative", "sold": fold_qty}
        # Advance the anchor by exactly what was folded (NOT to ty_now: an unverified remainder of
        # the drop must stay visible until orders confirm it or the next push overwrites it).
        db.execute(text("UPDATE trendyol_mappings SET ty_accounted_qty=:a WHERE trendyol_barcode=:tb"),
                   {"a": new_acc, "tb": tb})
        db.execute(text("""UPDATE trendyol_order_lines
                           SET applied = true, skip_reason = 'folded_stock_delta'
                           WHERE id = ANY(:ids)"""), {"ids": line_ids})
        db.commit()
        audit_logger.log(category="STOCK", action="trendyol_sale_folded",
                         message=f"[{ean}] Trendyol sale -{fold_qty} folded (order-verified, {len(line_ids)} "
                                 f"lines; drop {acc}->{ty_now}) -> Q={new_q} ({tb})",
                         target=ean, severity="INFO",
                         details={"sold": fold_qty, "order_lines": len(line_ids), "acc": acc,
                                  "ty_now": ty_now, "new_acc": new_acc, "new_Q": new_q, "tb": tb})
        return {"folded": True, "sold": fold_qty, "new_q": new_q}
    finally:
        dist_lock.release(handle)


def _mapping_anchor(db, tb: str):
    """(ty_accounted_qty, latest_push_status) for a Trendyol barcode. A latest push still 'submitted'
    or 'failed' means Trendyol's state is uncertain -> inbound must wait (don't mistake our own
    in-flight push for a sale)."""
    acc = db.execute(text("SELECT ty_accounted_qty FROM trendyol_mappings WHERE trendyol_barcode=:tb"),
                     {"tb": tb}).scalar()
    st = db.execute(text("SELECT status FROM trendyol_pushes WHERE trendyol_barcode=:tb "
                         "ORDER BY id DESC LIMIT 1"), {"tb": tb}).scalar()
    return acc, st


def _seed_anchor(db, tb: str, ty_now: int) -> None:
    db.execute(text("UPDATE trendyol_mappings SET ty_accounted_qty=:q "
                    "WHERE trendyol_barcode=:tb AND ty_accounted_qty IS NULL"),
               {"q": int(ty_now), "tb": tb})
    db.commit()


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

        # RECORD-ONLY: order lines are the activity/audit feed, NOT the stock signal. Stock is folded
        # from Trendyol's own quantity (stock-delta, in reconcile/push_sweep) — immune to the package-
        # split double-count that per-line folding suffers (the split cron re-issues package ids, so a
        # line would fold again under a new key) and to cancellations (a cancelled line silently
        # un-decrements Trendyol's qty, which stock-delta accounts for; per-line folding could not).
        for nl in new_lines:
            row = db.query(models.TrendyolOrderLine).filter_by(id=nl["row_id"]).first()
            if not row:
                continue
            m = db.query(models.TrendyolMapping).filter_by(trendyol_barcode=nl["tb"]).first()
            row.ean_barcode = m.ean_barcode if (m and m.active) else None
            row.applied = False
            row.skip_reason = "recorded"      # stock handled by stock-delta reconcile, not per-line
            db.commit()
        if new_lines:
            audit_logger.log(category="STOCK", action="trendyol_orders_polled",
                             message=f"Trendyol orders: {len(new_lines)} new line(s) recorded "
                                     f"(stock folded via stock-delta reconcile, not per-line)",
                             severity="INFO", details={"new": len(new_lines)})
        return {"new": len(new_lines), "recorded": len(new_lines)}
    except Exception as e:
        alerting.warning("trendyol.orders_poll", f"orders poll failed: {e}", {})
        return {"error": str(e)}
    finally:
        db.close()


# ---------------------------------------------------------------------------------------------------
# RECONCILE — full-state compare (the poll-world live-truth sweep)
# ---------------------------------------------------------------------------------------------------

def reconcile() -> Dict[str, Any]:
    """Scheduled ~5 min. Full approved-products read, then two passes on ONE snapshot:
      INBOUND  — a Trendyol qty BELOW the accounted anchor = marketplace sales -> fold ONCE
                 (stock-delta, split/cancel-proof) BEFORE any push, so a push can't erase a sale.
      OUTBOUND — remaining pool-vs-Trendyol drift (Shopify-side moves) -> re-push + report.
    Also refreshes the UI snapshot and flags unmapped/unapproved/missing items."""
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
                    imgs = it.get("images") or []
                    ty_stock[bc] = {"q": int(it.get("quantity") or 0),
                                    "approved": bool(it.get("approved", False)),
                                    "archived": bool(it.get("archived", False)),
                                    "title": (it.get("title") or "")[:400],
                                    "image": (imgs[0].get("url") if imgs and isinstance(imgs[0], dict) else None),
                                    "price": it.get("salePrice"), "list_price": it.get("listPrice")}
            page += 1

        # refresh the UI snapshot on every mapping we saw (rich Trendyol data without per-row calls)
        for bc, t in ty_stock.items():
            db.execute(text("""UPDATE trendyol_mappings SET trendyol_title=:ti, trendyol_image=:im,
                    trendyol_price=:pr, trendyol_list_price=:lp, trendyol_quantity=:q,
                    trendyol_approved=:ap, trendyol_archived=:ar, ty_synced_at=now()
                WHERE trendyol_barcode=:bc"""),
                {"ti": t.get("title"), "im": t.get("image"), "pr": t.get("price"),
                 "lp": t.get("list_price"), "q": t["q"], "ap": t["approved"],
                 "ar": t["archived"], "bc": bc})
        db.commit()

        # INBOUND PASS (stock-delta) — fold Trendyol-side sales BEFORE the outbound push below.
        in_folded = in_units = 0
        for m in db.query(models.TrendyolMapping).filter_by(active=True).all():
            if not m.ean_barcode:
                continue
            t = ty_stock.get(m.trendyol_barcode)
            if t is None or not t["approved"] or t["archived"]:
                continue                                   # only approved, listed items carry a stock signal
            acc, st = _mapping_anchor(db, m.trendyol_barcode)
            if acc is None:
                _seed_anchor(db, m.trendyol_barcode, t["q"])   # first sight: establish baseline, no fold
                continue
            if st in ("submitted", "failed"):
                continue                                   # our own push in flight — Trendyol state uncertain
            r = _inbound_fold(db, m.trendyol_barcode, m.ean_barcode, t["q"], acc)
            if r.get("folded"):
                in_folded += 1
                in_units += r["sold"]

        maps = db.query(models.TrendyolMapping).filter_by(active=True).all()
        drift, not_on_ty, unapproved, pushed = [], [], [], 0
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
            if not t["approved"] or t["archived"]:
                # stock pushes only apply to APPROVED, non-archived listings — report, don't push
                if t["q"] != desired:
                    unapproved.append(m.trendyol_barcode)
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
                         message=f"Trendyol reconcile: {len(ty_stock)} items; inbound folded "
                                 f"{in_folded} sale(s)/-{in_units}u; drift={len(drift)} "
                                 f"(re-pushed {pushed}), unapproved-drifting={len(unapproved)}, "
                                 f"mapped-but-missing={len(not_on_ty)}, unmapped-on-trendyol={len(unmapped)}",
                         severity="WARN" if drift else "INFO",
                         details={"inbound_folded": in_folded, "inbound_units": in_units,
                                  "drift": drift[:25], "unapproved": unapproved[:15],
                                  "not_on_trendyol": not_on_ty[:15],
                                  "unmapped_count": len(unmapped), "unmapped_sample": unmapped[:15]})
        if drift and not push_enabled():
            alerting.warning("trendyol.drift",
                             f"Trendyol stock drift on {len(drift)} item(s) (push disabled — read-only)",
                             {"examples": drift[:10]})
        return {"approved": len(ty_stock), "inbound_folded": in_folded, "inbound_units": in_units,
                "drift": len(drift), "pushed": pushed,
                "not_on_trendyol": len(not_on_ty), "unmapped": len(unmapped)}
    except Exception as e:
        alerting.warning("trendyol.reconcile", f"reconcile failed: {e}", {})
        return {"error": str(e)}
    finally:
        db.close()
