# trendyol_import.py
"""
Import the Trendyol<->Shopify product mapping (from the Scripturi dashboard's trendyol_mapping.json)
into trendyol_mappings, resolve each row's EAN (our pool key), and seed pool state for
single-listing EANs so the sync has an authoritative pool to work with. Idempotent.

    python trendyol_import.py /path/to/trendyol_mapping.json [--apply]

EAN resolution: mapping rows join by shopify_sku; we look the SKU up in product_variants on enabled
synced stores (preferring the mapped store, GRAN=Grandia) and take its barcode. Rows whose SKU
resolves to zero or multiple distinct EANs stay INACTIVE (note says why) — never guess identity.

Pool seeding: a Grandia-only product is a single listing (no multi-store pool), but Trendyol now
makes it a 2-replica pool. For each active mapping without an authoritative pool: if the EAN has
exactly ONE listing, seed PoolState = that listing's live value (single listing = trivially agreed
truth), stamp backfilled_at, and write the per-listing baseline — the same contract as a live-truth
backfill. Multi-listing pools are left to the onboarding sweep (they need cross-listing agreement).
"""
import json
import sys
from datetime import datetime, timezone

from sqlalchemy import text
from database import SessionLocal
from services import live_truth
import models


def run(path: str, apply: bool) -> None:
    data = json.load(open(path, encoding="utf-8"))
    rows = data.get("mapping") or []
    db = SessionLocal()
    imported = resolved = ambiguous = seeded = left_multi = 0
    try:
        for r in rows:
            tb = str(r.get("trendyol_barcode") or "").strip()
            sku = str(r.get("shopify_sku") or "").strip()
            if not tb:
                continue
            hits = db.execute(text("""
                SELECT DISTINCT pv.barcode FROM product_variants pv
                JOIN products p ON p.id = pv.product_id AND p.deleted_at IS NULL
                JOIN stores s ON s.id = pv.store_id AND s.enabled AND s.sync_location_id IS NOT NULL
                WHERE btrim(pv.sku) = :sku AND pv.barcode IS NOT NULL AND btrim(pv.barcode) <> ''
                  AND pv.inventory_item_id IS NOT NULL
            """), {"sku": sku}).fetchall() if sku else []
            eans = sorted({h[0] for h in hits})
            ean, active, note = None, False, None
            if len(eans) == 1:
                ean, active = eans[0], True
                resolved += 1
            elif len(eans) == 0:
                note = "sku not found on synced stores"
                ambiguous += 1
            else:
                note = f"sku maps to {len(eans)} EANs — resolve manually"
                ambiguous += 1
            if apply:
                db.execute(text("""
                    INSERT INTO trendyol_mappings
                        (trendyol_barcode, trendyol_sku, shopify_store, shopify_sku, ean_barcode, active, note)
                    VALUES (:tb,:ts,:st,:sku,:ean,:act,:note)
                    ON CONFLICT (trendyol_barcode) DO UPDATE SET
                        trendyol_sku=:ts, shopify_store=:st, shopify_sku=:sku,
                        ean_barcode=:ean, active=:act, note=:note, updated_at=now()
                """), {"tb": tb, "ts": str(r.get("trendyol_sku") or ""), "st": str(r.get("shopify_store") or ""),
                       "sku": sku, "ean": ean, "act": active, "note": note})
            imported += 1
        if apply:
            db.commit()

        # seed pools for active single-listing EANs
        if apply:
            eans = [x[0] for x in db.execute(text(
                "SELECT DISTINCT ean_barcode FROM trendyol_mappings WHERE active AND ean_barcode IS NOT NULL"
            )).fetchall()]
            for ean in eans:
                st = db.execute(text(
                    "SELECT backfilled_at IS NOT NULL FROM pool_states WHERE barcode=:b"), {"b": ean}).first()
                if st and st[0]:
                    continue                     # already authoritative
                listings = live_truth._group_rows(db, ean)
                if len(listings) != 1:
                    left_multi += 1              # multi-listing: onboarding sweep's job
                    continue
                r0 = listings[0]
                live = live_truth._read_live(r0["shopify_url"], r0["api_token"],
                                             r0["inventory_item_id"], r0["sync_location_id"])
                if live is None:
                    left_multi += 1
                    continue
                q = max(int(live), 0)
                now = datetime.now(timezone.utc)
                db.execute(text("""
                    INSERT INTO pool_states (barcode, quantity, version, backfilled_at, source_timestamp)
                    VALUES (:b,:q,1,:now,:now)
                    ON CONFLICT (barcode) DO UPDATE SET quantity=:q,
                        version=pool_states.version+1, backfilled_at=:now
                """), {"b": ean, "q": q, "now": now})
                db.execute(text("""INSERT INTO pool_events
                    (barcode, source_store_id, source_variant_id, inventory_item_id,
                     observed_quantity, source_timestamp, kind, applied)
                    VALUES (:b,:s,:v,:i,:q, now(), 'backfill_baseline', true)"""),
                    {"b": ean, "s": r0["store_id"], "v": r0["variant_id"],
                     "i": r0["inventory_item_id"], "q": q})
                db.commit()
                seeded += 1
        print("imported=%d resolved=%d unresolved=%d pools_seeded=%d left_for_onboarding=%d apply=%s"
              % (imported, resolved, ambiguous, seeded, left_multi, apply))
    finally:
        db.close()


if __name__ == "__main__":
    args = [a for a in sys.argv[1:]]
    apply = "--apply" in args
    paths = [a for a in args if not a.startswith("--")]
    if not paths:
        print("usage: python trendyol_import.py <trendyol_mapping.json> [--apply]"); sys.exit(1)
    run(paths[0], apply)
