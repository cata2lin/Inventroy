from __future__ import annotations

from datetime import datetime, date, timezone, timedelta
from typing import Optional, Dict, Any, List

from sqlalchemy import text, func
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert

import models

# ---------- Writers ----------

def create_snapshot_for_store(db: Session, store_id: int) -> None:
    """
    Upsert one inventory snapshot row per variant for the given store at normalized day.
    Allows negative inventory. Uses stores.sync_location_id if set, else sums across locations.
    """
    now = datetime.now(timezone.utc)
    # normalize to midnight UTC to keep one row/day per variant+store
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # on_hand by variant
    sync_loc_id = db.query(models.Store.sync_location_id).filter(models.Store.id == store_id).scalar()
    if sync_loc_id:
        onhand_rows = db.execute(
            text("""
                -- CHANGE 1: Use il.available instead of il.on_hand
                SELECT pv.id AS variant_id, COALESCE(il.available, 0) AS on_hand
                FROM product_variants pv
                LEFT JOIN inventory_levels il
                  ON il.variant_id = pv.id AND il.location_id = :loc_id
                WHERE pv.store_id = :store_id
            """),
            {"loc_id": int(sync_loc_id), "store_id": int(store_id)},
        ).fetchall()
    else:
        onhand_rows = db.execute(
            text("""
                -- CHANGE 2: Sum il.available instead of il.on_hand
                SELECT pv.id AS variant_id, COALESCE(SUM(il.available), 0) AS on_hand
                FROM product_variants pv
                LEFT JOIN inventory_levels il
                  ON il.variant_id = pv.id
                WHERE pv.store_id = :store_id
                GROUP BY pv.id
            """),
            {"store_id": int(store_id)},
        ).fetchall()

    onhand_by_variant = {int(r.variant_id): int(r.on_hand) for r in onhand_rows}

    # price/cost per variant
    vrows = db.execute(
        text("""
            SELECT id AS variant_id, price, cost_per_item
            FROM product_variants
            WHERE store_id = :store_id
        """),
        {"store_id": int(store_id)},
    ).fetchall()
    pc_map = {int(r.variant_id): (r.price, r.cost_per_item) for r in vrows}

    rows: List[Dict[str, Any]] = []
    for vid, onh in onhand_by_variant.items():
        price, cost = pc_map.get(vid, (None, None))
        rows.append({
            "date": day,
            "product_variant_id": vid,
            "store_id": int(store_id),
            "on_hand": int(onh),
            "price": price,
            "cost_per_item": cost,
        })

    if not rows:
        return

    stmt = pg_insert(models.InventorySnapshot.__table__).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["date", "product_variant_id", "store_id"],
        set_={
            "on_hand": stmt.excluded.on_hand,
            "price": stmt.excluded.price,
            "cost_per_item": stmt.excluded.cost_per_item,
        },
    )
    db.execute(stmt)
    db.commit()


# ---------- Readers ----------

def get_products_with_velocity(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    store_id: Optional[int] = None,
    q: Optional[str] = None,
    sort_col: str = "days_left",
    sort_order: str = "asc",
    velocity_days: int = 7,
) -> Dict[str, Any]:
    """
    Get products GROUPED BY BARCODE with:
    - Total stock across all stores (from inventory_levels)
    - Combined sales velocity (total units sold/day over velocity_days)
    - Stock days left (total_stock / total_velocity)
    - Store count (number of stores the product is listed on)
    
    Products without barcodes are grouped individually.
    """
    params: Dict[str, Any] = {
        "velocity_days": velocity_days,
        "skip": int(skip),
        "limit": int(limit),
    }
    
    # Build WHERE clauses for initial filtering
    where_clauses = ["1=1"]
    if store_id is not None:
        where_clauses.append("pv.store_id = :store_id")
        params["store_id"] = int(store_id)
    
    # Fuzzy multi-word search: ALL words must match (in any order)
    # Works for: "Set rosu" matching "Set 6 Genti din Piele Ecologica, Model Granulat, Rosu"
    if q:
        search_text = q.strip()
        words = [w.strip() for w in search_text.split() if w.strip()]
        
        if words:
            # Build a condition that requires ALL words to be present in title, sku, or barcode
            word_conditions = []
            for i, word in enumerate(words):
                param_name = f"q{i}"
                # Each word must appear in at least one of: title, sku, barcode
                word_conditions.append(f"(LOWER(p.title) LIKE :{param_name} OR LOWER(pv.sku) LIKE :{param_name} OR LOWER(COALESCE(pv.barcode, '')) LIKE :{param_name})")
                params[param_name] = f"%{word.lower()}%"
            
            # ALL words must match
            where_clauses.append(f"({' AND '.join(word_conditions)})")
    
    where_sql = " AND ".join(where_clauses)
    
    # Sorting - handle special cases
    valid_sort_cols = {
        "days_left": "days_left",
        "velocity": "total_velocity", 
        "current_stock": "total_stock",
        "title": "title",
        "sku": "sku",
        "store_count": "store_count",
    }
    safe_sort = valid_sort_cols.get(sort_col, "days_left")
    so = "ASC" if (sort_order or "").lower() == "asc" else "DESC"
    
    # Handle NULL sorting - NULLs last for both ASC and DESC
    if safe_sort in ("days_left", "total_velocity"):
        order_sql = f"ORDER BY {safe_sort} IS NULL, {safe_sort} {so}"
    else:
        order_sql = f"ORDER BY {safe_sort} {so}"
    
    sql = text(f"""
    WITH variant_stock AS (
        -- Get current stock for each variant from inventory_levels
        SELECT 
            pv.id as variant_id,
            pv.barcode,
            pv.sku,
            pv.store_id,
            p.id as product_id,
            p.title,
            p.image_url,
            COALESCE(SUM(il.available), 0) as stock
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id
        LEFT JOIN inventory_levels il ON il.variant_id = pv.id
        WHERE {where_sql}
        GROUP BY pv.id, pv.barcode, pv.sku, pv.store_id, p.id, p.title, p.image_url
    ),
    oldest_snapshot AS (
        -- Get the oldest snapshot within velocity period for each variant
        SELECT DISTINCT ON (product_variant_id, store_id)
            product_variant_id,
            store_id,
            on_hand as old_stock,
            date as old_date
        FROM inventory_snapshots
        WHERE date >= (CURRENT_DATE - :velocity_days * INTERVAL '1 day')
        ORDER BY product_variant_id, store_id, date ASC
    ),
    variant_velocity AS (
        -- Calculate velocity per variant
        SELECT
            vs.variant_id,
            vs.barcode,
            vs.sku,
            vs.store_id,
            vs.product_id,
            vs.title,
            vs.image_url,
            vs.stock as current_stock,
            os.old_stock,
            CASE 
                WHEN os.old_stock IS NOT NULL AND os.old_date IS NOT NULL 
                     AND (CURRENT_DATE - os.old_date::date) > 0
                THEN GREATEST(0, (os.old_stock - vs.stock)::numeric / (CURRENT_DATE - os.old_date::date))
                ELSE 0
            END as velocity
        FROM variant_stock vs
        LEFT JOIN oldest_snapshot os ON os.product_variant_id = vs.variant_id AND os.store_id = vs.store_id
    ),
    barcode_grouped AS (
        -- Aggregate by barcode (or by variant_id if no barcode)
        SELECT
            COALESCE(NULLIF(barcode, ''), 'NO_BARCODE_' || MIN(variant_id)::text) as group_key,
            COALESCE(NULLIF(barcode, ''), '') as barcode,
            MIN(sku) as sku,
            MIN(title) as title,
            MIN(image_url) as image_url,
            SUM(current_stock) as total_stock,
            SUM(velocity) as total_velocity,
            COUNT(DISTINCT store_id) as store_count,
            CASE 
                WHEN SUM(velocity) > 0 
                THEN SUM(current_stock)::numeric / SUM(velocity)
                ELSE NULL
            END as days_left
        FROM variant_velocity
        GROUP BY COALESCE(NULLIF(barcode, ''), 'NO_BARCODE_' || variant_id::text), barcode
    )
    SELECT 
        bg.*,
        COUNT(*) OVER() as _total_count
    FROM barcode_grouped bg
    {order_sql}
    LIMIT :limit OFFSET :skip
    """)
    
    rows = db.execute(sql, params).mappings().all()
    total = int(rows[0]["_total_count"]) if rows else 0
    
    products: List[Dict[str, Any]] = []
    for r in rows:
        velocity_val = float(r["total_velocity"]) if r["total_velocity"] is not None else None
        days_left_val = float(r["days_left"]) if r["days_left"] is not None else None
        
        products.append({
            "barcode": r["barcode"] if r["barcode"] else None,
            "sku": r["sku"],
            "title": r["title"],
            "image_url": r["image_url"],
            "total_stock": int(r["total_stock"]) if r["total_stock"] is not None else 0,
            "velocity": round(velocity_val, 2) if velocity_val is not None else None,
            "days_left": round(days_left_val, 1) if days_left_val is not None else None,
            "store_count": int(r["store_count"]) if r["store_count"] is not None else 0,
        })
    
    return {"products": products, "total_count": total, "velocity_days": velocity_days}


def get_last_snapshot_date_by_store(db: Session, store_id: Optional[int] = None) -> Optional[datetime]:
    """Get the most recent snapshot date for a store (or all stores if store_id is None)."""
    query = db.query(func.max(models.InventorySnapshot.date))
    if store_id is not None:
        query = query.filter(models.InventorySnapshot.store_id == store_id)
    result = query.scalar()
    return result


def has_snapshot_data(db: Session, store_id: Optional[int] = None) -> bool:
    """Check if any snapshot data exists for the given store."""
    query = db.query(models.InventorySnapshot.id).limit(1)
    if store_id is not None:
        query = query.filter(models.InventorySnapshot.store_id == store_id)
    return query.first() is not None


def get_current_inventory_fallback(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    store_id: Optional[int] = None,
    q: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get current inventory when no snapshots are available.
    This provides a simplified view without historical metrics.
    """
    from sqlalchemy import func as sql_func
    
    # Build base query for variants with their inventory
    query = db.query(
        models.ProductVariant.id.label("variant_id"),
        models.ProductVariant.sku,
        models.ProductVariant.shopify_gid,
        models.ProductVariant.store_id,
        models.ProductVariant.price,
        models.ProductVariant.cost_per_item,
        models.Product.id.label("product_id"),
        models.Product.title.label("product_title"),
        models.Product.image_url,
        sql_func.coalesce(sql_func.sum(models.InventoryLevel.available), 0).label("on_hand"),
    ).join(
        models.Product, models.Product.id == models.ProductVariant.product_id
    ).outerjoin(
        models.InventoryLevel, models.InventoryLevel.variant_id == models.ProductVariant.id
    )
    
    if store_id is not None:
        query = query.filter(models.ProductVariant.store_id == store_id)
    
    if q:
        search_term = f"%{q.strip()}%"
        query = query.filter(
            (models.ProductVariant.sku.ilike(search_term)) |
            (models.Product.title.ilike(search_term))
        )
    
    query = query.group_by(
        models.ProductVariant.id,
        models.ProductVariant.sku,
        models.ProductVariant.shopify_gid,
        models.ProductVariant.store_id,
        models.ProductVariant.price,
        models.ProductVariant.cost_per_item,
        models.Product.id,
        models.Product.title,
        models.Product.image_url,
    )
    
    # Get total count
    total_count = query.count()
    
    # Get paginated results
    rows = query.order_by(models.Product.title).offset(skip).limit(limit).all()
    
    snapshots = []
    for r in rows:
        snapshots.append({
            "id": r.variant_id,
            "date": datetime.now(timezone.utc).date(),
            "store_id": r.store_id,
            "product_variant_id": r.variant_id,
            "on_hand": r.on_hand,
            "product_variant": {
                "id": r.variant_id,
                "shopify_gid": r.shopify_gid,
                "sku": r.sku,
                "product": {
                    "id": r.product_id,
                    "title": r.product_title,
                    "image_url": r.image_url,
                },
            },
            "metrics": {
                # No historical metrics available - show None for all
                "average_stock_level": None,
                "min_stock_level": None,
                "max_stock_level": None,
                "stock_range": None,
                "stock_stddev": None,
                "days_out_of_stock": None,
                "stockout_rate": None,
                "replenishment_days": None,
                "depletion_days": None,
                "total_outflow": None,
                "stock_turnover": None,
                "avg_days_in_inventory": None,
                "dead_stock_days": None,
                "dead_stock_ratio": None,
                "avg_inventory_value": float(r.on_hand) * float(r.price or 0) if r.price else None,
                "stock_health_index": None,
            },
        })
    
    return {"snapshots": snapshots, "total_count": total_count, "is_fallback": True}

