# crud/snapshots.py
from datetime import datetime, date, timedelta, timezone
from typing import List, Optional, Tuple, Dict, Any

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import text, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

import models


def create_snapshot_for_store(db: Session, store_id: int):
    now = datetime.now(timezone.utc)

    inventory_data = (
        db.query(
            models.InventoryLevel.variant_id,
            models.InventoryLevel.on_hand,
            models.ProductVariant.price,
            models.ProductVariant.cost_per_item
        )
        .join(models.ProductVariant, models.InventoryLevel.variant_id == models.ProductVariant.id)
        .filter(models.ProductVariant.store_id == store_id)
        .all()
    )

    if not inventory_data:
        print(f"[SNAPSHOT] No inventory levels to snapshot for store {store_id}.")
        return

    snapshot_entries = []
    for variant_id, on_hand, price, cost in inventory_data:
        snapshot_entries.append({
            "date": now.date(),
            "product_variant_id": variant_id,
            "store_id": store_id,
            "on_hand": on_hand or 0,
            "price": price,
            "cost_per_item": cost,
        })

    if snapshot_entries:
        stmt = pg_insert(models.InventorySnapshot).values(snapshot_entries)
        update_dict = {
            'on_hand': stmt.excluded.on_hand,
            'price': stmt.excluded.price,
            'cost_per_item': stmt.excluded.cost_per_item,
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=['date', 'product_variant_id', 'store_id'],
            set_=update_dict
        )
        db.execute(stmt)
        db.commit()
        print(f"[SNAPSHOT] Successfully created/updated snapshot for store {store_id} with {len(snapshot_entries)} entries.")


def get_snapshots_with_metrics(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    store_id: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[str, Any]:
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=30)

    # Step 1: Calculate metrics
    metrics_sql = text("""
        WITH lagged AS (
          SELECT
            s.product_variant_id,
            s.date,
            s.on_hand AS quantity,
            s.price,
            s.cost_per_item,
            LAG(s.on_hand) OVER (PARTITION BY s.product_variant_id ORDER BY s.date) AS prev_quantity
          FROM inventory_snapshots s
          WHERE s.date BETWEEN :start_date AND :end_date
            AND (:store_id IS NULL OR s.store_id = :store_id)
        ),
        derived AS (
          SELECT
            *,
            (quantity - prev_quantity) AS quantity_change,
            (quantity * cost_per_item) AS inventory_value
          FROM lagged
        )
        SELECT
          product_variant_id,
          AVG(quantity) AS average_stock_level,
          MIN(quantity) AS min_stock_level,
          MAX(quantity) AS max_stock_level,
          (MAX(quantity) - MIN(quantity)) AS stock_range,
          STDDEV_SAMP(quantity) AS stock_stddev,
          COUNT(*) FILTER (WHERE quantity = 0) AS days_out_of_stock,
          100.0 * COUNT(*) FILTER (WHERE quantity = 0) / NULLIF(COUNT(*), 0) AS stockout_rate,
          COUNT(*) FILTER (WHERE quantity_change > 0) AS replenishment_days,
          COUNT(*) FILTER (WHERE quantity_change < 0) AS depletion_days,
          SUM(GREATEST(prev_quantity - quantity, 0)) AS total_outflow,
          SUM(GREATEST(prev_quantity - quantity, 0)) / NULLIF(AVG(quantity), 0) AS stock_turnover,
          COUNT(DISTINCT date) / NULLIF(SUM(GREATEST(prev_quantity - quantity, 0)) / NULLIF(AVG(quantity), 0), 0) AS avg_days_in_inventory,
          COUNT(*) FILTER (WHERE quantity_change = 0) AS dead_stock_days,
          100.0 * COUNT(*) FILTER (WHERE quantity_change = 0) / NULLIF(COUNT(*), 0) AS dead_stock_ratio,
          AVG(inventory_value) AS avg_inventory_value,
          (1 - (COALESCE(100.0 * COUNT(*) FILTER (WHERE quantity = 0) / NULLIF(COUNT(*), 0), 0)) / 100)
          * (1 - (COALESCE(100.0 * COUNT(*) FILTER (WHERE quantity_change = 0) / NULLIF(COUNT(*), 0), 0)) / 100)
          AS stock_health_index
        FROM derived
        GROUP BY product_variant_id
    """)

    metrics_result = db.execute(metrics_sql, {"start_date": start_date, "end_date": end_date, "store_id": store_id}).mappings().all()
    metrics_by_variant = {row["product_variant_id"]: dict(row) for row in metrics_result}

    # Step 2: Get latest snapshot per variant
    latest_snapshot_subquery = (
        db.query(
            models.InventorySnapshot.product_variant_id,
            func.max(models.InventorySnapshot.date).label("max_date")
        )
        .filter(models.InventorySnapshot.date <= end_date)
        .group_by(models.InventorySnapshot.product_variant_id)
        .subquery("latest")
    )

    base_query = (
        db.query(models.InventorySnapshot)
        .join(
            latest_snapshot_subquery,
            (models.InventorySnapshot.product_variant_id == latest_snapshot_subquery.c.product_variant_id)
            & (models.InventorySnapshot.date == latest_snapshot_subquery.c.max_date)
        )
        .options(
            joinedload(models.InventorySnapshot.product_variant)
            .joinedload(models.ProductVariant.product)
        )
    )

    if store_id:
        base_query = base_query.filter(models.InventorySnapshot.store_id == store_id)

    total_count = base_query.count()
    results = base_query.order_by(models.InventorySnapshot.date.desc()).offset(skip).limit(limit).all()

    # Step 3: Attach metrics
    for snapshot in results:
        snapshot.metrics = metrics_by_variant.get(snapshot.product_variant_id, {})

    # Step 4: Serialize snapshots for JSON
    def serialize_snapshot(snapshot):
        variant = snapshot.product_variant
        product = variant.product if variant else None
        return {
            "id": snapshot.id,
            "on_hand": snapshot.on_hand,
            "product_variant": {
                "id": variant.id if variant else None,
                "sku": variant.sku if variant else None,
                "product": {
                    "id": product.id if product else None,
                    "title": product.title if product else None,
                    "image_url": product.image_url if product else None
                } if product else None
            } if variant else None,
            "metrics": snapshot.metrics
        }

    serialized_snapshots = [serialize_snapshot(s) for s in results]

    return {"snapshots": serialized_snapshots, "total_count": total_count}
