# crud/snapshots.py
from datetime import datetime, date
from typing import List, Optional, Tuple, Dict, Any

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import text, func

import models

def create_snapshot_for_store(db: Session, store_id: int):
    """
    Creates a snapshot of all current inventory levels for a given store,
    including price and cost at the time of the snapshot.
    """
    now = datetime.now(date.today().tzinfo)

    # Join InventoryLevel with ProductVariant to get all necessary data in one query
    inventory_data = (
        db.query(
            models.InventoryLevel.variant_id,
            models.InventoryLevel.on_hand,
            models.ProductVariant.price,
            models.ProductVariant.cost_per_item
        )
        .join(models.ProductVariant, models.InventoryLevel.variant_id == models.ProductVariant.id)
        .filter(
            models.ProductVariant.store_id == store_id,
            models.InventoryLevel.on_hand > 0
        )
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
        db.bulk_insert_mappings(models.InventorySnapshot, snapshot_entries)
        db.commit()
        print(f"[SNAPSHOT] Successfully created snapshot for store {store_id} with {len(snapshot_entries)} entries.")


def get_snapshots(
    db: Session, skip: int = 0, limit: int = 100, store_id: Optional[int] = None, snapshot_date: Optional[date] = None
) -> Tuple[List[models.InventorySnapshot], int]:
    """
    Retrieves paginated and filterable inventory snapshots.
    """
    query = db.query(models.InventorySnapshot).options(
        joinedload(models.InventorySnapshot.product_variant).joinedload(models.ProductVariant.product)
    )

    if store_id:
        query = query.filter(models.InventorySnapshot.store_id == store_id)
    if snapshot_date:
        query = query.filter(models.InventorySnapshot.date == snapshot_date)

    total_count = query.count()
    snapshots = query.order_by(models.InventorySnapshot.date.desc()).offset(skip).limit(limit).all()

    return snapshots, total_count


def get_snapshot_metrics(db: Session, variant_id: int, start_date: date, end_date: date) -> Optional[Dict[str, Any]]:
    """
    Calculates advanced inventory metrics for a specific product variant
    over a date range using a raw SQL query for performance.
    """
    
    # This query is a direct implementation of your provided specification.
    sql_query = text("""
    WITH lagged AS (
      SELECT
        s.date,
        s.on_hand AS quantity,
        s.price,
        s.cost_per_item,
        LAG(s.on_hand) OVER (PARTITION BY s.product_variant_id ORDER BY s.date) AS prev_quantity
      FROM inventory_snapshots s
      WHERE s.product_variant_id = :variant_id AND s.date BETWEEN :start_date AND :end_date
    ),
    derived AS (
      SELECT
        *,
        (quantity - prev_quantity) AS quantity_change,
        (quantity * cost_per_item) AS inventory_value,
        (quantity * price) AS sales_value,
        ((price - cost_per_item) * quantity) AS gross_margin_value
      FROM lagged
    )
    SELECT
      AVG(quantity) AS average_stock_level,
      MIN(quantity) AS min_stock_level,
      MAX(quantity) AS max_stock_level,
      (MAX(quantity) - MIN(quantity)) AS stock_range,
      STDDEV_SAMP(quantity) AS stock_stddev,
      COUNT(*) FILTER (WHERE quantity = 0) AS days_out_of_stock,
      100.0 * COUNT(*) FILTER (WHERE quantity = 0) / COUNT(*) AS stockout_rate,
      COUNT(*) FILTER (WHERE quantity_change > 0) AS replenishment_days,
      COUNT(*) FILTER (WHERE quantity_change < 0) AS depletion_days,
      SUM(GREATEST(prev_quantity - quantity, 0)) AS total_outflow,
      SUM(GREATEST(prev_quantity - quantity, 0)) / NULLIF(AVG(quantity), 0) AS stock_turnover,
      COUNT(DISTINCT date) / NULLIF(SUM(GREATEST(prev_quantity - quantity, 0)) / NULLIF(AVG(quantity), 0), 0) AS avg_days_in_inventory,
      COUNT(*) FILTER (WHERE quantity_change = 0) AS dead_stock_days,
      100.0 * COUNT(*) FILTER (WHERE quantity_change = 0) / COUNT(*) AS dead_stock_ratio,
      AVG(inventory_value) AS avg_inventory_value,
      AVG(sales_value) AS avg_sales_value,
      AVG(gross_margin_value) AS avg_gross_margin_value,
      100.0 * AVG(CASE WHEN ABS((quantity - prev_quantity) / NULLIF(prev_quantity, 0)) < 0.05 THEN 1 ELSE 0 END) AS stability_index,
      (1 - (100.0 * COUNT(*) FILTER (WHERE quantity = 0) / COUNT(*)) / 100) * (1 - (100.0 * COUNT(*) FILTER (WHERE quantity_change = 0) / COUNT(*)) / 100) AS stock_health_index
    FROM derived;
    """)

    result = db.execute(sql_query, {
        "variant_id": variant_id,
        "start_date": start_date,
        "end_date": end_date
    }).fetchone()

    if result:
        # The result is a Row object, convert it to a dictionary
        return dict(result._mapping)
    return None