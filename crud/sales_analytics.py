# crud/sales_analytics.py

from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import date

def get_sales_by_product_data(
    db: Session,
    start_ts: date,
    end_ts: date,
    stores: Optional[List[int]],
    only_paid: bool,
    exclude_canceled: bool,
    search: Optional[str],
    limit: int,
    offset: int
):
    """
    Executes a complex SQL query to get sales data aggregated by product barcode.
    """
    params = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "stores": stores,
        "only_paid": only_paid,
        "exclude_canceled": exclude_canceled,
        "search": f"%{search}%" if search else None,
        "limit": limit,
        "offset": offset,
    }

    query = text("""
        WITH eligible_orders AS (
            SELECT id, store_id FROM orders
            WHERE created_at >= :start_ts AND created_at < :end_ts + interval '1 day'
            AND (:exclude_canceled = false OR cancelled_at IS NULL)
            AND (:only_paid = false OR financial_status IN ('paid','partially_paid','partially_refunded','refunded'))
            AND (:stores IS NULL OR store_id = ANY(:stores))
        ),
        sales_li AS (
            SELECT COALESCE(pv.barcode_normalized,'NO_BARCODE') AS barcode,
                   COUNT(DISTINCT li.order_id) AS orders_count,
                   SUM(li.quantity) AS units_sold,
                   SUM(li.quantity * li.price)::numeric(18,2) AS gross_sales,
                   SUM(li.total_discount)::numeric(18,2) AS discounts,
                   MIN(p.title) as product_title -- Select one title for display
            FROM line_items li
            JOIN eligible_orders o ON o.id = li.order_id
            LEFT JOIN product_variants pv ON pv.id = li.variant_id
            LEFT JOIN products p ON p.id = pv.product_id
            WHERE (:search IS NULL OR p.title ILIKE :search OR pv.sku ILIKE :search OR pv.barcode_normalized ILIKE :search)
            GROUP BY 1
        ),
        refunds_in_window AS (
            SELECT id FROM refunds
            WHERE created_at >= :start_ts AND created_at < :end_ts + interval '1 day'
        ),
        rli AS (
            SELECT COALESCE(pv.barcode_normalized,'NO_BARCODE') AS barcode,
                   SUM(rli.quantity) AS refunded_units,
                   SUM(rli.subtotal)::numeric(18,2) AS returns_value
            FROM refund_line_items rli
            JOIN refunds_in_window rw ON rw.id = rli.refund_id
            JOIN line_items li ON li.id = rli.line_item_id
            LEFT JOIN product_variants pv ON pv.id = li.variant_id
            GROUP BY 1
        ),
        days AS (
            SELECT GREATEST(1, 1 + DATE_PART('day', :end_ts::timestamptz - :start_ts::timestamptz))::numeric AS days_in_window
        )
        SELECT
            s.barcode,
            s.product_title,
            s.orders_count,
            s.units_sold,
            COALESCE(r.refunded_units,0) AS refunded_units,
            CASE WHEN s.units_sold > 0 THEN ROUND(COALESCE(r.refunded_units,0)::numeric / s.units_sold, 4) ELSE 0 END AS refund_rate_qty,
            s.gross_sales,
            s.discounts,
            COALESCE(r.returns_value,0) AS returns_value,
            (s.gross_sales - s.discounts - COALESCE(r.returns_value,0))::numeric(18,2) AS net_sales,
            ROUND( (s.units_sold - COALESCE(r.refunded_units,0)) / (SELECT days_in_window FROM days), 4) AS velocity_units_per_day,
            CASE WHEN s.units_sold > 0 THEN (s.gross_sales - s.discounts) / s.units_sold ELSE 0 END as asp
        FROM sales_li s
        LEFT JOIN rli r USING (barcode)
        ORDER BY net_sales DESC NULLS LAST
        LIMIT :limit OFFSET :offset;
    """)
    
    result = db.execute(query, params).fetchall()
    return [row._asdict() for row in result]

def get_inventory_for_barcode_data(db: Session, barcode: str):
    """
    Gets the inventory breakdown for a given barcode.
    """
    params = {"barcode": barcode}
    query = text("""
        WITH group_variants AS (
            SELECT gm.group_id, pv.id AS variant_id, pv.inventory_item_id
            FROM group_membership gm
            JOIN product_variants pv ON pv.id = gm.variant_id
            WHERE COALESCE(pv.barcode_normalized,'NO_BARCODE') = :barcode
        ),
        onhand AS (
            SELECT gv.group_id, l.store_id, SUM(il.on_hand) AS on_hand
            FROM group_variants gv
            JOIN inventory_levels il ON il.inventory_item_id = gv.inventory_item_id
            JOIN locations l ON l.id = il.location_id
            GROUP BY 1,2
        )
        SELECT
            s.id AS store_id, s.name AS store,
            COALESCE(o.on_hand,0) AS on_hand,
            COALESCE(c.committed_units,0) AS committed,
            COALESCE(o.on_hand,0) - COALESCE(c.committed_units,0) AS free_to_sell
        FROM stores s
        LEFT JOIN onhand o ON o.store_id = s.id
        LEFT JOIN committed_stock c ON c.store_id = s.id AND c.group_id = (SELECT group_id FROM group_variants LIMIT 1)
        ORDER BY store;
    """)
    
    result = db.execute(query, params).fetchall()
    return [row._asdict() for row in result]