# routes/data_quality.py

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, text

from database import get_db
import models

router = APIRouter(prefix="/api/data-quality", tags=["Data Quality"])


@router.get("/issues")
def get_data_quality_issues(
    store_id: Optional[int] = Query(None),
    issue_type: Optional[str] = Query(None, description="Filter by: no_barcode, no_sku, sku_mismatch, barcode_mismatch"),
    search: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db)
):
    """
    Get products with data quality issues:
    - no_barcode: Products without a barcode
    - no_sku: Products without a SKU
    - sku_mismatch: Same SKU but different barcodes across stores
    - barcode_mismatch: Same barcode but different SKUs across stores
    """
    
    # Build base query for all variants
    params: Dict[str, Any] = {"skip": skip, "limit": limit}
    
    store_filter = ""
    if store_id:
        store_filter = "AND pv.store_id = :store_id"
        params["store_id"] = store_id
    
    search_filter = ""
    if search:
        search_terms = [w.strip().lower() for w in search.split() if w.strip()]
        if search_terms:
            search_conditions = []
            for i, term in enumerate(search_terms):
                param_name = f"search{i}"
                search_conditions.append(f"(LOWER(p.title) LIKE :{param_name} OR LOWER(COALESCE(pv.sku, '')) LIKE :{param_name} OR LOWER(COALESCE(pv.barcode, '')) LIKE :{param_name})")
                params[param_name] = f"%{term}%"
            search_filter = "AND " + " AND ".join(search_conditions)
    
    # Get issue counts for summary
    summary_sql = text(f"""
    WITH variants AS (
        SELECT 
            pv.id,
            pv.sku,
            pv.barcode,
            pv.store_id,
            p.title,
            p.image_url,
            s.name as store_name
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id
        JOIN stores s ON s.id = pv.store_id
        WHERE 1=1 {store_filter} {search_filter}
    ),
    no_barcode AS (
        SELECT COUNT(*) as cnt FROM variants WHERE barcode IS NULL OR barcode = ''
    ),
    no_sku AS (
        SELECT COUNT(*) as cnt FROM variants WHERE sku IS NULL OR sku = ''
    ),
    sku_groups AS (
        SELECT sku, COUNT(DISTINCT NULLIF(barcode, '')) as barcode_count
        FROM variants
        WHERE sku IS NOT NULL AND sku != ''
        GROUP BY sku
        HAVING COUNT(DISTINCT NULLIF(barcode, '')) > 1
    ),
    barcode_groups AS (
        SELECT barcode, COUNT(DISTINCT NULLIF(sku, '')) as sku_count
        FROM variants
        WHERE barcode IS NOT NULL AND barcode != ''
        GROUP BY barcode
        HAVING COUNT(DISTINCT NULLIF(sku, '')) > 1
    )
    SELECT 
        (SELECT cnt FROM no_barcode) as no_barcode_count,
        (SELECT cnt FROM no_sku) as no_sku_count,
        (SELECT COUNT(*) FROM sku_groups) as sku_mismatch_count,
        (SELECT COUNT(*) FROM barcode_groups) as barcode_mismatch_count
    """)
    
    summary = db.execute(summary_sql, params).mappings().first()
    
    # Get detailed issues based on filter
    issues = []
    
    if issue_type == "no_barcode" or issue_type is None:
        no_barcode_sql = text(f"""
        SELECT 
            pv.id as variant_id,
            pv.sku,
            pv.barcode,
            p.title,
            p.image_url,
            s.name as store_name,
            'no_barcode' as issue_type
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id
        JOIN stores s ON s.id = pv.store_id
        WHERE (pv.barcode IS NULL OR pv.barcode = '') {store_filter} {search_filter}
        ORDER BY p.title, s.name
        LIMIT :limit OFFSET :skip
        """)
        rows = db.execute(no_barcode_sql, params).mappings().all()
        issues.extend([dict(r) for r in rows])
    
    if issue_type == "no_sku" or issue_type is None:
        no_sku_sql = text(f"""
        SELECT 
            pv.id as variant_id,
            pv.sku,
            pv.barcode,
            p.title,
            p.image_url,
            s.name as store_name,
            'no_sku' as issue_type
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id
        JOIN stores s ON s.id = pv.store_id
        WHERE (pv.sku IS NULL OR pv.sku = '') {store_filter} {search_filter}
        ORDER BY p.title, s.name
        LIMIT :limit OFFSET :skip
        """)
        rows = db.execute(no_sku_sql, params).mappings().all()
        issues.extend([dict(r) for r in rows])
    
    if issue_type == "sku_mismatch" or issue_type is None:
        # Same SKU, different barcodes
        sku_mismatch_sql = text(f"""
        WITH problem_skus AS (
            SELECT sku
            FROM product_variants pv
            WHERE sku IS NOT NULL AND sku != '' {store_filter.replace('pv.store_id', 'store_id')}
            GROUP BY sku
            HAVING COUNT(DISTINCT NULLIF(barcode, '')) > 1
        )
        SELECT 
            pv.id as variant_id,
            pv.sku,
            pv.barcode,
            p.title,
            p.image_url,
            s.name as store_name,
            'sku_mismatch' as issue_type
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id
        JOIN stores s ON s.id = pv.store_id
        WHERE pv.sku IN (SELECT sku FROM problem_skus) {store_filter} {search_filter}
        ORDER BY pv.sku, s.name
        LIMIT :limit OFFSET :skip
        """)
        rows = db.execute(sku_mismatch_sql, params).mappings().all()
        issues.extend([dict(r) for r in rows])
    
    if issue_type == "barcode_mismatch" or issue_type is None:
        # Same barcode, different SKUs
        barcode_mismatch_sql = text(f"""
        WITH problem_barcodes AS (
            SELECT barcode
            FROM product_variants pv
            WHERE barcode IS NOT NULL AND barcode != '' {store_filter.replace('pv.store_id', 'store_id')}
            GROUP BY barcode
            HAVING COUNT(DISTINCT NULLIF(sku, '')) > 1
        )
        SELECT 
            pv.id as variant_id,
            pv.sku,
            pv.barcode,
            p.title,
            p.image_url,
            s.name as store_name,
            'barcode_mismatch' as issue_type
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id
        JOIN stores s ON s.id = pv.store_id
        WHERE pv.barcode IN (SELECT barcode FROM problem_barcodes) {store_filter} {search_filter}
        ORDER BY pv.barcode, s.name
        LIMIT :limit OFFSET :skip
        """)
        rows = db.execute(barcode_mismatch_sql, params).mappings().all()
        issues.extend([dict(r) for r in rows])
    
    return {
        "summary": {
            "no_barcode": int(summary["no_barcode_count"] or 0),
            "no_sku": int(summary["no_sku_count"] or 0),
            "sku_mismatch": int(summary["sku_mismatch_count"] or 0),
            "barcode_mismatch": int(summary["barcode_mismatch_count"] or 0),
        },
        "issues": issues,
        "total_count": len(issues),
    }


@router.get("/stores")
def list_stores(db: Session = Depends(get_db)):
    rows = db.execute(text("SELECT id, name FROM stores WHERE enabled = TRUE ORDER BY name")).mappings().all()
    return [{"id": int(r["id"]), "name": r["name"]} for r in rows]
