# routes/forecasting.py

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session
from typing import List, Optional
import pandas as pd
import io

from database import get_db
from crud import forecasting as crud_forecasting

router = APIRouter(
    prefix="/api/forecasting",
    tags=["Forecasting"],
)

def _parse_store_ids(store_ids_str: Optional[List[str]] = None) -> Optional[List[int]]:
    """
    Safely parses a list of strings into a list of integers, ignoring empty or invalid values.
    """
    if store_ids_str is None:
        return None
    
    parsed_ids = []
    for s_id in store_ids_str:
        try:
            if s_id:
                parsed_ids.append(int(s_id))
        except (ValueError, TypeError):
            continue
    return parsed_ids if parsed_ids else None

@router.get("/report")
def get_forecasting_report(
    db: Session = Depends(get_db),
    search: Optional[str] = Query(None),
    lead_time: int = 30,
    coverage_period: int = 60,
    store_ids: Optional[List[str]] = Query(None),
    product_types: Optional[List[str]] = Query(None),
    stock_statuses: Optional[List[str]] = Query(None),
    reorder_start_date: Optional[str] = Query(None),
    reorder_end_date: Optional[str] = Query(None),
    use_custom_velocity: bool = Query(False),
    velocity_start_date: Optional[str] = Query(None),
    velocity_end_date: Optional[str] = Query(None),
    active_velocity_metric: str = Query('velocity_30d')
):
    parsed_store_ids = _parse_store_ids(store_ids)

    data = crud_forecasting.get_forecasting_data(
        db, search, lead_time, coverage_period, parsed_store_ids, product_types, 
        reorder_start_date, reorder_end_date,
        use_custom_velocity, velocity_start_date, velocity_end_date,
        active_velocity_metric
    )
    if stock_statuses:
        data = [item for item in data if item['stock_status'] in stock_statuses]
    return data
    
@router.get("/filters")
def get_forecasting_filters(db: Session = Depends(get_db)):
    return crud_forecasting.get_forecasting_filters(db)

@router.get("/export")
def export_forecasting_report(
    db: Session = Depends(get_db),
    search: Optional[str] = Query(None),
    lead_time: int = 30,
    coverage_period: int = 60,
    store_ids: Optional[List[str]] = Query(None),
    product_types: Optional[List[str]] = Query(None),
    stock_statuses: Optional[List[str]] = Query(None),
    reorder_start_date: Optional[str] = Query(None),
    reorder_end_date: Optional[str] = Query(None),
    use_custom_velocity: bool = Query(False),
    velocity_start_date: Optional[str] = Query(None),
    velocity_end_date: Optional[str] = Query(None),
    active_velocity_metric: str = Query('velocity_30d')
):
    parsed_store_ids = _parse_store_ids(store_ids)

    data = crud_forecasting.get_forecasting_data(
        db, search, lead_time, coverage_period, parsed_store_ids, product_types, 
        reorder_start_date, reorder_end_date,
        use_custom_velocity, velocity_start_date, velocity_end_date,
        active_velocity_metric
    )
    if stock_statuses:
        data = [item for item in data if item['stock_status'] in stock_statuses]
        
    df = pd.DataFrame(data)

    # Define the columns and their desired order for the export
    column_map = {
        'image_url': 'Image URL',
        'product_title': 'Product',
        'sku': 'SKU',
        'total_stock': 'Total Stock',
        'velocity_7d': 'Velocity (7d)',
        'velocity_30d': 'Velocity (30d)',
        'velocity_lifetime': 'Velocity (Lifetime)',
        'days_of_stock': 'Days of Stock',
        'stock_status': 'Stock Status',
        'reorder_date': 'Reorder Date',
        'reorder_qty': 'Reorder Qty'
    }

    # Conditionally add the period velocity columns
    if use_custom_velocity and 'velocity_period' in df.columns:
        column_map['velocity_period'] = 'Velocity (Period)'
        df['velocity_period_dates'] = f"{velocity_start_date} to {velocity_end_date}"
        column_map['velocity_period_dates'] = 'Velocity Period Dates'

    # Prepare the DataFrame for export
    df.rename(columns=column_map, inplace=True)
    export_columns = [col_name for col_name in column_map.values() if col_name in df.columns]
    df = df[export_columns]
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Forecasting Report')
    
    # This is the crucial fix: reset the stream's position to the beginning
    output.seek(0)

    return Response(
        content=output.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=forecasting_report.xlsx"}
    )