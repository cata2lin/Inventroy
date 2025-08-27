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

@router.get("/report")
def get_forecasting_report(
    db: Session = Depends(get_db),
    search: Optional[str] = Query(None),
    lead_time: int = 30,
    coverage_period: int = 60,
    store_ids: Optional[List[int]] = Query(None),
    product_types: Optional[List[str]] = Query(None),
    stock_statuses: Optional[List[str]] = Query(None),
    reorder_start_date: Optional[str] = Query(None),
    reorder_end_date: Optional[str] = Query(None),
    use_custom_velocity: bool = Query(False),
    velocity_start_date: Optional[str] = Query(None),
    velocity_end_date: Optional[str] = Query(None),
    velocity_metric: str = Query('period')
):
    data = crud_forecasting.get_forecasting_data(
        db, search, lead_time, coverage_period, store_ids, product_types, 
        reorder_start_date, reorder_end_date,
        use_custom_velocity, velocity_start_date, velocity_end_date,
        velocity_metric
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
    store_ids: Optional[List[int]] = Query(None),
    product_types: Optional[List[str]] = Query(None),
    stock_statuses: Optional[List[str]] = Query(None),
    reorder_start_date: Optional[str] = Query(None),
    reorder_end_date: Optional[str] = Query(None),
    use_custom_velocity: bool = Query(False),
    velocity_start_date: Optional[str] = Query(None),
    velocity_end_date: Optional[str] = Query(None),
    velocity_metric: str = Query('period')
):
    data = crud_forecasting.get_forecasting_data(
        db, search, lead_time, coverage_period, store_ids, product_types, 
        reorder_start_date, reorder_end_date,
        use_custom_velocity, velocity_start_date, velocity_end_date,
        velocity_metric
    )
    if stock_statuses:
        data = [item for item in data if item['stock_status'] in stock_statuses]
        
    df = pd.DataFrame(data)
    
    # Add custom velocity to export if used
    if use_custom_velocity:
        df['velocity_period_dates'] = f"{velocity_start_date} to {velocity_end_date}"
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Forecasting Report')
    
    return Response(
        content=output.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=forecasting_report.xlsx"}
    )