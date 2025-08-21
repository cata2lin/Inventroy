# services/valuation_service.py

from sqlalchemy.orm import Session
import models

def get_group_valuation(db: Session, group_id: str):
    """
    Calculates the retail and inventory value for a given barcode group.
    """
    group = db.query(models.BarcodeGroup).filter_by(id=group_id).first()
    if not group:
        return None

    total_on_hand = 0
    total_available = 0
    
    # In a real implementation, you'd gather all members and their snapshots/costs
    # For now, we'll use placeholder logic.

    # Placeholder: Use a fixed price for valuation
    valuation_price = 10.0 # This should come from a policy (e.g., max price in group)
    valuation_cost = 5.0  # This should use the cost_per_item precedence

    # Clamp available at 0 for valuation to handle negative stock
    total_available_for_valuation = max(0, group.pool_available)
    negative_stock_debt = abs(min(0, group.pool_available))

    # This is a simplified calculation; the real one would sum on_hand across all members
    # total_on_hand = db.query(func.sum(...)) 

    return {
        "retail_value_available": total_available_for_valuation * valuation_price,
        "retail_value_on_hand": total_on_hand * valuation_price,
        "inventory_value_available": total_available_for_valuation * valuation_cost,
        "inventory_value_on_hand": total_on_hand * valuation_cost,
        "negative_stock_debt_units": negative_stock_debt,
        "negative_stock_debt_value": negative_stock_debt * valuation_cost
    }