# inventory_service.py

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, or_
from typing import List, Optional
import models
from product_service import ProductService
from fastapi import BackgroundTasks

class InventoryService:
    def __init__(self, db_session: Session):
        self.db = db_session

    def _get_product_group(self, barcode: str) -> List[models.ProductVariant]:
        """Gets all product variants that share the same barcode."""
        return self.db.query(models.ProductVariant).filter(models.ProductVariant.barcode == barcode).all()

    def _adjust_inventory_for_group(self, barcode: str, quantity: int, reason: str, source_info: str, is_absolute_set: bool):
        """
        Adjusts the on-hand quantity for all SKUs within a barcode group.
        This function handles the core logic of setting or changing inventory and logging the movement.
        """
        product_group = self._get_product_group(barcode)
        if not product_group:
            raise ValueError(f"Product group with barcode '{barcode}' not found.")

        # Assume on-hand is consistent for the group, take the first one.
        current_on_hand = product_group[0].inventory_quantity or 0
        new_on_hand = quantity if is_absolute_set else current_on_hand + quantity
        actual_change = new_on_hand - current_on_hand

        if actual_change == 0 and not is_absolute_set:
            return {"message": f"No change in quantity for barcode '{barcode}'."}

        # Update all variants in the group
        variant_ids_to_update = [p.id for p in product_group]
        self.db.query(models.ProductVariant).\
            filter(models.ProductVariant.id.in_(variant_ids_to_update)).\
            update({"inventory_quantity": new_on_hand}, synchronize_session=False)

        # Log a movement for each SKU in the group
        for variant in product_group:
            movement = models.StockMovement(
                product_sku=variant.sku,
                change_quantity=actual_change,
                new_quantity=new_on_hand,
                reason=reason,
                source_info=source_info
            )
            self.db.add(movement)
        
        self.db.commit()

        return {"message": f"Inventory for group '{barcode}' updated. New On Hand: {new_on_hand}"}

    def set_inventory(self, barcode: str, quantity: int, reason: str, source_info: str):
        return self._adjust_inventory_for_group(barcode, quantity, reason, source_info, is_absolute_set=True)

    def add_inventory(self, barcode: str, quantity: int, reason: str, source_info: str):
        return self._adjust_inventory_for_group(barcode, quantity, reason, source_info, is_absolute_set=False)

    def subtract_inventory(self, barcode: str, quantity: int, reason: str, source_info: str):
        return self._adjust_inventory_for_group(barcode, -quantity, reason, source_info, is_absolute_set=False)