# crud/webhooks.py

from sqlalchemy.orm import Session
import models
import schemas

def create_webhook_registration(db: Session, store_id: int, webhook_data: dict):
    """Saves a webhook registration to the local database."""
    db_webhook = models.Webhook(
        shopify_webhook_id=webhook_data['id'],
        store_id=store_id,
        topic=webhook_data['topic'],
        address=webhook_data['address']
    )
    db.add(db_webhook)
    db.commit()
    db.refresh(db_webhook)
    return db_webhook

def get_webhook_registrations_for_store(db: Session, store_id: int):
    """Retrieves all webhook registrations for a specific store."""
    return db.query(models.Webhook).filter(models.Webhook.store_id == store_id).all()

def delete_webhook_registration(db: Session, shopify_webhook_id: int):
    """Deletes a webhook registration from the local database."""
    db.query(models.Webhook).filter(models.Webhook.shopify_webhook_id == shopify_webhook_id).delete()
    db.commit()