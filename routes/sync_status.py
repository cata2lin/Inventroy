# routes/sync_status.py

from fastapi import APIRouter, Depends
from services import sync_service

router = APIRouter(
    prefix="/api/sync-status",
    tags=["Sync Status"],
    responses={404: {"description": "Not found"}},
)

@router.get("/{task_id}")
def get_status(task_id: str):
    """
    Pollable endpoint to get the status of a background sync task.
    """
    return sync_service.get_sync_status(task_id)