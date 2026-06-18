# routes/classification.py
"""P4 — duplicate/group classification review & override API. Writes classification metadata
only (never inventory); every change is audited. Authenticated via the global middleware."""
from fastapi import APIRouter, Depends, Request, Body
from sqlalchemy.orm import Session

from database import get_db
from services import classification

router = APIRouter(prefix="/api/classification", tags=["Classification"])


def _actor(request: Request) -> str:
    return getattr(request.state, "user", None) or "operator"


@router.get("/groups")
def list_groups(classification_filter: str = None, limit: int = 200, db: Session = Depends(get_db)):
    return {"groups": classification.list_groups(db, classification=classification_filter, limit=limit)}


@router.get("/groups/{group_id}")
def group_detail(group_id: int, db: Session = Depends(get_db)):
    return classification.group_detail(db, group_id)


@router.post("/groups/{group_id}/classify")
def classify_group(group_id: int, request: Request,
                   payload: dict = Body(...), db: Session = Depends(get_db)):
    return classification.set_group_classification(
        db, group_id, payload.get("classification", ""), actor=_actor(request),
        notes=payload.get("notes"))


@router.post("/groups/{group_id}/authoritative")
def set_authoritative(group_id: int, request: Request,
                      payload: dict = Body(...), db: Session = Depends(get_db)):
    return classification.set_authoritative_variant(
        db, group_id, payload.get("variant_id"), actor=_actor(request))


@router.post("/members/{variant_id}/exclude")
def exclude_member(variant_id: int, request: Request,
                   payload: dict = Body(...), db: Session = Depends(get_db)):
    return classification.set_member_excluded(
        db, variant_id, bool(payload.get("excluded", True)), actor=_actor(request))
