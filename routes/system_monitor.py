# routes/system_monitor.py
"""
API endpoints for the System Monitor page.
Provides access to audit logs, system events, error history, and live stats.
"""
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, text, case

from database import get_db
from models import AuditLog, SystemEvent, Store, SyncRun

router = APIRouter(prefix="/api/system-monitor", tags=["System Monitor"])


@router.get("/stats")
def get_system_stats(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Dashboard stats: uptime metrics, error rates, operation counts."""
    now = datetime.now(timezone.utc)
    last_24h = now - timedelta(hours=24)
    last_1h = now - timedelta(hours=1)

    # Total audit logs in last 24h
    total_24h = db.query(func.count(AuditLog.id)).filter(
        AuditLog.timestamp >= last_24h
    ).scalar() or 0

    # Errors in last 24h
    errors_24h = db.query(func.count(AuditLog.id)).filter(
        AuditLog.timestamp >= last_24h,
        AuditLog.severity.in_(["ERROR", "CRITICAL"])
    ).scalar() or 0

    # Webhooks in last 24h
    webhooks_24h = db.query(func.count(AuditLog.id)).filter(
        AuditLog.timestamp >= last_24h,
        AuditLog.category == "WEBHOOK"
    ).scalar() or 0

    # Syncs in last 24h
    syncs_24h = db.query(func.count(AuditLog.id)).filter(
        AuditLog.timestamp >= last_24h,
        AuditLog.category == "SYNC",
        AuditLog.action.in_(["sync_started", "sync_completed"])
    ).scalar() or 0

    # Stock changes in last 24h
    stock_changes_24h = db.query(func.count(AuditLog.id)).filter(
        AuditLog.timestamp >= last_24h,
        AuditLog.category == "STOCK"
    ).scalar() or 0

    # Webhooks per hour (last 1h)
    webhooks_1h = db.query(func.count(AuditLog.id)).filter(
        AuditLog.timestamp >= last_1h,
        AuditLog.category == "WEBHOOK"
    ).scalar() or 0

    # Unresolved system errors
    unresolved_errors = db.query(func.count(SystemEvent.id)).filter(
        SystemEvent.resolved == False
    ).scalar() or 0

    # Average webhook processing time (last 24h)
    avg_webhook_ms = db.query(func.avg(AuditLog.duration_ms)).filter(
        AuditLog.timestamp >= last_24h,
        AuditLog.category == "WEBHOOK",
        AuditLog.duration_ms.isnot(None)
    ).scalar()

    # Last sync run
    last_sync = db.query(SyncRun).order_by(desc(SyncRun.started_at)).first()

    # Category breakdown for last 24h
    category_counts = db.query(
        AuditLog.category, func.count(AuditLog.id)
    ).filter(
        AuditLog.timestamp >= last_24h
    ).group_by(AuditLog.category).all()

    return {
        "total_events_24h": total_24h,
        "errors_24h": errors_24h,
        "webhooks_24h": webhooks_24h,
        "webhooks_per_hour": webhooks_1h,
        "syncs_24h": syncs_24h,
        "stock_changes_24h": stock_changes_24h,
        "unresolved_errors": unresolved_errors,
        "avg_webhook_ms": round(avg_webhook_ms, 1) if avg_webhook_ms else None,
        "last_sync": {
            "store_id": last_sync.store_id,
            "started_at": last_sync.started_at.isoformat() if last_sync.started_at else None,
            "status": last_sync.status,
        } if last_sync else None,
        "category_breakdown": {cat: count for cat, count in category_counts},
    }


@router.get("/audit-logs")
def get_audit_logs(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    category: Optional[str] = None,
    severity: Optional[str] = None,
    store_id: Optional[int] = None,
    search: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Paginated, filterable audit log viewer."""
    query = db.query(AuditLog)

    if category:
        query = query.filter(AuditLog.category == category.upper())
    if severity:
        query = query.filter(AuditLog.severity == severity.upper())
    if store_id:
        query = query.filter(AuditLog.store_id == store_id)
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            (AuditLog.message.ilike(search_term)) |
            (AuditLog.target.ilike(search_term)) |
            (AuditLog.action.ilike(search_term)) |
            (AuditLog.actor.ilike(search_term))
        )
    if start_date:
        try:
            sd = datetime.fromisoformat(start_date)
            query = query.filter(AuditLog.timestamp >= sd)
        except ValueError:
            pass
    if end_date:
        try:
            ed = datetime.fromisoformat(end_date)
            query = query.filter(AuditLog.timestamp <= ed)
        except ValueError:
            pass

    total = query.count()
    logs = query.order_by(desc(AuditLog.timestamp)).offset(skip).limit(limit).all()

    return {
        "total_count": total,
        "logs": [
            {
                "id": l.id,
                "timestamp": l.timestamp.isoformat() if l.timestamp else None,
                "category": l.category,
                "action": l.action,
                "severity": l.severity,
                "actor": l.actor,
                "store_id": l.store_id,
                "store_name": l.store_name,
                "target": l.target,
                "message": l.message,
                "details": l.details,
                "duration_ms": l.duration_ms,
                "error_message": l.error_message,
                "stack_trace": l.stack_trace,
            }
            for l in logs
        ],
    }


@router.get("/errors")
def get_system_errors(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    resolved: Optional[bool] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """System error log with resolution tracking."""
    query = db.query(SystemEvent)

    if resolved is not None:
        query = query.filter(SystemEvent.resolved == resolved)
    if source:
        query = query.filter(SystemEvent.source.ilike(f"%{source}%"))

    total = query.count()
    events = query.order_by(desc(SystemEvent.timestamp)).offset(skip).limit(limit).all()

    return {
        "total_count": total,
        "events": [
            {
                "id": e.id,
                "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                "level": e.level,
                "source": e.source,
                "message": e.message,
                "details": e.details,
                "stack_trace": e.stack_trace,
                "resolved": e.resolved,
                "resolved_at": e.resolved_at.isoformat() if e.resolved_at else None,
                "resolved_by": e.resolved_by,
            }
            for e in events
        ],
    }


@router.post("/errors/{event_id}/resolve")
def resolve_system_error(
    event_id: int,
    db: Session = Depends(get_db),
) -> Dict[str, str]:
    """Mark a system error as resolved."""
    event = db.query(SystemEvent).filter(SystemEvent.id == event_id).first()
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")

    event.resolved = True
    event.resolved_at = datetime.now(timezone.utc)
    event.resolved_by = "admin"
    db.commit()

    return {"status": "ok", "message": f"Event {event_id} marked as resolved."}


@router.get("/webhook-history")
def get_webhook_history(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    store_id: Optional[int] = None,
    topic: Optional[str] = None,
) -> Dict[str, Any]:
    """Webhook-specific history view."""
    query = db.query(AuditLog).filter(AuditLog.category == "WEBHOOK")

    if store_id:
        query = query.filter(AuditLog.store_id == store_id)
    if topic:
        query = query.filter(AuditLog.target.ilike(f"%{topic}%"))

    total = query.count()
    logs = query.order_by(desc(AuditLog.timestamp)).offset(skip).limit(limit).all()

    return {
        "total_count": total,
        "webhooks": [
            {
                "id": l.id,
                "timestamp": l.timestamp.isoformat() if l.timestamp else None,
                "topic": l.target,
                "store_id": l.store_id,
                "store_name": l.store_name,
                "action": l.action,
                "message": l.message,
                "duration_ms": l.duration_ms,
                "error_message": l.error_message,
                "details": l.details,
            }
            for l in logs
        ],
    }


@router.get("/activity-timeline")
def get_activity_timeline(
    db: Session = Depends(get_db),
    hours: int = Query(24, ge=1, le=168),
) -> Dict[str, Any]:
    """Hourly activity breakdown for chart rendering."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)

    # Use raw SQL for hourly bucketing
    result = db.execute(text("""
        SELECT
            date_trunc('hour', timestamp) AS hour,
            category,
            COUNT(*) AS count
        FROM audit_logs
        WHERE timestamp >= :start
        GROUP BY hour, category
        ORDER BY hour
    """), {"start": start}).fetchall()

    timeline = {}
    for row in result:
        hour_str = row[0].isoformat() if row[0] else "unknown"
        if hour_str not in timeline:
            timeline[hour_str] = {}
        timeline[hour_str][row[1]] = row[2]

    return {"timeline": timeline, "hours": hours}
